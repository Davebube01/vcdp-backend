import asyncio
import json
import logging
import os
from sqlalchemy import select, text, inspect
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.database import engine, init_db, AsyncSessionLocal, Base
from app.models import State, LGA, User, Project, Document, Transaction

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("deploy")


# ─────────────────────────────────────────────
# Column migration: ADD new columns if missing
# ─────────────────────────────────────────────

COLUMN_MIGRATIONS = [
    # (table_name, column_name, column_definition)
    ("transactions", "activity_name",    "VARCHAR(500)"),
    ("transactions", "category_costcode","VARCHAR(300)"),
    # Safety net: ensure bulk-upload related columns land on Postgres
    ("transactions", "expenditure_ifad_loan",  "FLOAT DEFAULT 0"),
    ("transactions", "expenditure_ifad_grant", "FLOAT DEFAULT 0"),
    ("transactions", "expenditure_total_reported", "FLOAT DEFAULT 0"),
    ("transactions", "institution_code", "VARCHAR(100)"),
    ("transactions", "executing_agency", "VARCHAR(300)"),
    ("transactions", "activity_type_code", "VARCHAR(100)"),
    ("transactions", "record_type",      "VARCHAR(20) DEFAULT 'Actual'"),
    ("transactions", "exchange_rate",    "FLOAT DEFAULT 1.0"),
    ("transactions", "quarterly_beneficiary_data", "JSONB DEFAULT '{}'"),
    ("transactions", "value_chain_segments_other", "VARCHAR(500)"),
    ("transactions", "classification_notes", "VARCHAR(500)"),
    ("transactions", "rejection_reason", "TEXT"),
    ("transactions", "cofog_code",       "VARCHAR(50)"),
]


async def apply_column_migrations(conn):
    """
    Safely add new columns to existing tables using ALTER TABLE IF NOT EXISTS.
    This is idempotent — it won't fail if the column already exists.
    """
    logger.info("Applying column migrations...")
    is_postgres = "postgresql" in str(engine.url)

    for table, column, col_def in COLUMN_MIGRATIONS:
        try:
            if is_postgres:
                # PostgreSQL supports IF NOT EXISTS for ADD COLUMN
                sql = f'ALTER TABLE "{table}" ADD COLUMN IF NOT EXISTS "{column}" {col_def};'
            else:
                # SQLite — use a try/except approach since IF NOT EXISTS isn't supported
                sql = f'ALTER TABLE "{table}" ADD COLUMN "{column}" {col_def};'
            
            await conn.execute(text(sql))
            logger.info(f"  ✓ Column '{column}' on '{table}' is ready.")
        except Exception as e:
            err_str = str(e).lower()
            if "duplicate column" in err_str or "already exists" in err_str:
                logger.info(f"  - Column '{column}' on '{table}' already exists, skipping.")
            else:
                logger.warning(f"  ⚠ Could not add column '{column}' to '{table}': {e}")


# ─────────────────────────────────────────────
# Data sync: SQLite → PostgreSQL
# ─────────────────────────────────────────────

async def clear_remote_data(session):
    """Clear all records from target tables for a clean-slate sync."""
    logger.info("Clearing remote database tables for a clean sync...")
    tables = ["transactions", "documents", "projects", "users", "lgas", "states"]
    for table in tables:
        try:
            await session.execute(text(f'DELETE FROM "{table}"'))
        except Exception as e:
            logger.warning(f"Could not clear table {table}: {e}")
    await session.commit()


async def migrate_table(sqlite_session, pg_session, model, name):
    """Sync records for a specific table from SQLite to Postgres with JSON parsing."""
    result = await sqlite_session.execute(select(model))
    items = result.scalars().all()
    if not items:
        logger.info(f"  No records found for {name}.")
        return

    json_fields = {
        "commodity", "vcdp_component", "vcdp_sub_components", "lgas",
        "threeFS_primary", "threeFS_sub_components", "fiscal_quarter",
        "funding_sources", "sub_funding_sources", "beneficiary_categories",
        "value_chain_segments", "data_source", "supporting_documents",
        "cofog_divisions", "cofog_groups", "quarterly_beneficiary_data"
    }

    skipped = 0
    inserted = 0
    for item in items:
        data = {k: v for k, v in item.__dict__.items() if not k.startswith('_sa_')}

        # Convert JSON strings back to Python objects for Postgres JSONB columns
        for field, value in data.items():
            if field in json_fields and isinstance(value, str):
                try:
                    data[field] = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    data[field] = [value] if value.strip() else []

        # Strip None ref_ids so Postgres doesn't complain about NOT NULL
        if name == "Transactions":
            if not data.get("ref_id"):
                import uuid
                data["ref_id"] = f"MIGRATED-{uuid.uuid4().hex[:8].upper()}"

        try:
            pg_session.add(model(**data))
            inserted += 1
        except Exception as e:
            logger.warning(f"  Skipping one {name} record due to error: {e}")
            skipped += 1

    try:
        await pg_session.commit()
    except Exception as e:
        await pg_session.rollback()
        logger.error(f"  Batch commit failed for {name}: {e}")
        raise

    logger.info(f"  Synced {inserted} records for {name}. ({skipped} skipped)")


async def sync_data():
    """Sync all data from local SQLite to Postgres."""
    sqlite_path = "vcdp.db"
    if not os.path.exists(sqlite_path):
        logger.warning("Local vcdp.db not found — skipping data sync. Remote DB will only have schema.")
        return

    if "postgresql" not in str(engine.url):
        logger.info("Target database is not Postgres. Data sync is only for live deployment.")
        return

    logger.info("Detected Postgres target. Starting fresh data sync from local SQLite...")

    sqlite_engine = create_async_engine(f"sqlite+aiosqlite:///{sqlite_path}")
    SqliteSession = async_sessionmaker(sqlite_engine, expire_on_commit=False)

    async with SqliteSession() as s_session:
        async with AsyncSessionLocal() as p_session:
            try:
                await clear_remote_data(p_session)
                await migrate_table(s_session, p_session, State, "States")
                await migrate_table(s_session, p_session, LGA, "LGAs")
                await migrate_table(s_session, p_session, User, "Users")
                await migrate_table(s_session, p_session, Project, "Projects")
                await migrate_table(s_session, p_session, Document, "Documents")
                await migrate_table(s_session, p_session, Transaction, "Transactions")
                logger.info("Fresh data sync completed successfully.")
            except Exception as e:
                logger.error(f"Error during data sync: {e}")
                await p_session.rollback()
                raise
            finally:
                await s_session.close()

    await sqlite_engine.dispose()


# ─────────────────────────────────────────────
# JSON repair pass (post-sync safety)
# ─────────────────────────────────────────────

async def repair_json_fields():
    """Fix any JSON fields that arrived as plain strings (defensive pass)."""
    logger.info("Running JSON field safety repair pass...")
    json_fields = [
        "commodity", "vcdp_component", "vcdp_sub_components", "lgas",
        "threeFS_primary", "threeFS_sub_components", "fiscal_quarter",
        "funding_sources", "sub_funding_sources", "beneficiary_categories",
        "value_chain_segments", "data_source", "supporting_documents",
        "cofog_divisions", "cofog_groups",
    ]

    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Transaction))
        transactions = result.scalars().all()

        repair_count = 0
        for tx in transactions:
            updated = False
            for field in json_fields:
                val = getattr(tx, field, None)
                if val is None:
                    setattr(tx, field, [])
                    updated = True
                elif isinstance(val, str):
                    try:
                        parsed = json.loads(val)
                        setattr(tx, field, parsed if isinstance(parsed, list) else [parsed])
                    except Exception:
                        setattr(tx, field, [val] if val.strip() else [])
                    updated = True
            if updated:
                repair_count += 1

        if repair_count > 0:
            await session.commit()
            logger.info(f"  Repaired JSON fields on {repair_count} transaction(s).")
        else:
            logger.info("  No JSON repairs needed.")


# ─────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────

async def run_migrations():
    logger.info("=" * 60)
    logger.info("  VCDP Deployment Manager Starting")
    logger.info("=" * 60)

    # Step 1: Create any missing tables (safe, idempotent)
    logger.info("[1/4] Initializing schema (create_all)...")
    await init_db()
    logger.info("  Schema initialised.")

    # Step 2: Add any new columns that weren't in the original CREATE TABLE
    logger.info("[2/4] Applying incremental column migrations...")
    async with engine.begin() as conn:
        await apply_column_migrations(conn)

    # Step 3: Sync SQLite → Postgres (only on live deployments)
    logger.info("[3/4] Syncing data from local SQLite to Postgres...")
    await sync_data()

    # Step 4: Repair any malformed JSON fields
    logger.info("[4/4] Running JSON field safety checks...")
    await repair_json_fields()

    logger.info("=" * 60)
    logger.info("  Deployment preparation complete! Ready for traffic.")
    logger.info("=" * 60)


if __name__ == "__main__":
    import sys
    sys.path.append(os.getcwd())
    asyncio.run(run_migrations())
