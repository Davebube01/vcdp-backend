import asyncio
import json
import logging
import os
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.database import engine, init_db, AsyncSessionLocal, Base
from app.models import State, LGA, User, Project, Document, Transaction

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("deploy")

async def clear_remote_data(session):
    """Clear all records from target tables for a clean-slate sync."""
    logger.info("Clearing remote database tables for a clean sync...")
    # Order matters for foreign key constraints if any
    tables = ["transactions", "documents", "projects", "users", "lgas", "states"]
    for table in tables:
        try:
            await session.execute(text(f"DELETE FROM {table}"))
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
    
    # List of fields that we know should be JSON (lists/dicts)
    json_fields = {
        "commodity", "vcdp_component", "vcdp_sub_components", "lgas", 
        "threeFS_primary", "threeFS_sub_components", "fiscal_quarter",
        "funding_sources", "sub_funding_sources", "beneficiary_categories",
        "value_chain_segments", "data_source", "supporting_documents",
        "cofog_divisions", "cofog_groups", "quarterly_beneficiary_data"
    }

    for item in items:
        data = {k: v for k, v in item.__dict__.items() if not k.startswith('_sa_')}
        
        # Postgres JSON Correction: If a field is supposed to be JSON but arrives as a string, parse it.
        for field, value in data.items():
            if field in json_fields and isinstance(value, str):
                try:
                    data[field] = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    # If it's not valid JSON, treat it as a single-item list or empty
                    data[field] = [value] if value.strip() else []
        
        pg_session.add(model(**data))
    
    await pg_session.commit()
    logger.info(f"  Synced {len(items)} records for {name}.")

async def sync_data():
    """Sync all data from local SQLite to Postgres."""
    sqlite_path = "vcdp.db"
    if not os.path.exists(sqlite_path):
        logger.error("Local vcdp.db not found! Cannot sync data.")
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
                # 1. Clear remote data first
                await clear_remote_data(p_session)
                
                # 2. Sync in dependency order
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
                raise e
            finally:
                await s_session.close()
                await p_session.close()
    
    await sqlite_engine.dispose()

async def run_migrations():
    logger.info("Starting updated deployment workflow...")
    
    # 1. Initialize schema (ensure all columns exist)
    await init_db()
    logger.info("Schema initialized.")

    # 2. Automated Fresh Sync (SQLite -> Postgres)
    await sync_data()

    # 3. Final JSON Safety Check (including new COFOG fields)
    logger.info("Running final JSON data safety checks...")
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Transaction))
        transactions = result.scalars().all()
        
        json_fields = [
            "commodity", "vcdp_sub_components", "lgas", 
            "threeFS_primary", "threeFS_sub_components", 
            "funding_sources", "sub_funding_sources", 
            "beneficiary_categories", "value_chain_segments", 
            "data_source", "supporting_documents",
            "cofog_divisions", "cofog_groups"
        ]
        
        repair_count = 0
        for tx in transactions:
            updated = False
            for field in json_fields:
                val = getattr(tx, field)
                if val is None:
                    setattr(tx, field, [])
                    updated = True
                elif isinstance(val, str):
                    try:
                        parsed = json.loads(val)
                        if not isinstance(parsed, list):
                            setattr(tx, field, [parsed])
                            updated = True
                    except:
                        setattr(tx, field, [val] if val.strip() else [])
                        updated = True
            if updated:
                repair_count += 1
        
        if repair_count > 0:
            await session.commit()
            logger.info(f"Fixed JSON formatting for {repair_count} records.")

    logger.info("Deployment preparation completed. System is ready for live traffic.")

if __name__ == "__main__":
    import sys
    sys.path.append(os.getcwd())
    asyncio.run(run_migrations())

