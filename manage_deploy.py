from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.database import engine, init_db, AsyncSessionLocal, Base
from app.models import State, LGA, User, Project, Document, Transaction

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("deploy")

async def migrate_table(sqlite_session, pg_session, model, name):
    """Sync records for a specific table from SQLite to Postgres."""
    result = await sqlite_session.execute(select(model))
    items = result.scalars().all()
    if not items:
        return
    
    for item in items:
        data = {k: v for k, v in item.__dict__.items() if not k.startswith('_sa_')}
        await pg_session.merge(model(**data))
    await pg_session.commit()
    logger.info(f"  Synced {len(items)} records for {name}.")

async def sync_data():
    """Detect if we need to sync from local SQLite to Postgres."""
    sqlite_path = "vcdp.db"
    if not os.path.exists(sqlite_path):
        logger.info("Local vcdp.db not found. Skipping data sync.")
        return

    # Check if the target is Postgres
    if "postgresql" not in str(engine.url):
        logger.info("Target database is not Postgres. Skipping sync.")
        return

    logger.info("Detected Postgres target and local SQLite source. Starting sync...")
    
    sqlite_engine = create_async_engine(f"sqlite+aiosqlite:///{sqlite_path}")
    SqliteSession = async_sessionmaker(sqlite_engine, expire_on_commit=False)
    
    async with SqliteSession() as s_session:
        async with AsyncSessionLocal() as p_session:
            try:
                # Sync in dependency order
                await migrate_table(s_session, p_session, State, "States")
                await migrate_table(s_session, p_session, LGA, "LGAs")
                await migrate_table(s_session, p_session, User, "Users")
                await migrate_table(s_session, p_session, Project, "Projects")
                await migrate_table(s_session, p_session, Document, "Documents")
                await migrate_table(s_session, p_session, Transaction, "Transactions")
                logger.info("Data sync completed successfully.")
            except Exception as e:
                logger.error(f"Error during data sync: {e}")
                await p_session.rollback()
            finally:
                await s_session.close()
                await p_session.close()
    
    await sqlite_engine.dispose()

async def run_migrations():
    logger.info("Starting deployment workflow...")
    
    # 1. Initialize tables on the live server
    await init_db()
    logger.info("Schema initialized.")

    # 2. Automated Sync (SQLite -> Postgres)
    # This runs every time the server starts, ensuring your dev data is pushed to live
    await sync_data()

    # 3. Legacy Column Check (for safety)
    async with engine.begin() as conn:
        for column, col_type in [("sub_funding_sources", "TEXT DEFAULT '[]'"), ("expenditure_total_reported", "FLOAT DEFAULT 0.0")]:
            try:
                await conn.execute(text(f"ALTER TABLE transactions ADD COLUMN {column} {col_type}"))
                logger.info(f"Ensured {column} exists.")
            except Exception:
                pass

    # 4. Data Harmonization
    try:
        from migrate_3fs_v2 import migrate_to_3fs_v2
        await migrate_to_3fs_v2()
    except Exception as e:
        logger.warning(f"Skipping 3FS harmonization: {e}")

    # 5. Final JSON Safety Check
    logger.info("Running final JSON data safety checks...")
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Transaction))
        transactions = result.scalars().all()
        
        json_fields = [
            "commodity", "vcdp_sub_components", "lgas", 
            "threeFS_primary", "threeFS_sub_components", 
            "funding_sources", "sub_funding_sources", 
            "beneficiary_categories", "value_chain_segments", 
            "data_source", "supporting_documents"
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

    logger.info("Deployment workflow completed successfully.")

if __name__ == "__main__":
    import os
    import sys
    sys.path.append(os.getcwd())
    asyncio.run(run_migrations())

