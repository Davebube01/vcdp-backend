import asyncio
import json
import logging
from sqlalchemy import text
from app.database import engine, init_db, AsyncSessionLocal
from app.models import Transaction

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("deploy")

async def run_migrations():
    logger.info("Starting deployment migrations...")
    
    # 1. Initialize new tables (e.g., documents)
    await init_db()
    logger.info("Base tables initialized.")

    async with engine.begin() as conn:
        # 2. Add sub_funding_sources column if missing
        try:
            # Check if column exists (DB specific but we'll try/except)
            await conn.execute(text("ALTER TABLE transactions ADD COLUMN sub_funding_sources TEXT DEFAULT '[]'"))
            logger.info("Added sub_funding_sources column.")
        except Exception:
            logger.info("sub_funding_sources column already exists or could not be added.")

        # 3. Add expenditure_total_reported column if missing
        try:
            await conn.execute(text("ALTER TABLE transactions ADD COLUMN expenditure_total_reported FLOAT DEFAULT 0.0"))
            logger.info("Added expenditure_total_reported column.")
        except Exception:
            logger.info("expenditure_total_reported column already exists or could not be added.")

    # 4. Harmonize 3FS names (v2)
    logger.info("Harmonizing 3FS names to v2 structure...")
    from migrate_3fs_v2 import migrate_to_3fs_v2
    await migrate_to_3fs_v2()

    # 5. Repair JSON data
    logger.info("Checking for JSON data repairs...")
    async with AsyncSessionLocal() as session:
        from sqlalchemy import select
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
                
                # If it's a string, try to ensure it's a JSON array
                if val is None:
                    setattr(tx, field, [])
                    updated = True
                elif isinstance(val, str):
                    try:
                        parsed = json.loads(val)
                        if not isinstance(parsed, list):
                            setattr(tx, field, [parsed])
                            updated = True
                    except (json.JSONDecodeError, TypeError):
                        # It's a plain string, wrap in list
                        if val.strip() == "":
                            setattr(tx, field, [])
                        else:
                            setattr(tx, field, [val])
                        updated = True
                elif not isinstance(val, list):
                    # It's some other type (e.g. number/bool), wrap in list
                    setattr(tx, field, [val])
                    updated = True
            
            if updated:
                repair_count += 1
        
        if repair_count > 0:
            await session.commit()
            logger.info(f"Repaired JSON data for {repair_count} records.")
        else:
            logger.info("No JSON repairs needed.")

    logger.info("Migrations completed successfully.")

if __name__ == "__main__":
    asyncio.run(run_migrations())
