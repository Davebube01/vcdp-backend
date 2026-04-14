import asyncio
import json
import logging
from sqlalchemy import select
from app.database import AsyncSessionLocal
from app.models import Transaction

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("migrate_3fs")

# Official Mapping based on THREEFS_COMPONENTS in meta.py
THREEFS_MAPPING = {
    "5. Governance": "5. Enabling Environment",
    "4. Enabling environment": "5. Enabling Environment",
    "2. Food processing": "2. Food Supply Chains",
    "3. Food social protection": "3. Food Environments",
    "Food processing": "2. Food Supply Chains",
    "Governance": "5. Enabling Environment",
    "Food Supply Chains": "2. Food Supply Chains",
    "Food Production": "1. Food Production",
    # Add other variations if needed
}

# The official keys from THREEFS_COMPONENTS
OFFICIAL_KEYS = [
    "1. Food Production",
    "2. Food Supply Chains",
    "3. Food Environments",
    "4. Food Utilisation",
    "5. Enabling Environment"
]

async def harmonize_3fs_names():
    logger.info("Starting 3FS name harmonization...")
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Transaction))
        transactions = result.scalars().all()
        
        update_count = 0
        for tx in transactions:
            if not tx.threeFS_primary:
                continue
                
            original_list = tx.threeFS_primary
            new_list = []
            changed = False
            
            for item in original_list:
                # 1. Check if it's already an official key
                if item in OFFICIAL_KEYS:
                    if item not in new_list:
                        new_list.append(item)
                    continue
                
                # 2. Try to map it
                mapped = THREEFS_MAPPING.get(item)
                if mapped:
                    if mapped not in new_list:
                        new_list.append(mapped)
                    changed = True
                    logger.info(f"Mapping '{item}' -> '{mapped}' for record {tx.id}")
                else:
                    # 3. If unmapped, either keep it (might still cause duplicates) 
                    # or try to find a partial match
                    found_partial = False
                    for official in OFFICIAL_KEYS:
                        # Extract the number prefix (e.g., "1.")
                        prefix = official.split(".")[0] + "."
                        if item.startswith(prefix):
                            if official not in new_list:
                                new_list.append(official)
                            changed = True
                            logger.info(f"Partial match '{item}' -> '{official}' for record {tx.id}")
                            found_partial = True
                            break
                    
                    if not found_partial:
                        if item not in new_list:
                            new_list.append(item)
            
            if changed or len(new_list) != len(original_list):
                tx.threeFS_primary = new_list
                update_count += 1
        
        if update_count > 0:
            await session.commit()
            logger.info(f"Harmonized 3FS names for {update_count} records.")
        else:
            logger.info("No 3FS name changes needed.")

if __name__ == "__main__":
    asyncio.run(harmonize_3fs_names())
