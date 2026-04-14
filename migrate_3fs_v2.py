import asyncio
import json
import logging
from sqlalchemy import select
from app.database import AsyncSessionLocal
from app.models import Transaction

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("migrate_3fs_v2")

# Mapping from previous harmonized names to new Component structure
MAPPING_V2 = {
    "1. Food Production": "Component 1: Agricultural Development and Value Chains",
    "2. Food Supply Chains": "Component 1: Agricultural Development and Value Chains",
    "3. Food Environments": "Component 3: Nutrition and Health",
    "4. Food Utilisation": "Component 3: Nutrition and Health",
    "5. Enabling Environment": "Component 5: Climate Change and Natural Resources",
}

NEW_COMPONENTS = [
    "Component 1: Agricultural Development and Value Chains",
    "Component 2: Infrastructure for Food Systems",
    "Component 3: Nutrition and Health",
    "Component 4: Social Assistance",
    "Component 5: Climate Change and Natural Resources"
]

async def migrate_to_3fs_v2():
    logger.info("Starting migration to new 3FS Component structure...")
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
                # 1. If already in new structure, keep it
                if item in NEW_COMPONENTS:
                    if item not in new_list:
                        new_list.append(item)
                    continue
                
                # 2. Map old or legacy names
                mapped = MAPPING_V2.get(item)
                if mapped:
                    if mapped not in new_list:
                        new_list.append(mapped)
                    changed = True
                    logger.info(f"Mapping '{item}' -> '{mapped}' for record {tx.id}")
                else:
                    # 3. Handle numbers if they exist (e.g. "1. ..." -> Component 1)
                    if item.startswith("1."):
                        target = NEW_COMPONENTS[0]
                    elif item.startswith("2."):
                        # Defaulting to C1 as it contains agro-processing and linkages
                        target = NEW_COMPONENTS[0] 
                    elif item.startswith("3."):
                        target = NEW_COMPONENTS[2]
                    elif item.startswith("4."):
                        target = NEW_COMPONENTS[2]
                    elif item.startswith("5."):
                        target = NEW_COMPONENTS[4]
                    else:
                        target = None
                    
                    if target:
                        if target not in new_list:
                            new_list.append(target)
                        changed = True
                        logger.info(f"Numeric mapping '{item}' -> '{target}' for record {tx.id}")
                    else:
                        # Keep unmapped as is (e.g. if user already started typing new ones)
                        if item not in new_list:
                            new_list.append(item)
            
            if changed or len(new_list) != len(original_list):
                tx.threeFS_primary = new_list
                # Also reset sub-components as they likely won't match the new hierarchy
                tx.threeFS_sub_components = []
                update_count += 1
        
        if update_count > 0:
            await session.commit()
            logger.info(f"Migrated {update_count} records to 3FS v2 structure.")
        else:
            logger.info("No 3FS v2 migration needed.")

if __name__ == "__main__":
    asyncio.run(migrate_to_3fs_v2())
