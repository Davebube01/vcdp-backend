import asyncio
import os
import sys
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

# Add current directory to path so 'app' can be imported
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.models import Transaction
from app.config import settings

# Database setup
engine = create_async_engine(
    settings.database_url,
    echo=True,
    connect_args={"check_same_thread": False} if "sqlite" in settings.database_url else {},
)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)

MAPPING = {
    "Component 1": "Component 1: Agricultural Market Development",
    "Component 2": "Component 2: Smallholder Productivity Enhancement",
    "Component 3": "Component 3: Programme Management and Coordination",
}

async def migrate():
    async with AsyncSessionLocal() as session:
        print("Starting migration of VCDP component names...")
        
        for old_name, new_name in MAPPING.items():
            result = await session.execute(
                update(Transaction)
                .where(Transaction.vcdp_component == old_name)
                .values(vcdp_component=new_name)
            )
            print(f"Updated records for {old_name} -> {new_name}. Rows affected: {result.rowcount}")
        
        await session.commit()
        print("Migration completed successfully!")

if __name__ == "__main__":
    asyncio.run(migrate())
