import asyncio
import json
import os
import sys
from datetime import datetime
from typing import List, Type

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

# Add project root to path
sys.path.append(os.getcwd())

from app.database import Base, PG_JSON
from app.models import State, LGA, User, Project, Document, Transaction, TransactionStatus, UserRole

def get_postgres_url():
    """Get Postgres URL from environment or user input."""
    # Check for LIVE_DATABASE_URL or just DATABASE_URL (if user updated it)
    url = os.getenv("LIVE_DATABASE_URL")
    if not url:
        print("\n" + "="*50)
        print("DATABASE MIGRATION TOOL: LOCAL SQLITE -> LIVE POSTGRES")
        print("="*50)
        url = input("\nPlease enter your LIVE POSTGRES DATABASE URL: ").strip()
    
    if not url:
        print("Error: No database URL provided. Exiting.")
        sys.exit(1)
        
    # Ensure correct async driver
    if url.startswith("postgres://") or url.startswith("postgresql://"):
        url = url.replace("postgres://", "postgresql+asyncpg://", 1).replace("postgresql://", "postgresql+asyncpg://", 1)
    
    return url

async def migrate_table(sqlite_session: AsyncSession, pg_session: AsyncSession, model: Type[Base], name: str):
    """Migrate all records for a specific model."""
    print(f"Migrating {name}...")
    
    # 1. Fetch from SQLite
    result = await sqlite_session.execute(select(model))
    items = result.scalars().all()
    
    if not items:
        print(f"  No records found for {name}. Skipping.")
        return

    count = 0
    for item in items:
        # Clone the item for Postgres
        # We use __dict__ but remove SQLAlchemy internal state
        data = {k: v for k, v in item.__dict__.items() if not k.startswith('_sa_')}
        
        # Merge ensures we handle existing IDs (upsert)
        await pg_session.merge(model(**data))
        count += 1
    
    await pg_session.commit()
    print(f"  Successfully migrated {count} records for {name}.")

async def run_migration():
    # 1. Setup Engines
    sqlite_url = "sqlite+aiosqlite:///./vcdp.db"
    sqlite_engine = create_async_engine(sqlite_url)
    SqliteSession = async_sessionmaker(sqlite_engine, expire_on_commit=False)
    
    pg_url = get_postgres_url()
    pg_engine = create_async_engine(pg_url)
    PgSession = async_sessionmaker(pg_engine, expire_on_commit=False)
    
    # 2. Initialize Tables in Postgres (Schema only)
    print("\nEnsuring tables exist on the live server...")
    async with pg_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("Remote schema ready.")

    async with SqliteSession() as s_session:
        async with PgSession() as p_session:
            try:
                # 3. Migrate in Dependency Order
                # Order: States -> LGAs -> Users -> Projects -> Documents -> Transactions
                await migrate_table(s_session, p_session, State, "States")
                await migrate_table(s_session, p_session, LGA, "LGAs")
                await migrate_table(s_session, p_session, User, "Users")
                await migrate_table(s_session, p_session, Project, "Projects")
                await migrate_table(s_session, p_session, Document, "Documents")
                await migrate_table(s_session, p_session, Transaction, "Transactions")
                
                print("\n" + "="*50)
                print("MIGRATION COMPLETE!")
                print("Your live database is now in sync with your local data.")
                print("="*50)
                
            except Exception as e:
                print(f"\nCRITICAL ERROR DURING MIGRATION: {e}")
                await p_session.rollback()
            finally:
                await s_session.close()
                await p_session.close()

    await sqlite_engine.dispose()
    await pg_engine.dispose()

if __name__ == "__main__":
    asyncio.run(run_migration())
