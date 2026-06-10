import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.database import AsyncSessionLocal
from app.models import State, Transaction, User
from sqlalchemy import select, func

async def main():
    async with AsyncSessionLocal() as db:
        print("States in Meta Table:")
        result = await db.execute(select(State))
        for s in result.scalars().all():
            print(f" - {s.name}")
            
        print("\nUsers & States:")
        result = await db.execute(select(User.email, User.state))
        for email, state in result.all():
            print(f" - {email}: {state}")

if __name__ == "__main__":
    asyncio.run(main())
