import asyncio
import uuid
import sys
import os
import bcrypt
import random
from datetime import datetime, timedelta

# Add current directory to path so 'app' can be imported
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.database import Base
from app.models import User, UserRole, State, LGA, Transaction
from app.config import settings

def simple_hash(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

# Database setup
engine = create_async_engine(
    settings.database_url,
    echo=True,
    connect_args={"check_same_thread": False} if "sqlite" in settings.database_url else {},
)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)

VCDP_STATES = [
    {"name": "Anambra", "code": "AN"},
    {"name": "Benue", "code": "BE"},
    {"name": "Ebonyi", "code": "EB"},
    {"name": "Enugu", "code": "EN"},
    {"name": "Kogi", "code": "KO"},
    {"name": "Nasarawa", "code": "NA"},
    {"name": "Niger", "code": "NI"},
    {"name": "Ogun", "code": "OG"},
    {"name": "Taraba", "code": "TA"},
    {"name": "FCT", "code": "FCT"},
]

# Sample LGAs (approx 6-7 per state to reach ~63)
STATED_LGAS = {
    "Anambra": ["Anyamelum", "Awka North", "Dunukofia", "Ogbaru", "Orumba North", "Orumba South"],
    "Benue": ["Guma", "Gwer East", "Logo", "Okpokwu", "Ogbadibo", "Agatu"],
    "Ebonyi": ["Afikpo South", "Abakaliki", "Izzi", "Ikwo", "Ishielu", "Ohaozara"],
    "Enugu": ["Aninri", "Enugu East", "Nkanu East", "Udenu", "Uzo-Uwani"],
    "Kogi": ["Ajaokuta", "Ibaji", "Idah", "Lokoja", "Olalamaboro"],
    "Nasarawa": ["Doma", "Karue", "Lafia", "Nasarawa", "Wamba"],
    "Niger": ["Bida", "Katcha", "Kontagora", "Shiroro", "Wushishi"],
    "Ogun": ["Obafemi Owode", "Ijebu North East", "Ifo", "Yewa North", "Odeda"],
    "Taraba": ["Ardo-Kola", "Gassol", "Jalingo", "Karim Lamido", "Wukari"],
    "FCT": ["Abaji", "Bwari", "Gwagwalada", "Kuje", "Kwali", "AMAC"],
}

async def seed():
    async with engine.begin() as conn:
        # Recreate tables
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    async with AsyncSessionLocal() as session:
        # Create admin
        admin = User(
            name="National Admin",
            email="admin@vcdp.org",
            hashed_password=simple_hash("admin123"),
            role=UserRole.NATIONAL_ADMIN,
        )
        session.add(admin)

        # Create states and LGAs
        for s_data in VCDP_STATES:
            state = State(name=s_data["name"], code=s_data["code"])
            session.add(state)
            await session.flush() # Get state ID

            lgas_list = STATED_LGAS.get(s_data["name"], [])
            for lga_name in lgas_list:
                lga = LGA(name=lga_name, state_id=state.id)
                session.add(lga)
        
        # Create a state coordinator for testing
        coord = User(
            name="Anambra Coordinator",
            email="anambra@vcdp.org",
            hashed_password=simple_hash("password123"),
            role=UserRole.STATE_COORDINATOR,
            state="Anambra"
        )
        session.add(coord)

        # Generate dummy transactions
        print("Generating dummy transactions...")
        components = [
            "Component 1: Agricultural Market Development",
            "Component 2: Smallholder Productivity Enhancement",
            "Component 3: Programme Management and Coordination"
        ]
        threefs_primaries = [
            "1. Food Production", 
            "2. Food processing", 
            "3. Food social protection", 
            "4. Enabling environment",
            "5. Governance"
        ]
        commodities = ["Rice", "Cassava"]
        segments = ["Production", "Processing", "Marketing"]
        
        # Get all states and LGAs from session
        result = await session.execute(select(State))
        states_objs = result.scalars().all()
        
        for i in range(100):
            state_obj = random.choice(states_objs)
            
            # Efficiently get LGAs for this state
            lga_result = await session.execute(select(LGA).where(LGA.state_id == state_obj.id))
            lgas_obj = lga_result.scalars().all()
            lga_names = [random.choice(lgas_obj).name] if lgas_obj else ["Sample LGA"]
            
            fy_awarded = random.randint(2013, 2024)
            fy_completed = fy_awarded + random.randint(1, 2)
            
            # Auto-derive phase
            if 2013 <= fy_awarded <= 2018:
                phase = "Original (2013-2018)"
            elif 2019 <= fy_awarded <= 2021:
                phase = "1st AF"
            else:
                phase = "2nd AF"
                
            fgn = random.uniform(10000, 50000)
            ifad = random.uniform(50000, 150000)
            state_exp = random.uniform(5000, 20000)
            total = fgn + ifad + state_exp
            
            trx = Transaction(
                ref_id=f"VCDP/{state_obj.code}/{fy_awarded}/{1000 + i}",
                project_name=f"Infrastructure Project {i+1} in {state_obj.name}",
                commodity=[random.choice(commodities)],
                fy_awarded=fy_awarded,
                fy_completed=fy_completed,
                programme_phase=phase,
                fiscal_quarter=random.choice(["Q1", "Q2", "Q3", "Q4"]),
                vcdp_component=random.choice(components),
                vcdp_sub_components=[f"Sub-component {random.randint(1,3)}"],
                state=state_obj.name,
                lgas=lga_names,
                threeFS_primary=[random.choice(threefs_primaries)],
                expenditure_fgn=fgn,
                expenditure_ifad=ifad,
                expenditure_state=state_exp,
                expenditure_total=total,
                climate_flag=random.choice(["Yes", "No"]),
                data_source="Seed Data",
                entered_by=admin.id,
                entered_at=datetime.now() - timedelta(days=random.randint(0, 365))
            )
            session.add(trx)

        await session.commit()
        print("Database seeded successfully!")

if __name__ == "__main__":
    asyncio.run(seed())
