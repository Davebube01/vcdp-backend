from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy import select
from app.database import get_db
from app.models import State, LGA
from app.schemas import StateRead, LGARead
from app.auth import require_active_user
from app.models import User

router = APIRouter(prefix="/api/meta", tags=["meta"])


# ── VCDP reference data (static) ──────────────────────────────────────────────

VCDP_COMPONENTS = {
    "Component 1: Agricultural Market Development": [
        "Value Addition & Market Linkages",
        "Market Infrastructure",
    ],
    "Component 2: Smallholder Productivity Enhancement": [
        "Strengthening Farmers' Organisations",
        "Smallholder Production",
    ],
    "Component 3: Programme Management and Coordination": [
        "Gender",
        "Youth",
        "Environment",
        "Knowledge Management",
    ],
}

THREEFS_COMPONENTS = {
    "1. Food Production": [
        "1.1 Crop production",
        "1.2 Livestock production",
        "1.3 Fisheries & aquaculture",
        "1.4 Forestry",
    ],
    "2. Food Supply Chains": [
        "2.1 Post-harvest handling & storage",
        "2.2 Processing & packaging",
        "2.3 Distribution & marketing",
        "2.4 Trade facilitation",
    ],
    "3. Food Environments": [
        "3.1 Access to markets",
        "3.2 Food safety & quality standards",
        "3.3 Consumer information",
    ],
    "4. Food Utilisation": [
        "4.1 Nutrition education",
        "4.2 Water, sanitation & hygiene",
        "4.3 Health services",
    ],
    "5. Enabling Environment": [
        "5.1 Policy & governance",
        "5.2 Research & development",
        "5.3 Capacity development",
        "5.4 Climate & environment",
    ],
}

FUNDING_SOURCES = {
    "Domestic": ["FGN (Federal Government of Nigeria)", "State/LGA Government"],
    "International": ["IFAD ODA", "IFAD OOF"],
    "Private": ["Beneficiary Contribution", "Capital Market", "Commercial Banks"],
}

VALUE_CHAIN_SEGMENTS = [
    "Production",
    "Input Supply",
    "Post-Harvest",
    "Processing",
    "Packaging",
    "Distribution/Marketing",
    "Other",
]

COMMODITIES = ["Rice", "Cassava", "Cross-cutting"]

FISCAL_YEARS = list(range(2013, 2026))


@router.get("/vcdp-components")
async def get_vcdp_components(_: User = Depends(require_active_user)):
    return VCDP_COMPONENTS


@router.get("/threefs-components")
async def get_threefs_components(_: User = Depends(require_active_user)):
    return THREEFS_COMPONENTS


@router.get("/funding-sources")
async def get_funding_sources(_: User = Depends(require_active_user)):
    return FUNDING_SOURCES


@router.get("/value-chain-segments")
async def get_value_chain_segments(_: User = Depends(require_active_user)):
    return VALUE_CHAIN_SEGMENTS


@router.get("/commodities")
async def get_commodities(_: User = Depends(require_active_user)):
    return COMMODITIES


@router.get("/fiscal-years")
async def get_fiscal_years(_: User = Depends(require_active_user)):
    return FISCAL_YEARS


# ── States & LGAs (from DB) ───────────────────────────────────────────────────

@router.get("/states", response_model=list[StateRead])
async def get_states(
    _: User = Depends(require_active_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(State).options(selectinload(State.lgas)).order_by(State.name)
    )
    return [StateRead.model_validate(s) for s in result.scalars().all()]


@router.get("/states/{state_id}/lgas", response_model=list[LGARead])
async def get_lgas_for_state(
    state_id: str,
    _: User = Depends(require_active_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(LGA).where(LGA.state_id == state_id).order_by(LGA.name)
    )
    return [LGARead.model_validate(l) for l in result.scalars().all()]
