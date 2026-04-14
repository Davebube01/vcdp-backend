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
        "Mainstreaming activities such as gender/youth, financial inclusion, E&CC and nutrition are cross-cutting",
    ],
    "Component 2: Smallholder Productivity Enhancement": [
        "Strengthening Farmers' Organisations",
        "Smallholder Production",
        "Mainstreaming activities such as gender/youth, financial inclusion, E&CC and nutrition are cross-cutting"
    ],
    "Component 3: Programme Management and Coordination": [
        "Procurement",
        "Knowledge Management & Sharing",
        "M&E",
        "Finance/Audit",
        "Admin",
    ],
}

THREEFS_COMPONENTS = {
    "Component 1: Agricultural Development and Value Chains": [
        "Production support (On-farm)",
        "Input supply & technologies",
        "Extension services",
        "Agro-processing",
        "Market linkages",
    ],
    "Component 2: Infrastructure for Food Systems": [
        "Rural roads or other transportation networks",
        "Market facilities/infrastructure",
        "Storage facilities/infrastructure",
        "Irrigation systems",
        "Processing centers",
        "Rural electrification support",
    ],
    "Component 3: Nutrition and Health": [
        "Nutrition-sensitive agriculture",
        "Food fortification",
        "Nutrition education",
        "Dietary diversity",
        "Food safety",
        "health services",
    ],
    "Component 4: Social Assistance": [
        "Cash transfer schemes",
        "Emergency food assistance",
        "Voucher programs",
        "Social safety nets",
        "School feeding programs",
        "Subsidies",
    ],
    "Component 5: Climate Change and Natural Resources": [
        "Climate adaptation",
        "Climate-smart agriculture",
        "Land management",
        "Biodiversity",
        "Risk management",
        "Climate mitigation",
        "Natural resource management",
    ],
}

FUNDING_SOURCES = {
    "Domestic Public Financing": [
        "FGN counterpart funding", 
        "State/LGA contribution"
    ],
    "International Development Financing": [
        "IFAD loan - Official Development Assistance (ODA)", 
        "Other Official Flows (OOF)"
    ],
    "Private Sector Financing": [
        "Beneficiary contribution", 
        "Capital market operators", 
        "Banking systems"
    ],
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

FISCAL_YEARS = list(range(2013, 2051))


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
