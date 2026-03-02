import uuid
from datetime import datetime
from sqlalchemy import (
    String, Text, Float, Boolean, DateTime, ForeignKey, Enum as SAEnum,
    JSON, func
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.database import Base
import enum


# ─────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────

class UserRole(str, enum.Enum):
    NATIONAL_ADMIN = "NATIONAL_ADMIN"
    STATE_COORDINATOR = "STATE_COORDINATOR"
    VIEWER = "VIEWER"


class ProgrammePhase(str, enum.Enum):
    ORIGINAL = "Original (2013-2018)"
    FIRST_AF = "1st AF"
    SECOND_AF = "2nd AF"


class FiscalQuarter(str, enum.Enum):
    Q1 = "Q1"
    Q2 = "Q2"
    Q3 = "Q3"
    Q4 = "Q4"


class VCDPComponent(str, enum.Enum):
    COMP1 = "Component 1: Agricultural Market Development"
    COMP2 = "Component 2: Smallholder Productivity Enhancement"
    COMP3 = "Component 3: Programme Management and Coordination"


class ClimateFlag(str, enum.Enum):
    YES = "Yes"
    NO = "No"


# ─────────────────────────────────────────────
# User model
# ─────────────────────────────────────────────

class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    email: Mapped[str] = mapped_column(String(200), unique=True, nullable=False, index=True)
    phone: Mapped[str | None] = mapped_column(String(50))
    hashed_password: Mapped[str] = mapped_column(String, nullable=False)
    role: Mapped[UserRole] = mapped_column(SAEnum(UserRole), default=UserRole.STATE_COORDINATOR)
    state: Mapped[str | None] = mapped_column(String(100))  # None = national access
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    records: Mapped[list["Transaction"]] = relationship("Transaction", back_populates="entered_by_user")


# ─────────────────────────────────────────────
# State & LGA lookup tables
# ─────────────────────────────────────────────

class State(Base):
    __tablename__ = "states"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    code: Mapped[str] = mapped_column(String(10), unique=True, nullable=False)

    lgas: Mapped[list["LGA"]] = relationship("LGA", back_populates="state")


class LGA(Base):
    __tablename__ = "lgas"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    state_id: Mapped[str] = mapped_column(ForeignKey("states.id"), nullable=False)

    state: Mapped["State"] = relationship("State", back_populates="lgas")


# ─────────────────────────────────────────────
# VCDP Transaction record  (one row = one transaction)
# ─────────────────────────────────────────────

class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))

    # Fields 1–2: Ref ID / Project Name
    ref_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    project_name: Mapped[str] = mapped_column(String(300), nullable=False)

    # Field 3: Commodity (stored as JSON list)
    commodity: Mapped[list] = mapped_column(JSON, nullable=False, default=list)

    # Fields 4–5: Fiscal Years
    fy_awarded: Mapped[int | None] = mapped_column()
    fy_completed: Mapped[int | None] = mapped_column()

    # Field 6: Programme Phase (auto-derived from fy_awarded)
    programme_phase: Mapped[str | None] = mapped_column(String(50))

    # Field 7: Fiscal Quarter
    fiscal_quarter: Mapped[str | None] = mapped_column(String(10))

    # Fields 8–9: VCDP Component / Sub-Component
    vcdp_component: Mapped[str | None] = mapped_column(String(100))
    vcdp_sub_components: Mapped[list] = mapped_column(JSON, default=list)

    # Fields 10–11: State / LGA(s)
    state: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    lgas: Mapped[list] = mapped_column(JSON, nullable=False, default=list)

    # Fields 12–13: 3FS Primary / Sub-Component
    threeFS_primary: Mapped[list] = mapped_column(JSON, default=list)
    threeFS_sub_components: Mapped[list] = mapped_column(JSON, default=list)

    # Field 14: COFOG Code
    cofog_code: Mapped[str | None] = mapped_column(String(50))

    # Fields 15–16: Funding Source / Sub-Source
    funding_sources: Mapped[list] = mapped_column(JSON, default=list)

    # Field 17: Expenditure breakdown (stored as JSON object keyed by source)
    expenditure_fgn: Mapped[float] = mapped_column(Float, default=0.0)
    expenditure_state: Mapped[float] = mapped_column(Float, default=0.0)
    expenditure_ifad: Mapped[float] = mapped_column(Float, default=0.0)
    expenditure_oof: Mapped[float] = mapped_column(Float, default=0.0)
    expenditure_beneficiary: Mapped[float] = mapped_column(Float, default=0.0)
    expenditure_other: Mapped[float] = mapped_column(Float, default=0.0)
    expenditure_total: Mapped[float] = mapped_column(Float, default=0.0)

    # Field 18: Beneficiary data
    beneficiary_categories: Mapped[list] = mapped_column(JSON, default=list)
    beneficiary_total: Mapped[int | None] = mapped_column()
    beneficiary_male: Mapped[int | None] = mapped_column()
    beneficiary_female: Mapped[int | None] = mapped_column()
    beneficiary_youth_under35: Mapped[int | None] = mapped_column()

    # Field 19: Value Chain Segment
    value_chain_segments: Mapped[list] = mapped_column(JSON, default=list)

    # Field 20: Climate/Environment Flag
    climate_flag: Mapped[str | None] = mapped_column(String(5))

    # Fields 21–22: Data Source / Supporting Documents
    data_source: Mapped[str | None] = mapped_column(String(200))
    supporting_documents: Mapped[list] = mapped_column(JSON, default=list)

    # Fields 23–24: Entered By / Date + Notes
    entered_by: Mapped[str | None] = mapped_column(ForeignKey("users.id"))
    entered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    classification_notes: Mapped[str | None] = mapped_column(Text)

    entered_by_user: Mapped["User | None"] = relationship("User", back_populates="records")
