from datetime import datetime
from typing import Optional
from pydantic import BaseModel, EmailStr, field_validator
from pydantic import BaseModel, EmailStr, field_validator
from app.models import UserRole, TransactionStatus


# ─────────────────────────────────────────────
# Auth / User schemas
# ─────────────────────────────────────────────

class UserCreate(BaseModel):
    name: str
    email: EmailStr
    phone: Optional[str] = None
    password: str
    role: UserRole = UserRole.STATE_COORDINATOR
    state: Optional[str] = None  # Required for state roles


class UserRead(BaseModel):
    id: str
    name: str
    email: str
    phone: Optional[str]
    role: UserRole
    state: Optional[str]
    is_active: bool
    created_at: datetime

    model_config = {"from_attributes": True}


class UserUpdate(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    role: Optional[UserRole] = None
    state: Optional[str] = None
    is_active: Optional[bool] = None


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserRead


# ─────────────────────────────────────────────
# Transaction schemas
# ─────────────────────────────────────────────

class TransactionCreate(BaseModel):
    ref_id: str
    project_name: str
    commodity: list[str] = []
    fy_awarded: Optional[int] = None
    fy_completed: Optional[int] = None
    programme_phase: Optional[str] = None
    fiscal_quarter: Optional[str] = None
    vcdp_component: list[str] = []
    vcdp_sub_components: list[str] = []
    state: str
    lgas: list[str] = []
    threeFS_primary: list[str] = []
    threeFS_sub_components: list[str] = []
    cofog_code: Optional[str] = None
    funding_sources: list[str] = []
    sub_funding_sources: list[str] = []
    expenditure_fgn: float = 0.0
    expenditure_state: float = 0.0
    expenditure_ifad: float = 0.0
    expenditure_oof: float = 0.0
    expenditure_beneficiary: float = 0.0
    expenditure_other: float = 0.0
    expenditure_total_reported: float = 0.0
    beneficiary_categories: list[str] = []
    beneficiary_total: Optional[int] = None
    beneficiary_male: Optional[int] = None
    beneficiary_female: Optional[int] = None
    beneficiary_youth_under35: Optional[int] = None
    beneficiary_male_percentage: Optional[float] = None
    beneficiary_female_percentage: Optional[float] = None
    beneficiary_youth_percentage: Optional[float] = None
    beneficiary_plwd: Optional[int] = None
    value_chain_segments: list[str] = []
    climate_flag: Optional[str] = None
    data_source: list[str] = []
    supporting_documents: list[str] = []
    classification_notes: Optional[str] = None
    status: Optional[TransactionStatus] = TransactionStatus.PUBLISHED
    rejection_reason: Optional[str] = None

    @field_validator("expenditure_fgn", "expenditure_state", "expenditure_ifad",
                     "expenditure_oof", "expenditure_beneficiary", "expenditure_other")
    @classmethod
    def must_be_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError("Expenditure cannot be negative")
        return v

    @field_validator("classification_notes")
    @classmethod
    def max_30_words(cls, v: Optional[str]) -> Optional[str]:
        if v and len(v.split()) > 30:
            raise ValueError("Classification notes must be 30 words or fewer")
        return v


class TransactionRead(BaseModel):
    id: str
    ref_id: str
    project_name: str
    commodity: list[str]
    fy_awarded: Optional[int]
    fy_completed: Optional[int]
    programme_phase: Optional[str]
    fiscal_quarter: Optional[str]
    vcdp_component: list[str] = []
    vcdp_sub_components: list[str]
    state: str
    lgas: list[str]
    threeFS_primary: list[str]
    threeFS_sub_components: list[str]
    funding_sources: list[str]
    sub_funding_sources: list[str]
    expenditure_fgn: float
    expenditure_state: float
    expenditure_ifad: float
    expenditure_oof: float
    expenditure_beneficiary: float
    expenditure_other: float
    expenditure_total: float
    expenditure_total_reported: float
    beneficiary_categories: list[str]
    beneficiary_total: Optional[int]
    beneficiary_male: Optional[int]
    beneficiary_female: Optional[int]
    beneficiary_youth_under35: Optional[int]
    beneficiary_plwd: Optional[int]
    value_chain_segments: list[str]
    climate_flag: Optional[str]
    data_source: list[str]
    supporting_documents: list[str]
    entered_by: Optional[str]
    entered_at: datetime
    classification_notes: Optional[str]
    status: TransactionStatus
    rejection_reason: Optional[str]

    model_config = {"from_attributes": True}


class TransactionUpdate(BaseModel):
    project_name: Optional[str] = None
    commodity: Optional[list[str]] = None
    fy_awarded: Optional[int] = None
    fy_completed: Optional[int] = None
    programme_phase: Optional[str] = None
    fiscal_quarter: Optional[str] = None
    vcdp_component: Optional[list[str]] = None
    vcdp_sub_components: Optional[list[str]] = None
    state: Optional[str] = None
    lgas: Optional[list[str]] = None
    threeFS_primary: Optional[list[str]] = None
    threeFS_sub_components: Optional[list[str]] = None
    cofog_code: Optional[str] = None
    funding_sources: Optional[list[str]] = None
    sub_funding_sources: Optional[list[str]] = None
    expenditure_fgn: Optional[float] = None
    expenditure_state: Optional[float] = None
    expenditure_ifad: Optional[float] = None
    expenditure_oof: Optional[float] = None
    expenditure_beneficiary: Optional[float] = None
    expenditure_other: Optional[float] = None
    expenditure_total_reported: Optional[float] = None
    beneficiary_categories: Optional[list[str]] = None
    beneficiary_total: Optional[int] = None
    beneficiary_male: Optional[int] = None
    beneficiary_female: Optional[int] = None
    status: Optional[TransactionStatus] = None
    rejection_reason: Optional[str] = None
    beneficiary_youth_under35: Optional[int] = None
    beneficiary_plwd: Optional[int] = None
    value_chain_segments: Optional[list[str]] = None
    climate_flag: Optional[str] = None
    data_source: Optional[list[str]] = None
    supporting_documents: Optional[list[str]] = None
    classification_notes: Optional[str] = None


# ─────────────────────────────────────────────
# State / LGA schemas
# ─────────────────────────────────────────────

class LGARead(BaseModel):
    id: str
    name: str
    state_id: str
    model_config = {"from_attributes": True}


class StateRead(BaseModel):
    id: str
    name: str
    code: str
    lgas: list[LGARead] = []
    model_config = {"from_attributes": True}


# ─────────────────────────────────────────────
# Pagination wrapper
# ─────────────────────────────────────────────

class PaginatedTransactions(BaseModel):
    items: list[TransactionRead]
    total: int
    page: int
    size: int
    pages: int


# ─────────────────────────────────────────────
# Document Repository Schemas
# ─────────────────────────────────────────────

class DocumentBase(BaseModel):
    name: str
    state: str
    data_source: str

class DocumentCreate(DocumentBase):
    pass

class DocumentRead(DocumentBase):
    id: str
    filename: str
    file_path: str
    uploaded_by: str | None
    uploaded_at: datetime
    model_config = {"from_attributes": True}


# ─────────────────────────────────────────────
# Project Schemas
# ─────────────────────────────────────────────

class ProjectBase(BaseModel):
    ref_id: str
    name: str

class ProjectCreate(ProjectBase):
    pass

class ProjectUpdate(BaseModel):
    ref_id: str | None = None
    name: str | None = None

class ProjectRead(ProjectBase):
    id: str
    created_by: str | None
    created_at: datetime
    model_config = {"from_attributes": True}
