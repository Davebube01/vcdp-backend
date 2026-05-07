from datetime import datetime
from typing import Optional
from pydantic import BaseModel, EmailStr, field_validator
from pydantic import BaseModel, EmailStr, field_validator
from app.models import UserRole, TransactionStatus, Currency


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
    fiscal_quarter: list[str] = []
    vcdp_component: list[str] = []
    vcdp_sub_components: list[str] = []
    state: str
    lgas: list[str] = []
    threeFS_primary: list[str] = []
    threeFS_sub_components: list[str] = []
    cofog_code: Optional[str] = None
    cofog_divisions: list[str] = []
    cofog_groups: list[str] = []
    funding_sources: list[str] = []
    sub_funding_sources: list[str] = []
    expenditure_fgn: float = 0.0
    expenditure_state: float = 0.0
    expenditure_ifad: float = 0.0
    expenditure_ifad_loan: float = 0.0
    expenditure_ifad_grant: float = 0.0
    expenditure_oof: float = 0.0
    expenditure_beneficiary: float = 0.0
    expenditure_other: float = 0.0
    expenditure_private_sector: float = 0.0
    expenditure_value_chain: float = 0.0
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
    value_chain_segments_other: Optional[str] = None
    climate_flag: Optional[str] = None
    data_source: list[str] = []
    supporting_documents: list[str] = []
    unit: str = "Person"
    executing_agency: Optional[str] = None
    quarterly_beneficiary_data: dict = {}
    classification_notes: Optional[str] = None
    status: Optional[TransactionStatus] = TransactionStatus.PUBLISHED
    rejection_reason: Optional[str] = None
    record_type: str = "Actual"
    institution_code: Optional[str] = None
    activity_type_code: Optional[str] = None
    currency: Currency = Currency.USD
    exchange_rate: float = 1.0

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
    commodity: Optional[list[str]] = []
    fy_awarded: Optional[int] = None
    fy_completed: Optional[int] = None
    programme_phase: Optional[str] = None
    fiscal_quarter: Optional[list[str]] = []
    vcdp_component: Optional[list[str]] = []
    vcdp_sub_components: Optional[list[str]] = []
    state: str
    lgas: Optional[list[str]] = []
    threeFS_primary: Optional[list[str]] = []
    threeFS_sub_components: Optional[list[str]] = []
    cofog_code: Optional[str] = None
    cofog_divisions: Optional[list[str]] = []
    cofog_groups: Optional[list[str]] = []
    funding_sources: Optional[list[str]] = []
    sub_funding_sources: Optional[list[str]] = []
    expenditure_fgn: float = 0.0
    expenditure_state: float = 0.0
    expenditure_ifad: float = 0.0
    expenditure_ifad_loan: float = 0.0
    expenditure_ifad_grant: float = 0.0
    expenditure_oof: float = 0.0
    expenditure_beneficiary: float = 0.0
    expenditure_other: float = 0.0
    expenditure_private_sector: float = 0.0
    expenditure_value_chain: float = 0.0
    expenditure_total: float = 0.0
    expenditure_total_reported: float = 0.0
    beneficiary_categories: Optional[list[str]] = []
    beneficiary_total: Optional[int] = None
    beneficiary_male: Optional[int] = None
    beneficiary_female: Optional[int] = None
    beneficiary_youth_under35: Optional[int] = None
    beneficiary_plwd: Optional[int] = None
    value_chain_segments: Optional[list[str]] = []
    value_chain_segments_other: Optional[str] = None
    executing_agency: Optional[str] = None
    climate_flag: Optional[str] = None
    data_source: Optional[list[str]] = []
    supporting_documents: Optional[list[str]] = []
    unit: str = "Person"
    quarterly_beneficiary_data: dict = {}
    entered_by: Optional[str] = None
    entered_at: datetime
    classification_notes: Optional[str] = None
    status: TransactionStatus
    rejection_reason: Optional[str] = None
    record_type: str = "Actual"
    institution_code: Optional[str] = None
    activity_type_code: Optional[str] = None
    currency: Currency = Currency.USD
    exchange_rate: float = 1.0

    model_config = {"from_attributes": True}


class TransactionUpdate(BaseModel):
    project_name: Optional[str] = None
    commodity: Optional[list[str]] = None
    fy_awarded: Optional[int] = None
    fy_completed: Optional[int] = None
    programme_phase: Optional[str] = None
    fiscal_quarter: Optional[list[str]] = None
    vcdp_component: Optional[list[str]] = None
    vcdp_sub_components: Optional[list[str]] = None
    state: Optional[str] = None
    lgas: Optional[list[str]] = None
    threeFS_primary: Optional[list[str]] = None
    threeFS_sub_components: Optional[list[str]] = None
    cofog_code: Optional[str] = None
    cofog_divisions: Optional[list[str]] = None
    cofog_groups: Optional[list[str]] = None
    funding_sources: Optional[list[str]] = None
    sub_funding_sources: Optional[list[str]] = None
    expenditure_fgn: Optional[float] = None
    expenditure_state: Optional[float] = None
    expenditure_ifad: Optional[float] = None
    expenditure_ifad_loan: Optional[float] = None
    expenditure_ifad_grant: Optional[float] = None
    expenditure_oof: Optional[float] = None
    expenditure_beneficiary: Optional[float] = None
    expenditure_other: Optional[float] = None
    expenditure_private_sector: Optional[float] = None
    expenditure_value_chain: Optional[float] = None
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
    value_chain_segments_other: Optional[str] = None
    climate_flag: Optional[str] = None
    data_source: Optional[list[str]] = None
    supporting_documents: Optional[list[str]] = None
    unit: Optional[str] = None
    quarterly_beneficiary_data: Optional[dict] = None
    classification_notes: Optional[str] = None
    record_type: Optional[str] = None
    institution_code: Optional[str] = None
    activity_type_code: Optional[str] = None
    currency: Optional[Currency] = None
    exchange_rate: Optional[float] = None


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
    activity_type_code: str
    name: str
    vcdp_component: Optional[str] = None

class ProjectCreate(ProjectBase):
    pass

class ProjectUpdate(BaseModel):
    activity_type_code: Optional[str] = None
    name: Optional[str] = None
    vcdp_component: Optional[str] = None

class ProjectRead(ProjectBase):
    id: str
    created_by: str | None
    created_at: datetime
    model_config = {"from_attributes": True}


# ─────────────────────────────────────────────
# Institution Schemas
# ─────────────────────────────────────────────

class InstitutionBase(BaseModel):
    state: str
    code: str
    name: str

class InstitutionCreate(InstitutionBase):
    pass

class InstitutionUpdate(BaseModel):
    state: Optional[str] = None
    code: Optional[str] = None
    name: Optional[str] = None

class InstitutionRead(InstitutionBase):
    id: str
    created_at: datetime
    model_config = {"from_attributes": True}
