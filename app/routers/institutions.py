from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.database import get_db
from app.models import Institution, User
from app.schemas import InstitutionCreate, InstitutionRead, InstitutionUpdate
from app.auth import require_national_admin, require_active_user
import uuid

router = APIRouter(prefix="/api/v1/institutions", tags=["institutions"])

@router.get("", response_model=list[InstitutionRead])
async def list_institutions(
    state: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_active_user)
):
    """List institutions, optionally filtered by state."""
    query = select(Institution)
    if state:
        query = query.where(Institution.state == state)
    
    result = await db.execute(query.order_by(Institution.code.asc()))
    return result.scalars().all()

@router.post("", response_model=InstitutionRead, status_code=status.HTTP_201_CREATED)
async def create_institution(
    data: InstitutionCreate,
    db: AsyncSession = Depends(get_db),
    _current_admin: User = Depends(require_national_admin),
):
    """Create a new institution (Admin Only)."""
    # Check if code already exists for this state
    existing = await db.execute(
        select(Institution).where(Institution.state == data.state, Institution.code == data.code)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Institution with code '{data.code}' already exists for {data.state}."
        )

    new_inst = Institution(
        id=str(uuid.uuid4()),
        state=data.state,
        code=data.code,
        name=data.name
    )
    db.add(new_inst)
    await db.commit()
    await db.refresh(new_inst)
    return new_inst

@router.put("/{inst_id}", response_model=InstitutionRead)
async def update_institution(
    inst_id: str,
    data: InstitutionUpdate,
    db: AsyncSession = Depends(get_db),
    _current_admin: User = Depends(require_national_admin)
):
    """Update an institution (Admin Only)."""
    result = await db.execute(select(Institution).where(Institution.id == inst_id))
    inst = result.scalar_one_or_none()
    
    if not inst:
        raise HTTPException(status_code=404, detail="Institution not found")
        
    if data.state is not None:
        inst.state = data.state
    if data.code is not None:
        inst.code = data.code
    if data.name is not None:
        inst.name = data.name
        
    await db.commit()
    await db.refresh(inst)
    return inst

@router.delete("/{inst_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_institution(
    inst_id: str,
    db: AsyncSession = Depends(get_db),
    _current_admin: User = Depends(require_national_admin)
):
    """Delete an institution (Admin Only)."""
    result = await db.execute(select(Institution).where(Institution.id == inst_id))
    inst = result.scalar_one_or_none()
    
    if not inst:
        raise HTTPException(status_code=404, detail="Institution not found")
        
    await db.delete(inst)
    await db.commit()
