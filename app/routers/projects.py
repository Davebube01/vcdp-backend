from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.database import get_db
from app.models import Project, User
from app.schemas import ProjectCreate, ProjectRead, ProjectUpdate
from app.auth import require_national_admin, require_active_user
import uuid

router = APIRouter(prefix="/api/v1/projects", tags=["projects"])


@router.get("", response_model=list[ProjectRead])
async def list_projects(
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_active_user)
):
    """List all available projects globally."""
    result = await db.execute(select(Project).order_by(Project.created_at.desc()))
    projects = result.scalars().all()
    return projects


@router.post("", response_model=ProjectRead, status_code=status.HTTP_201_CREATED)
async def create_project(
    data: ProjectCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_national_admin),
):
    """Create a new project (Admin Only)."""
    # Check if a project with the same code exists
    existing = await db.execute(select(Project).where(Project.activity_type_code == data.activity_type_code))
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="A project with this Activity Type Code already exists."
        )

    new_project = Project(
        id=str(uuid.uuid4()),
        activity_type_code=data.activity_type_code,
        name=data.name,
        vcdp_component=data.vcdp_component,
        created_by=current_user.id
    )
    db.add(new_project)
    await db.commit()
    await db.refresh(new_project)
    return new_project

@router.put("/{project_id}", response_model=ProjectRead)
async def update_project(
    project_id: str,
    data: ProjectUpdate,
    db: AsyncSession = Depends(get_db),
    _current_admin: User = Depends(require_national_admin)
):
    """Update a project's details (Admin Only)."""
    result = await db.execute(select(Project).where(Project.id == project_id))
    project = result.scalar_one_or_none()
    
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
        
    if data.activity_type_code is not None and data.activity_type_code != project.activity_type_code:
        existing = await db.execute(select(Project).where(Project.activity_type_code == data.activity_type_code))
        if existing.scalar_one_or_none():
            raise HTTPException(status_code=400, detail="Another project with this code already exists.")
        project.activity_type_code = data.activity_type_code
        
    if data.name is not None:
        project.name = data.name

    if data.vcdp_component is not None:
        project.vcdp_component = data.vcdp_component
        
    await db.commit()
    await db.refresh(project)
    return project

@router.delete("/{project_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_project(
    project_id: str,
    db: AsyncSession = Depends(get_db),
    _current_admin: User = Depends(require_national_admin)
):
    """Delete a project (Admin Only)."""
    result = await db.execute(select(Project).where(Project.id == project_id))
    project = result.scalar_one_or_none()
    
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
        
    await db.delete(project)
    await db.commit()

