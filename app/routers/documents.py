import os
import uuid
import shutil
from pathlib import Path
from typing import List, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, Query
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.database import get_db
from app.models import Document, User
from app.schemas import DocumentRead
from app.auth import get_current_user

router = APIRouter(prefix="/api/documents", tags=["documents"])

# Ensure upload directory exists
UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

@router.post("/upload", response_model=DocumentRead)
async def upload_document(
    name: str = Form(...),
    state: str = Form(...),
    data_source: str = Form(...),
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    # Create unique filename
    file_id = str(uuid.uuid4())
    suffix = Path(file.filename or "").suffix
    filename = f"{file_id}{suffix}"
    file_path = UPLOAD_DIR / filename

    # Save file
    try:
        with file_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not save file: {str(e)}")

    # Save metadata to DB
    new_doc = Document(
        id=file_id,
        name=name,
        filename=file.filename or "unnamed",
        file_path=str(file_path),
        state=state,
        data_source=data_source,
        uploaded_by=current_user.id
    )
    db.add(new_doc)
    await db.commit()
    await db.refresh(new_doc)
    return new_doc

@router.get("/", response_model=List[DocumentRead])
async def list_documents(
    state: Optional[str] = Query(None),
    data_source: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    query = select(Document)
    if state:
        query = query.where(Document.state == state)
    if data_source:
        query = query.where(Document.data_source == data_source)
    
    query = query.order_by(Document.uploaded_at.desc())
    result = await db.execute(query)
    return result.scalars().all()

@router.get("/{document_id}/file")
async def get_document_file(
    document_id: str,
    db: AsyncSession = Depends(get_db)
):
    query = select(Document).where(Document.id == document_id)
    result = await db.execute(query)
    doc = result.scalar_one_or_none()
    
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    
    if not os.path.exists(doc.file_path):
        raise HTTPException(status_code=404, detail="File not found on server")

    return FileResponse(
        path=doc.file_path,
        filename=doc.filename,
        media_type="application/octet-stream"
    )

@router.patch("/{document_id}", response_model=DocumentRead)
async def update_document(
    document_id: str,
    name: Optional[str] = Form(None),
    state: Optional[str] = Form(None),
    data_source: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    query = select(Document).where(Document.id == document_id)
    result = await db.execute(query)
    doc = result.scalar_one_or_none()
    
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")

    if name:
        doc.name = name
    if state:
        doc.state = state
    if data_source:
        doc.data_source = data_source
    
    if file:
        # Delete old file
        if os.path.exists(doc.file_path):
            os.remove(doc.file_path)
        
        # Save new file
        suffix = Path(file.filename or "").suffix
        filename = f"{doc.id}{suffix}"
        file_path = UPLOAD_DIR / filename
        
        try:
            with file_path.open("wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
            doc.filename = file.filename or "unnamed"
            doc.file_path = str(file_path)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Could not save file: {str(e)}")

    await db.commit()
    await db.refresh(doc)
    return doc

@router.delete("/{document_id}")
async def delete_document(
    document_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    query = select(Document).where(Document.id == document_id)
    result = await db.execute(query)
    doc = result.scalar_one_or_none()
    
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    
    # Optional: Check permissions
    # if doc.uploaded_by != current_user.id and current_user.role != "NATIONAL_ADMIN":
    #     raise HTTPException(status_code=403, detail="Not authorized")

    # Delete file
    if os.path.exists(doc.file_path):
        os.remove(doc.file_path)
    
    # Delete from DB
    await db.delete(doc)
    await db.commit()
    return {"status": "deleted"}
