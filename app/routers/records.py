import math
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, or_
from typing import List
import pandas as pd
import io
from datetime import datetime
from app.database import get_db
from app.models import Transaction, UserRole
from app.models import User
from app.schemas import (
    TransactionCreate, TransactionRead, TransactionUpdate, PaginatedTransactions
)
from app.auth import require_active_user, get_current_user

router = APIRouter(prefix="/api/records", tags=["records"])


def _compute_total(data: dict) -> float:
    return sum([
        data.get("expenditure_fgn", 0),
        data.get("expenditure_state", 0),
        data.get("expenditure_ifad", 0),
        data.get("expenditure_oof", 0),
        data.get("expenditure_beneficiary", 0),
        data.get("expenditure_other", 0),
    ])


def _derive_phase(fy_awarded: int | None) -> str | None:
    if fy_awarded is None:
        return None
    if 2013 <= fy_awarded <= 2018:
        return "Original (2013-2018)"
    elif 2019 <= fy_awarded <= 2021:
        return "1st AF"
    else:
        return "2nd AF"


@router.get("/", response_model=PaginatedTransactions)
async def list_records(
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    search: str | None = Query(None),
    state: str | None = Query(None),
    fy_awarded: int | None = Query(None),
    vcdp_component: str | None = Query(None),
    threeFS_primary: str | None = Query(None),
    climate_flag: str | None = Query(None),
    current_user: User = Depends(require_active_user),
    db: AsyncSession = Depends(get_db),
):
    query = select(Transaction)

    if search:
        search_filter = or_(
            Transaction.ref_id.ilike(f"%{search}%"),
            Transaction.project_name.ilike(f"%{search}%")
        )
        query = query.where(search_filter)

    # State-based visibility: state users only see their own state
    if current_user.role == UserRole.STATE_COORDINATOR:
        query = query.where(Transaction.state == current_user.state)
    elif state and state != "all":
        query = query.where(Transaction.state == state)

    if fy_awarded:
        query = query.where(Transaction.fy_awarded == fy_awarded)
    if vcdp_component:
        query = query.where(Transaction.vcdp_component == vcdp_component)
    if climate_flag:
        query = query.where(Transaction.climate_flag == climate_flag)

    # Count total
    count_result = await db.execute(select(func.count()).select_from(query.subquery()))
    total = count_result.scalar_one()

    # Paginate
    query = query.offset((page - 1) * size).limit(size).order_by(Transaction.entered_at.desc())
    result = await db.execute(query)
    items = [TransactionRead.model_validate(t) for t in result.scalars().all()]

    return PaginatedTransactions(
        items=items,
        total=total,
        page=page,
        size=size,
        pages=math.ceil(total / size) if total else 1,
    )


@router.post("/", response_model=TransactionRead, status_code=201)
async def create_record(
    data: TransactionCreate,
    current_user: User = Depends(require_active_user),
    db: AsyncSession = Depends(get_db),
):
    # Hard validations
    total = _compute_total(data.model_dump())
    if total <= 0:
        raise HTTPException(status_code=422, detail="expenditure_total must be greater than 0")
    if not data.threeFS_primary:
        raise HTTPException(status_code=422, detail="At least one 3FS Primary component is required")
    if not data.lgas:
        raise HTTPException(status_code=422, detail="At least one LGA is required")
    if not data.data_source:
        raise HTTPException(status_code=422, detail="data_source is required")

    # State coordinators can only create records for their own state
    if current_user.role != UserRole.NATIONAL_ADMIN and data.state != current_user.state:
        raise HTTPException(status_code=403, detail="Cannot create records for another state")

    # Auto-derive phase
    phase = data.programme_phase or _derive_phase(data.fy_awarded)

    record = Transaction(
        **data.model_dump(exclude={"programme_phase"}),
        programme_phase=phase,
        expenditure_total=total,
        entered_by=current_user.id,
    )
    db.add(record)
    await db.commit()
    await db.refresh(record)
    return TransactionRead.model_validate(record)


@router.get("/{record_id}", response_model=TransactionRead)
async def get_record(
    record_id: str,
    current_user: User = Depends(require_active_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Transaction).where(Transaction.id == record_id))
    record = result.scalar_one_or_none()
    if not record:
        raise HTTPException(status_code=404, detail="Record not found")
    if current_user.role != UserRole.NATIONAL_ADMIN and record.state != current_user.state:
        raise HTTPException(status_code=403, detail="Access denied")
    return TransactionRead.model_validate(record)


@router.patch("/{record_id}", response_model=TransactionRead)
async def update_record(
    record_id: str,
    data: TransactionUpdate,
    current_user: User = Depends(require_active_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Transaction).where(Transaction.id == record_id))
    record = result.scalar_one_or_none()
    if not record:
        raise HTTPException(status_code=404, detail="Record not found")
    if current_user.role != UserRole.NATIONAL_ADMIN and record.state != current_user.state:
        raise HTTPException(status_code=403, detail="Access denied")

    for field, value in data.model_dump(exclude_unset=True).items():
        setattr(record, field, value)

    # Recompute total
    record.expenditure_total = _compute_total({
        "expenditure_fgn": record.expenditure_fgn,
        "expenditure_state": record.expenditure_state,
        "expenditure_ifad": record.expenditure_ifad,
        "expenditure_oof": record.expenditure_oof,
        "expenditure_beneficiary": record.expenditure_beneficiary,
        "expenditure_other": record.expenditure_other,
    })

    await db.commit()
    await db.refresh(record)
    return TransactionRead.model_validate(record)


@router.delete("/{record_id}", status_code=204)
async def delete_record(
    record_id: str,
    current_user: User = Depends(require_active_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Transaction).where(Transaction.id == record_id))
    record = result.scalar_one_or_none()
    if not record:
        raise HTTPException(status_code=404, detail="Record not found")
    if current_user.role != UserRole.NATIONAL_ADMIN and record.state != current_user.state:
        raise HTTPException(status_code=403, detail="Access denied")
    await db.delete(record)
    await db.commit()
@router.get("/dashboard/metrics")
async def get_dashboard_metrics(
    state: str | None = Query(None),
    fy_awarded: int | None = Query(None),
    vcdp_component: str | None = Query(None),
    threeFS_primary: str | None = Query(None),
    funding_group: str | None = Query(None),
    programme_phase: str | None = Query(None),
    current_user: User = Depends(require_active_user),
    db: AsyncSession = Depends(get_db),
):
    query = select(Transaction)
    if current_user.role == UserRole.STATE_COORDINATOR:
        query = query.where(Transaction.state == current_user.state)
    elif state and state != "all":
        query = query.where(Transaction.state == state)
    
    if fy_awarded:
        query = query.where(Transaction.fy_awarded == fy_awarded)
    if vcdp_component:
        query = query.where(Transaction.vcdp_component == vcdp_component)
    if threeFS_primary:
        query = query.where(Transaction.threeFS_primary.contains([threeFS_primary]))
    if programme_phase:
        query = query.where(Transaction.programme_phase == programme_phase)
    
    if funding_group:
        if funding_group == "domestic":
            query = query.where((Transaction.expenditure_fgn + Transaction.expenditure_state) > 0)
        elif funding_group == "international":
            query = query.where((Transaction.expenditure_ifad + Transaction.expenditure_oof) > 0)
        elif funding_group == "private":
            query = query.where((Transaction.expenditure_beneficiary + Transaction.expenditure_other) > 0)

    result = await db.execute(query)
    transactions = result.scalars().all()

    # Totals
    total_expenditure = sum(t.expenditure_total for t in transactions)
    total_count = len(transactions)
    climate_flagged = len([t for t in transactions if t.climate_flag == "Yes"])
    
    # 3FS Pie Data
    threefs_data = {}
    for t in transactions:
        for primary in t.threeFS_primary:
            threefs_data[primary] = threefs_data.get(primary, 0) + t.expenditure_total
    
    # Trend Data (2013-2025)
    trend_data = {year: 0 for year in range(2013, 2026)}
    for t in transactions:
        if t.fy_awarded and 2013 <= t.fy_awarded <= 2025:
            trend_data[t.fy_awarded] += t.expenditure_total
            
    # State Performance
    state_perf = {}
    for t in transactions:
        state_perf[t.state] = state_perf.get(t.state, 0) + t.expenditure_total

    # Funding Source Breakdown
    funding_sources = {
        "Domestic": 0,
        "International": 0,
        "Private": 0
    }
    for t in transactions:
        funding_sources["Domestic"] += (t.expenditure_fgn + t.expenditure_state)
        funding_sources["International"] += (t.expenditure_ifad + t.expenditure_oof)
        funding_sources["Private"] += (t.expenditure_beneficiary + t.expenditure_other)

    return {
        "kpis": {
            "total_expenditure": total_expenditure,
            "total_transactions": total_count,
            "climate_flagged_pct": (climate_flagged / total_count * 100) if total_count else 0,
            "active_states": len(set(t.state for t in transactions))
        },
        "charts": {
            "threefs": [{"name": k, "value": v} for k, v in threefs_data.items()],
            "trend": [{"year": k, "expenditure": v} for k, v in sorted(trend_data.items())],
            "state_performance": [{"name": k, "value": v} for k, v in sorted(state_perf.items(), key=lambda x: x[1], reverse=True)],
            "funding_sources": [{"name": k, "value": v} for k, v in funding_sources.items()]
        }
    }

@router.get("/export/excel")
async def export_excel(
    state: str | None = Query(None),
    fy_awarded: int | None = Query(None),
    vcdp_component: str | None = Query(None),
    threeFS_primary: str | None = Query(None),
    funding_group: str | None = Query(None),
    programme_phase: str | None = Query(None),
    token: str | None = Query(None),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    query = select(Transaction)
    if current_user.role == UserRole.STATE_COORDINATOR:
        query = query.where(Transaction.state == current_user.state)
    elif state and state != "all":
        query = query.where(Transaction.state == state)
    if fy_awarded:
        query = query.where(Transaction.fy_awarded == fy_awarded)
    if vcdp_component:
        query = query.where(Transaction.vcdp_component == vcdp_component)
    if threeFS_primary:
        query = query.where(Transaction.threeFS_primary.contains([threeFS_primary]))
    if programme_phase:
        query = query.where(Transaction.programme_phase == programme_phase)
    
    if funding_group:
        if funding_group == "domestic":
            query = query.where((Transaction.expenditure_fgn + Transaction.expenditure_state) > 0)
        elif funding_group == "international":
            query = query.where((Transaction.expenditure_ifad + Transaction.expenditure_oof) > 0)
        elif funding_group == "private":
            query = query.where((Transaction.expenditure_beneficiary + Transaction.expenditure_other) > 0)

    result = await db.execute(query)
    transactions = result.scalars().all()

    # --- Prepare Summary Data (Mirroring Dashboard Logic) ---
    total_expenditure = sum(t.expenditure_total for t in transactions)
    total_count = len(transactions)
    climate_flagged = len([t for t in transactions if t.climate_flag == "Yes"])
    unique_states = len(set(t.state for t in transactions))

    kpi_df = pd.DataFrame([
        {"Metric": "Total Expenditure (USD)", "Value": f"{total_expenditure:,.2f}"},
        {"Metric": "Total Transactions", "Value": total_count},
        {"Metric": "Active States", "Value": unique_states},
        {"Metric": "Climate Alignment %", "Value": f"{(climate_flagged / total_count * 100) if total_count else 0:.1f}%"},
    ])

    # 3FS Breakdown
    threefs_data = {}
    for t in transactions:
        for primary in t.threeFS_primary:
            threefs_data[primary] = threefs_data.get(primary, 0) + t.expenditure_total
    threefs_df = pd.DataFrame([{"Component": k, "Expenditure (USD)": v} for k, v in threefs_data.items()])

    # State Performance
    state_perf = {}
    for t in transactions:
        state_perf[t.state] = state_perf.get(t.state, 0) + t.expenditure_total
    state_df = pd.DataFrame([{"State": k, "Expenditure (USD)": v} for k, v in sorted(state_perf.items(), key=lambda x: x[1], reverse=True)])

    # Detailed Records
    record_data = []
    for t in transactions:
        record_data.append({
            "Ref ID": t.ref_id,
            "Project Name": t.project_name,
            "State": t.state,
            "FY Awarded": t.fy_awarded,
            "VCDP Component": t.vcdp_component,
            "3FS Primary": ", ".join(t.threeFS_primary),
            "Expenditure Total (USD)": t.expenditure_total,
            "IFAD Contribution": t.expenditure_ifad,
            "FGN Contribution": t.expenditure_fgn,
            "Beneficiaries Total": t.beneficiary_total,
            "Climate Flag": t.climate_flag,
            "Date Entered": t.entered_at.strftime("%Y-%m-%d %H:%M") if t.entered_at else "N/A"
        })
    records_df = pd.DataFrame(record_data)
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        kpi_df.to_excel(writer, index=False, sheet_name='Executive Summary')
        threefs_df.to_excel(writer, index=False, sheet_name='3FS Analysis')
        state_df.to_excel(writer, index=False, sheet_name='State Performance')
        records_df.to_excel(writer, index=False, sheet_name='Detailed Submissions')

        # --- Adding Visual Charts using openpyxl ---
        workbook = writer.book
        
        # 3FS Pie Chart
        if not threefs_df.empty:
            ws_3fs = workbook['3FS Analysis']
            from openpyxl.chart import PieChart, Reference
            pie = PieChart()
            labels = Reference(ws_3fs, min_col=1, min_row=2, max_row=len(threefs_df) + 1)
            data = Reference(ws_3fs, min_col=2, min_row=1, max_row=len(threefs_df) + 1)
            pie.add_data(data, titles_from_data=True)
            pie.set_categories(labels)
            pie.title = 'Expenditure by 3FS Component'
            ws_3fs.add_chart(pie, "D2")

        # State Bar Chart
        if not state_df.empty:
            ws_state = workbook['State Performance']
            from openpyxl.chart import BarChart, Reference
            bar = BarChart()
            bar.type = "col"
            bar.style = 10
            bar.title = "Expenditure by State"
            bar.y_axis.title = 'Expenditure (USD)'
            bar.x_axis.title = 'State'
            
            data = Reference(ws_state, min_col=2, min_row=1, max_row=len(state_df) + 1)
            cats = Reference(ws_state, min_col=1, min_row=2, max_row=len(state_df) + 1)
            bar.add_data(data, titles_from_data=True)
            bar.set_categories(cats)
            bar.shape = 4
            ws_state.add_chart(bar, "D2")
    
    output.seek(0)
    
    filename = f"VCDP_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    headers = {
        'Content-Disposition': f'attachment; filename="{filename}"'
    }
    
    return StreamingResponse(
        output,
        headers=headers,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
