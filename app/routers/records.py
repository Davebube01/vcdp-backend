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
from app.models import TransactionStatus
from app.schemas import (
    TransactionCreate, TransactionRead, TransactionUpdate, PaginatedTransactions
)
from app.auth import require_active_user, get_current_user

router = APIRouter(prefix="/api/records", tags=["records"])

GLOBAL_EXCHANGE_RATE = 1550


def _compute_total(data: dict) -> float:
    # Use the sum of loan and grant if provided, otherwise use expenditure_ifad
    ifad_sum = data.get("expenditure_ifad_loan", 0) + data.get("expenditure_ifad_grant", 0)
    ifad_val = ifad_sum if ifad_sum > 0 else data.get("expenditure_ifad", 0)
    
    return sum([
        data.get("expenditure_fgn", 0),
        data.get("expenditure_state", 0),
        ifad_val,
        data.get("expenditure_oof", 0),
        data.get("expenditure_beneficiary", 0),
        data.get("expenditure_private_sector", 0),
        data.get("expenditure_value_chain", 0),
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
    status: TransactionStatus | None = Query(None),
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
        query = query.where(Transaction.vcdp_component.contains([vcdp_component]))
    if climate_flag:
        query = query.where(Transaction.climate_flag == climate_flag)
    if status:
        query = query.where(Transaction.status == status)

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

    # Duplicate Detection for records without Ref ID
    if not data.ref_id:
        duplicate_query = select(Transaction).where(
            Transaction.project_name == data.project_name,
            Transaction.state == data.state,
            Transaction.fy_awarded == data.fy_awarded,
            Transaction.expenditure_total_reported == data.expenditure_total_reported
        )
        existing = await db.execute(duplicate_query)
        if existing.scalar_one_or_none():
            raise HTTPException(status_code=400, detail="A similar record (same name, state, year, and amount) already exists.")

    # Auto-derive phase
    phase = data.programme_phase or _derive_phase(data.fy_awarded)

    if current_user.role != UserRole.NATIONAL_ADMIN:
        data.status = TransactionStatus.PENDING
    elif not data.status:
        data.status = TransactionStatus.DRAFT

    record = Transaction(
        **data.model_dump(exclude={"programme_phase"}),
        programme_phase=phase,
        expenditure_total=total,
        entered_by=current_user.id,
    )
    if current_user.role != UserRole.NATIONAL_ADMIN:
        record.status = TransactionStatus.PENDING

    try:
        db.add(record)
        await db.commit()
    except Exception as e:
        await db.rollback()
        # Check specifically for unique constraint on ref_id
        if "UNIQUE constraint failed: transactions.ref_id" in str(e):
            raise HTTPException(status_code=400, detail="A record with this Reference ID already exists.")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

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

    if current_user.role != UserRole.NATIONAL_ADMIN:
        data.status = TransactionStatus.PENDING

    for field, value in data.model_dump(exclude_unset=True).items():
        setattr(record, field, value)

    if current_user.role != UserRole.NATIONAL_ADMIN:
        record.status = TransactionStatus.PENDING

    # Recompute total
    record.expenditure_total = _compute_total({
        "expenditure_fgn": record.expenditure_fgn,
        "expenditure_state": record.expenditure_state,
        "expenditure_ifad": record.expenditure_ifad,
        "expenditure_ifad_loan": record.expenditure_ifad_loan,
        "expenditure_ifad_grant": record.expenditure_ifad_grant,
        "expenditure_oof": record.expenditure_oof,
        "expenditure_beneficiary": record.expenditure_beneficiary,
        "expenditure_private_sector": record.expenditure_private_sector,
        "expenditure_value_chain": record.expenditure_value_chain,
        "expenditure_other": record.expenditure_other,
    })

    try:
        await db.commit()
    except Exception as e:
        await db.rollback()
        # Check specifically for unique constraint on ref_id
        if "UNIQUE constraint failed: transactions.ref_id" in str(e):
            raise HTTPException(status_code=400, detail="A record with this Reference ID already exists.")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    await db.refresh(record)
    return TransactionRead.model_validate(record)


@router.put("/{record_id}/status", response_model=TransactionRead)
async def update_record_status(
    record_id: str,
    status: TransactionStatus,
    rejection_reason: str | None = Query(None),
    current_user: User = Depends(require_active_user),
    db: AsyncSession = Depends(get_db),
):
    if current_user.role != UserRole.NATIONAL_ADMIN:
        raise HTTPException(status_code=403, detail="Only National Admins can explicitly change status")
    
    result = await db.execute(select(Transaction).where(Transaction.id == record_id))
    record = result.scalar_one_or_none()
    if not record:
        raise HTTPException(status_code=404, detail="Record not found")
        
    record.status = status
    if status == TransactionStatus.REJECTED:
        record.rejection_reason = rejection_reason
    else:
        record.rejection_reason = None
        
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
    try:
        query = select(Transaction).where(Transaction.status == "PUBLISHED")
        if current_user.role == UserRole.STATE_COORDINATOR:
            query = query.where(Transaction.state == current_user.state)
        elif state and state != "all":
            query = query.where(Transaction.state == state)
        
        if fy_awarded:
            query = query.where(Transaction.fy_awarded == fy_awarded)
        if vcdp_component:
            # Using like for SQLite compatibility with JSON-as-text
            query = query.where(Transaction.vcdp_component.like(f'%"{vcdp_component}"%'))
        if threeFS_primary:
            query = query.where(Transaction.threeFS_primary.like(f'%"{threeFS_primary}"%'))
        if programme_phase:
            query = query.where(Transaction.programme_phase == programme_phase)
        
        if funding_group:
            if funding_group == "domestic":
                query = query.where((Transaction.expenditure_fgn + Transaction.expenditure_state + Transaction.expenditure_beneficiary) > 0)
            elif funding_group == "international":
                query = query.where((Transaction.expenditure_ifad + Transaction.expenditure_oof) > 0)
            elif funding_group == "private":
                query = query.where((Transaction.expenditure_private_sector + Transaction.expenditure_value_chain + Transaction.expenditure_other) > 0)

        result = await db.execute(query)
        transactions = result.scalars().all()

        # Totals
        total_expenditure = sum((t.expenditure_total or 0) for t in transactions)
        total_count = len(transactions)
        climate_flagged = len([t for t in transactions if t.climate_flag == "Yes"])
        
        # 3FS Pie Data
        from app.routers.meta import THREEFS_COMPONENTS
        threefs_data = {key: 0 for key in THREEFS_COMPONENTS.keys()}
        for t in transactions:
            if t.threeFS_primary:
                for primary in t.threeFS_primary:
                    if primary in threefs_data:
                        threefs_data[primary] += (t.expenditure_total or 0)
                    else:
                        threefs_data[primary] = (t.expenditure_total or 0)
        
        # Trend Data (2013-Current Year)
        import datetime
        current_year = datetime.datetime.now().year
        trend_data = {year: 0 for year in range(2013, current_year + 1)}
        for t in transactions:
            if t.fy_awarded and 2013 <= t.fy_awarded <= current_year:
                trend_data[t.fy_awarded] += (t.expenditure_total or 0)
                
        # State Performance
        state_perf = {}
        for t in transactions:
            state_perf[t.state] = state_perf.get(t.state, 0) + (t.expenditure_total or 0)

        # Funding Source Breakdown
        funding_sources = {
            "Domestic": 0,
            "International": 0,
            "Private": 0
        }
        for t in transactions:
            funding_sources["Domestic"] += ((t.expenditure_fgn or 0) + (t.expenditure_state or 0) + (t.expenditure_beneficiary or 0))
            funding_sources["International"] += ((t.expenditure_ifad or 0) + (t.expenditure_oof or 0))
            funding_sources["Private"] += ((t.expenditure_private_sector or 0) + (t.expenditure_value_chain or 0) + (t.expenditure_other or 0))

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
    except Exception as e:
        import logging
        logging.error(f"Error in get_dashboard_metrics: {e}")
        return {"error": "Internal server error during metrics calculation"}

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
    query = select(Transaction).where(Transaction.status == "PUBLISHED")
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
            query = query.where((Transaction.expenditure_fgn + Transaction.expenditure_state + Transaction.expenditure_beneficiary) > 0)
        elif funding_group == "international":
            query = query.where((Transaction.expenditure_ifad + Transaction.expenditure_oof) > 0)
        elif funding_group == "private":
            query = query.where((Transaction.expenditure_private_sector + Transaction.expenditure_value_chain + Transaction.expenditure_other) > 0)

    result = await db.execute(query)
    transactions = result.scalars().all()

    # --- Prepare Summary Data (Mirroring Dashboard Logic) ---
    def _to_usd(val, curr):
        if curr == "NGN":
            return val / GLOBAL_EXCHANGE_RATE
        return val

    total_expenditure = sum(_to_usd(t.expenditure_total or 0, t.currency) for t in transactions)
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
    from app.routers.meta import THREEFS_COMPONENTS
    threefs_data = {key: 0 for key in THREEFS_COMPONENTS.keys()}
    for t in transactions:
        usd_val = _to_usd(t.expenditure_total or 0, t.currency)
        for primary in (t.threeFS_primary or []):
            threefs_data[primary] = threefs_data.get(primary, 0) + usd_val
    threefs_df = pd.DataFrame([{"Component": k, "Expenditure (USD)": v} for k, v in threefs_data.items()])

    # State Performance
    state_perf = {}
    for t in transactions:
        usd_val = _to_usd(t.expenditure_total or 0, t.currency)
        state_perf[t.state] = state_perf.get(t.state, 0) + usd_val
    state_df = pd.DataFrame([{"State": k, "Expenditure (USD)": v} for k, v in sorted(state_perf.items(), key=lambda x: x[1], reverse=True)])

    # Detailed Records – full field coverage
    def _join(val):
        """Safely join list fields (they may be Python lists from JSON columns)."""
        if isinstance(val, list):
            return ", ".join(str(v) for v in val if v)
        return val or ""

    record_data = []
    for t in transactions:
        raw_total = t.expenditure_total or 0
        usd_total = _to_usd(raw_total, t.currency)

        record_data.append({
            # ── Identification ─────────────────────────────────────────────
            "Ref ID": t.ref_id or "",
            "Activity Code": t.activity_type_code or "",
            "Project / Activity Name": t.project_name,
            "Record Type": t.record_type or "Actual",
            "Status": t.status,
            "Date Entered": t.entered_at.strftime("%Y-%m-%d %H:%M") if t.entered_at else "N/A",

            # ── Location & Agency ──────────────────────────────────────────
            "State": t.state,
            "LGA(s)": _join(t.lgas),
            "Executing Agency": t.executing_agency or "",
            "Institution Code": t.institution_code or "",

            # ── Time & Programme ───────────────────────────────────────────
            "FY Awarded": t.fy_awarded,
            "FY Completed": t.fy_completed,
            "Programme Phase": t.programme_phase or "",
            "Fiscal Quarter": _join(t.fiscal_quarter),

            # ── Classification ─────────────────────────────────────────────
            "Commodity / Value Chain": _join(t.commodity),
            "VCDP Component": _join(t.vcdp_component),
            "VCDP Sub-Component(s)": _join(t.vcdp_sub_components),
            "3FS Primary": _join(t.threeFS_primary),
            "3FS Sub-Component(s)": _join(t.threeFS_sub_components),
            "COFOG Code": t.cofog_code or "",
            "COFOG Division(s)": _join(t.cofog_divisions) if t.cofog_divisions else "",
            "COFOG Group(s)": _join(t.cofog_groups) if t.cofog_groups else "",

            # ── Funding Sources ────────────────────────────────────────────
            "Funding Sources": _join(t.funding_sources),
            "Sub Funding Sources": _join(t.sub_funding_sources),

            # ── Expenditure (native currency) ──────────────────────────────
            "Currency": t.currency or "USD",
            "Exchange Rate (vs USD)": t.exchange_rate or 1.0,
            "Expenditure – FGN": t.expenditure_fgn or 0,
            "Expenditure – State": t.expenditure_state or 0,
            "Expenditure – IFAD Total": (t.expenditure_ifad or 0),
            "Expenditure – IFAD Loan": t.expenditure_ifad_loan or 0,
            "Expenditure – IFAD Grant": t.expenditure_ifad_grant or 0,
            "Expenditure – OOF": t.expenditure_oof or 0,
            "Expenditure – Beneficiary": t.expenditure_beneficiary or 0,
            "Expenditure – Private Sector": t.expenditure_private_sector or 0,
            "Expenditure – Value Chain": t.expenditure_value_chain or 0,
            "Expenditure – Other": t.expenditure_other or 0,
            "Expenditure – Total (Reported)": t.expenditure_total_reported or 0,
            "Expenditure – Total (Computed)": raw_total,
            "Expenditure – Total (USD)": round(usd_total, 2),

            # ── Beneficiaries ──────────────────────────────────────────────
            "Beneficiary Unit": t.unit or "Person",
            "Beneficiaries – Total": t.beneficiary_total or 0,
            "Beneficiaries – Male": t.beneficiary_male or 0,
            "Beneficiaries – Female": t.beneficiary_female or 0,
            "Beneficiaries – Youth (<35)": t.beneficiary_youth_under35 or 0,
            "Beneficiaries – PLWD": t.beneficiary_plwd or 0,

            # ── Other ──────────────────────────────────────────────────────
            "Value Chain Segment(s)": _join(t.value_chain_segments),
            "Climate Aligned": t.climate_flag or "No",
            "Data Source(s)": _join(t.data_source),
            "Classification Notes": t.classification_notes or "",
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


@router.get("/export/csv")
async def export_csv(
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
    query = select(Transaction).where(Transaction.status == "PUBLISHED")
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
            query = query.where((Transaction.expenditure_fgn + Transaction.expenditure_state + Transaction.expenditure_beneficiary) > 0)
        elif funding_group == "international":
            query = query.where((Transaction.expenditure_ifad + Transaction.expenditure_oof) > 0)
        elif funding_group == "private":
            query = query.where((Transaction.expenditure_private_sector + Transaction.expenditure_value_chain + Transaction.expenditure_other) > 0)

    result = await db.execute(query)
    transactions = result.scalars().all()

    def _join(val):
        if isinstance(val, list):
            return ", ".join(str(v) for v in val if v)
        return val or ""

    def _to_usd(val, curr):
        if curr == "NGN":
            return val / GLOBAL_EXCHANGE_RATE
        return val

    record_data = []
    for t in transactions:
        raw_total = t.expenditure_total or 0
        usd_total = _to_usd(raw_total, t.currency)
        record_data.append({
            "Ref ID": t.ref_id or "",
            "Activity Code": t.activity_type_code or "",
            "Project Name": t.project_name,
            "Record Type": t.record_type or "Actual",
            "Status": t.status,
            "Date Entered": t.entered_at.strftime("%Y-%m-%d %H:%M") if t.entered_at else "N/A",
            "State": t.state,
            "LGA(s)": _join(t.lgas),
            "Executing Agency": t.executing_agency or "",
            "Institution Code": t.institution_code or "",
            "FY Awarded": t.fy_awarded,
            "FY Completed": t.fy_completed,
            "Programme Phase": t.programme_phase or "",
            "Fiscal Quarter": _join(t.fiscal_quarter),
            "Commodity": _join(t.commodity),
            "VCDP Component": _join(t.vcdp_component),
            "VCDP Sub-Component(s)": _join(t.vcdp_sub_components),
            "3FS Primary": _join(t.threeFS_primary),
            "3FS Sub-Component(s)": _join(t.threeFS_sub_components),
            "COFOG Code": t.cofog_code or "",
            "COFOG Division(s)": _join(t.cofog_divisions) if t.cofog_divisions else "",
            "COFOG Group(s)": _join(t.cofog_groups) if t.cofog_groups else "",
            "Funding Sources": _join(t.funding_sources),
            "Sub Funding Sources": _join(t.sub_funding_sources),
            "Currency": t.currency or "USD",
            "Exchange Rate": t.exchange_rate or 1.0,
            "Expenditure Total (USD)": round(usd_total, 2),
            "Beneficiaries Total": t.beneficiary_total or 0,
            "Climate Aligned": t.climate_flag or "No",
        })
    
    df = pd.DataFrame(record_data)
    stream = io.StringIO()
    df.to_csv(stream, index=False)
    
    filename = f"VCDP_Export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    headers = {'Content-Disposition': f'attachment; filename="{filename}"'}
    
    return StreamingResponse(
        io.BytesIO(stream.getvalue().encode()),
        headers=headers,
        media_type='text/csv'
    )
