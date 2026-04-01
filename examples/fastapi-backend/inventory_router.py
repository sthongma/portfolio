"""
Inventory Dashboard API endpoints.

Patterns demonstrated:
- Dynamic WHERE clause building from optional filters
- Whitelist-based sort columns (prevents SQL injection)
- OFFSET/FETCH pagination with total count
- Pydantic response models
- Multi-select filter expansion
"""

import os
from datetime import date
from typing import Optional

from fastapi import APIRouter, Query, Request
from sqlalchemy import text

from ..filter_utils import expand_multi
from ..queries import inventory as sql
from ..schemas.inventory import (
    BalanceTrendItem,
    BalanceTrendResponse,
    DailyMovementItem,
    DailyMovementResponse,
    InventoryFilterOptions,
    InventorySummaryKPIs,
    SkuBalanceItem,
    SkuBalanceListResponse,
    SkuMovementDetail,
)

router = APIRouter()

# Whitelist of allowed sort columns (prevents SQL injection)
_SORT_COLUMNS = {
    "sku_code": "sku_code",
    "warehouse_name": "warehouse_name",
    "current_balance": "current_balance",
    "latest_daily_change": "latest_daily_change",
    "latest_date": "latest_date",
}

_MOVEMENT_SORT_COLUMNS = {
    "operation_date": "operation_date",
    "sku_code": "sku_code",
    "warehouse_name": "warehouse_name",
    "daily_quantity_change": "daily_quantity_change",
    "balance": "balance",
}


def _schema() -> str:
    return os.getenv("DB_SCHEMA_GOLD", "gold")


def _build_where(
    warehouse: Optional[str] = None,
    sku_code: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    search: Optional[str] = None,
) -> tuple[str, dict]:
    """Build dynamic WHERE clause and params from optional filters."""
    conditions: list[str] = []
    params: dict = {}

    if date_from:
        conditions.append("operation_date >= :date_from")
        params["date_from"] = date_from
    if date_to:
        conditions.append("operation_date <= :date_to")
        params["date_to"] = date_to

    # Multi-select expansion: "WH-A,WH-B" -> "warehouse_name IN (:wh_0, :wh_1)"
    clause = expand_multi(warehouse, "warehouse_name", "warehouse", params)
    if clause:
        conditions.append(clause)

    clause = expand_multi(sku_code, "sku_code", "sku_code", params)
    if clause:
        conditions.append(clause)

    if search:
        conditions.append("sku_code LIKE :search")
        params["search"] = f"%{search}%"

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    return where, params


# -- Summary KPIs --

@router.get("/summary", response_model=InventorySummaryKPIs)
async def get_summary(
    request: Request,
    warehouse: Optional[str] = Query(None),
    sku_code: Optional[str] = Query(None),
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    search: Optional[str] = Query(None),
):
    where, params = _build_where(warehouse, sku_code, date_from, date_to, search)
    schema = _schema()

    query = sql.SUMMARY.format(schema=schema, where=where)
    engine = request.app.state.engine
    with engine.connect() as conn:
        row = conn.execute(text(query), params).mappings().fetchone()

    if not row:
        return InventorySummaryKPIs()
    return InventorySummaryKPIs(**row)


# -- Daily Movement (paginated) --

@router.get("/daily-movement", response_model=DailyMovementResponse)
async def get_daily_movement(
    request: Request,
    warehouse: Optional[str] = Query(None),
    sku_code: Optional[str] = Query(None),
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    search: Optional[str] = Query(None),
    sort_by: str = Query("operation_date"),
    sort_dir: str = Query("desc"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
):
    where, params = _build_where(warehouse, sku_code, date_from, date_to, search)
    schema = _schema()

    # Whitelist-based sort (never interpolates user input into SQL directly)
    col = _MOVEMENT_SORT_COLUMNS.get(sort_by, "operation_date")
    direction = "ASC" if sort_dir.lower() == "asc" else "DESC"
    order_by = f"{col} {direction}"

    offset = (page - 1) * page_size
    params["offset"] = offset
    params["page_size"] = page_size

    query = sql.DAILY_MOVEMENT.format(schema=schema, where=where, order_by=order_by)
    count_query = sql.DAILY_MOVEMENT_COUNT.format(schema=schema, where=where)

    engine = request.app.state.engine
    with engine.connect() as conn:
        count_params = {k: v for k, v in params.items() if k not in ("offset", "page_size")}
        total = conn.execute(text(count_query), count_params).scalar() or 0
        rows = conn.execute(text(query), params).mappings().fetchall()

    return DailyMovementResponse(
        items=[DailyMovementItem(**r) for r in rows],
        total_count=total,
        page=page,
        page_size=page_size,
    )


# -- SKU Balance List (paginated) --

@router.get("/sku-list", response_model=SkuBalanceListResponse)
async def get_sku_list(
    request: Request,
    warehouse: Optional[str] = Query(None),
    sku_code: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    sort_by: str = Query("current_balance"),
    sort_dir: str = Query("desc"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
):
    where, params = _build_where(warehouse=warehouse, sku_code=sku_code, search=search)
    schema = _schema()

    col = _SORT_COLUMNS.get(sort_by, "current_balance")
    direction = "ASC" if sort_dir.lower() == "asc" else "DESC"

    offset = (page - 1) * page_size
    params["offset"] = offset
    params["page_size"] = page_size

    query = sql.SKU_LIST.format(schema=schema, where=where, order_by=f"{col} {direction}")
    count_query = sql.SKU_LIST_COUNT.format(schema=schema, where=where)

    engine = request.app.state.engine
    with engine.connect() as conn:
        count_params = {k: v for k, v in params.items() if k not in ("offset", "page_size")}
        total = conn.execute(text(count_query), count_params).scalar() or 0
        rows = conn.execute(text(query), params).mappings().fetchall()

    return SkuBalanceListResponse(
        items=[SkuBalanceItem(**r) for r in rows],
        total_count=total,
        page=page,
        page_size=page_size,
    )


# -- Single SKU Detail --

@router.get("/detail/{sku_code}/{warehouse_name}", response_model=SkuMovementDetail)
async def get_sku_detail(request: Request, sku_code: str, warehouse_name: str):
    schema = _schema()
    params = {"sku_code": sku_code, "warehouse_name": warehouse_name}

    engine = request.app.state.engine
    with engine.connect() as conn:
        row = conn.execute(
            text(sql.SKU_DETAIL.format(schema=schema)), params
        ).mappings().fetchone()
        trend_rows = conn.execute(
            text(sql.SKU_BALANCE_TREND.format(schema=schema)), params
        ).mappings().fetchall()

    if not row:
        return SkuMovementDetail(trend=[])

    return SkuMovementDetail(**row, trend=[BalanceTrendItem(**r) for r in trend_rows])


# -- Filter Options --

@router.get("/filter-options", response_model=InventoryFilterOptions)
async def get_filter_options(request: Request):
    schema = _schema()
    engine = request.app.state.engine

    with engine.connect() as conn:
        wh_rows = conn.execute(
            text(sql.FILTER_OPTIONS_WAREHOUSES.format(schema=schema))
        ).fetchall()
        date_row = conn.execute(
            text(sql.FILTER_OPTIONS_DATE_RANGE.format(schema=schema))
        ).mappings().fetchone()

    return InventoryFilterOptions(
        warehouses=[r[0] for r in wh_rows],
        min_date=date_row["min_date"] if date_row else None,
        max_date=date_row["max_date"] if date_row else None,
    )
