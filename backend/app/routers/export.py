"""数据分发接口：交付信封封存、投递接收端、回执对账与补证。

数据落盘确认后的批次是唯一可取内容；信封封存后摘要、编码、目标系统与通道身份即冻结。
投递只在本地模拟接收端发生，交付结论一律以回执对账为准，发送端 200 不作为完成依据。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.schemas import ActionResult, PageResult
from app.services.export import (
    ENCODINGS,
    SIMULATIONS,
    STAGE_ORDER,
    STAGE_VERDICTS,
    STATUSES,
    service,
)

router = APIRouter(prefix="/api/export", tags=["数据分发"])


class SealRequest(BaseModel):
    batch_id: int
    channel_id: int
    encoding: str = "UTF-8"
    simulation: str = "正常签收"
    sealed_by: str = "值班管理员"


class OperatorRequest(BaseModel):
    operator: str = "值班管理员"


class ReceiptRequest(BaseModel):
    stage: str
    verdict: str
    note: str = ""
    source: str = "人工补录"
    operator: str = "值班管理员"
    attempt_no: int | None = None
    channel_identity: str | None = None


class AppendRequest(BaseModel):
    rows: int = Field(default=2, ge=1, le=10)


@router.get("/channels", response_model=PageResult[dict])
def list_channels() -> PageResult[dict]:
    """分发通道身份列表：信封的通道归属以这里的通道身份为准。"""
    items = service.list_channels()
    return PageResult(items=items, total=len(items), page=1, size=max(len(items), 1))


@router.get("/batches", response_model=PageResult[dict])
def list_batches() -> PageResult[dict]:
    """可供封存的数据落盘确认批次。未确认批次不会出现在这里。"""
    items = service.list_batches()
    return PageResult(items=items, total=len(items), page=1, size=max(len(items), 1))


@router.get("/stats", response_model=dict)
def get_stats() -> dict[str, Any]:
    """信封对账总览：各交付状态计数、尚缺证明的信封数与传输次数。"""
    return service.stats()


@router.get("/options", response_model=dict)
def get_options() -> dict[str, Any]:
    """封存与补录时可选的编码、演练场景、状态与证明阶段。"""
    return {
        "encodings": list(ENCODINGS),
        "simulations": SIMULATIONS,
        "statuses": STATUSES,
        "stages": STAGE_ORDER,
        "stage_verdicts": STAGE_VERDICTS,
    }


@router.get("/envelopes", response_model=PageResult[dict])
def list_envelopes(
    channel: str | None = Query(default=None, description="按通道身份或通道编号筛选"),
    status: str | None = Query(default=None, description="按交付状态筛选"),
    missing: str | None = Query(default=None, description="只保留尚缺某阶段证明的信封"),
    sort: str = Query(default="stage_desc", description="time_desc、time_asc、stage_asc、stage_desc"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按通道、状态、缺失证明阶段筛选信封，并按时间或证明阶段排序。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    if missing and missing not in STAGE_ORDER:
        raise HTTPException(status_code=400, detail=f"证明阶段「{missing}」不在 {'、'.join(STAGE_ORDER)} 之列")
    if status and status not in STATUSES:
        raise HTTPException(status_code=400, detail=f"交付状态「{status}」不在允许范围内")
    if sort not in {"time_desc", "time_asc", "stage_asc", "stage_desc"}:
        raise HTTPException(status_code=400, detail="排序方式只支持 time_desc、time_asc、stage_asc、stage_desc")
    items, total = service.list_envelopes(
        channel=channel, status=status, missing=missing, sort=sort, page=page, size=size
    )
    return PageResult(items=items, total=total, page=page, size=size)


@router.post("/envelopes", response_model=ActionResult)
def seal_envelope(payload: SealRequest) -> ActionResult:
    """从已确认落盘批次取内容封存信封；同一摘要沿用既有交付身份，不新增数据份数。"""
    envelope, message, created = service.seal(
        batch_id=payload.batch_id,
        channel_id=payload.channel_id,
        encoding=payload.encoding,
        simulation=payload.simulation,
        sealed_by=payload.sealed_by,
    )
    if envelope is None:
        return ActionResult(ok=False, message=message)
    entry = service.detail(envelope)
    entry["existing"] = not created
    return ActionResult(ok=True, message=message, entry=entry)


@router.get("/envelopes/{envelope_id}", response_model=dict)
def get_envelope(envelope_id: int) -> dict[str, Any]:
    """信封明细：封存快照、传输记录、按阶段排列的回执与对账结论。"""
    envelope = service.envelope_by_id(envelope_id)
    if envelope is None:
        raise HTTPException(status_code=404, detail=f"交付信封 {envelope_id} 不存在或已归档")
    return service.detail(envelope)


@router.post("/envelopes/{envelope_id}/send", response_model=ActionResult)
def send_envelope(envelope_id: int, payload: OperatorRequest) -> ActionResult:
    """向本地模拟接收端投递；重送保持摘要与交付身份不变，发送端 200 不代表交付完成。"""
    envelope = service.envelope_by_id(envelope_id)
    if envelope is None:
        return ActionResult(ok=False, message=f"交付信封 {envelope_id} 不存在或已归档")
    updated, message = service.send(envelope, payload.operator)
    if updated is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=service.detail(updated))


@router.post("/envelopes/{envelope_id}/supplement", response_model=ActionResult)
def request_supplement(envelope_id: int, payload: OperatorRequest) -> ActionResult:
    """凭原信封要求接收端补发缺失证明；拒收结论不可被补证推翻。"""
    envelope = service.envelope_by_id(envelope_id)
    if envelope is None:
        return ActionResult(ok=False, message=f"交付信封 {envelope_id} 不存在或已归档")
    updated, message = service.request_supplement(envelope, payload.operator)
    if updated is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=service.detail(updated))


@router.post("/envelopes/{envelope_id}/receipts", response_model=ActionResult)
def record_receipt(envelope_id: int, payload: ReceiptRequest) -> ActionResult:
    """补录一条指定阶段的回执；跨通道回执或迟到低阶段证明会被标出并排除在对账之外。"""
    envelope = service.envelope_by_id(envelope_id)
    if envelope is None:
        return ActionResult(ok=False, message=f"交付信封 {envelope_id} 不存在或已归档")
    updated, message = service.record_receipt(
        envelope,
        stage=payload.stage,
        verdict=payload.verdict,
        note=payload.note,
        source=payload.source,
        operator=payload.operator,
        attempt_no=payload.attempt_no,
        channel_identity=payload.channel_identity,
    )
    if updated is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=service.detail(updated))


@router.post("/batches/{batch_id}/append", response_model=ActionResult)
def append_batch(batch_id: int, payload: AppendRequest) -> ActionResult:
    """演练用：往落盘批次追加内容，验证已封存信封的快照不随之变化。"""
    entry, message = service.append_batch_rows(batch_id, payload.rows)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
