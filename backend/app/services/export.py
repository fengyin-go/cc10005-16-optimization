"""数据分发 · 交付信封对账。

数据落盘确认后的批次先被封存成「交付信封」：内容快照、摘要、编码格式、目标系统与
通道身份一起冻结，后续落盘的追加内容不会混入这份快照。投递只发生在本地模拟接收端，
对账以接收端回执为准，发送端的 200 响应不构成交付完成。

回执分三个阶段：送达证明 → 完整性验证 → 接收确认。回执按通道身份归属，
其他通道的确认不能抵扣本通道；拒收在发生阶段即为最终结论，之后迟到的低阶段证明
不参与对账。摘要相同的重送沿用同一交付身份，传输次数不等于数据份数。
"""
from __future__ import annotations

import copy
import hashlib
import json
import threading
from datetime import datetime, timezone
from typing import Any

from app.seed import SEED_DELIVERY_ENVELOPES, SEED_STORAGE_BATCHES
from app.store import store

ENCODINGS = {
    "UTF-8": "utf-8",
    "GB18030": "gb18030",
    "UTF-16LE": "utf-16-le",
    "ASCII": "ascii",
}

STAGE_ORDER = ["送达证明", "完整性验证", "接收确认"]
STAGE_RANK = {stage: index + 1 for index, stage in enumerate(STAGE_ORDER)}
STAGE_VERDICTS = {
    "送达证明": ["已送达", "遗失", "拒收"],
    "完整性验证": ["摘要一致", "摘要不一致", "拒收"],
    "接收确认": ["已接收", "拒收", "未接收"],
}
REJECT_VERDICTS = {"拒收", "摘要不一致", "未接收"}

STATUS_PENDING_SEND = "待投递"
STATUS_REJECTED = "已拒收"
STATUS_LOST = "已遗失"
STATUS_LANDED_UNCONFIRMED = "已落盘待确认"
STATUS_PENDING_PROOF = "待补证"
STATUS_ACCEPTED = "已确认接收"
STATUSES = [
    STATUS_PENDING_SEND,
    STATUS_PENDING_PROOF,
    STATUS_LANDED_UNCONFIRMED,
    STATUS_ACCEPTED,
    STATUS_REJECTED,
    STATUS_LOST,
]

SIMULATIONS = ["正常签收", "传输遗失", "完整性拒收", "已落盘未确认"]
_SIMULATION_ALIAS = {
    "正常签收": "normal",
    "传输遗失": "lossy",
    "完整性拒收": "rejects",
    "已落盘未确认": "landed_unconfirmed",
    "duplicate_accept": "duplicate_accept",
}


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parse_time(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return ""
    return text


def canonical_bytes(content: list[dict[str, Any]], encoding: str) -> bytes:
    """按字段排序的规范 JSON 序列化，再按信封声明的编码生成字节。"""
    text = json.dumps(content, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return text.encode(ENCODINGS[encoding])


def digest_of(content: list[dict[str, Any]], encoding: str) -> tuple[str, int, str]:
    payload = canonical_bytes(content, encoding)
    return hashlib.sha256(payload).hexdigest(), len(payload), payload.decode(ENCODINGS[encoding])


def short_digest(digest: str) -> str:
    return f"{digest[:8]}…{digest[-8:]}"


class LocalReceiver:
    """本地模拟接收端：按摘要去重落盘，回执由它签发，确认可能迟到或遗失。

    落盘是幂等的：同一通道收到相同摘要，沿用既有落盘编号，不新增数据份数。
    """

    def __init__(self) -> None:
        self._intakes: dict[tuple[str, str], dict[str, Any]] = {}
        self._lock = threading.Lock()

    def prime(self, envelope: dict[str, Any]) -> None:
        if not envelope.get("receiver_landed"):
            return
        key = (envelope["channel_identity"], envelope["digest"])
        self._intakes.setdefault(key, {
            "landing_ref": envelope.get("landing_ref") or f"LAND-{envelope['envelope_no'][-4:]}-01",
            "digest": envelope["digest"],
            "row_count": envelope["row_count"],
            "byte_size": envelope["byte_size"],
            "received_at": envelope.get("sealed_at"),
            "attempts": [],
        })

    def lookup(self, channel_identity: str, digest: str) -> dict[str, Any] | None:
        return self._intakes.get((channel_identity, digest))

    def intake(
        self,
        *,
        channel_identity: str,
        digest: str,
        row_count: int,
        byte_size: int,
        attempt_no: int,
        received_at: str,
        corrupted: bool,
    ) -> dict[str, Any]:
        with self._lock:
            key = (channel_identity, digest)
            existing = self._intakes.get(key)
            if existing is None:
                existing = {
                    "landing_ref": f"LAND-{digest[:6]}-{len(self._intakes) + 1:02d}",
                    "digest": digest,
                    "row_count": row_count,
                    "byte_size": byte_size,
                    "received_at": received_at,
                    "attempts": [],
                    "duplicate_intakes": 0,
                }
                self._intakes[key] = existing
            else:
                existing["duplicate_intakes"] = existing.get("duplicate_intakes", 0) + 1
            existing["attempts"].append(attempt_no)
            return existing

    def intake_count(self) -> int:
        return len(self._intakes)


class DeliveryService:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._batches: list[dict[str, Any]] = [
            {
                **copy.deepcopy(batch),
                "content": copy.deepcopy(batch["content"]),
            }
            for batch in SEED_STORAGE_BATCHES
        ]
        self._envelopes: dict[int, dict[str, Any]] = {}
        self._next_envelope_id = 1
        self._next_receipt_id = 1
        self.receiver = LocalReceiver()
        for seed in SEED_DELIVERY_ENVELOPES:
            self._load_seed(seed)

    # ---------- 初始化 ----------

    def _load_seed(self, seed: dict[str, Any]) -> None:
        batch = self._batch(seed["batch_id"])
        if batch is None:
            return
        channel = self._channel(seed["channel_id"])
        if channel is None:
            return
        encoding = seed["encoding"]
        digest, byte_size, payload_text = digest_of(batch["content"], encoding)
        envelope = {
            "id": self._next_envelope_id,
            "envelope_no": seed["envelope_no"],
            "delivery_identity": f"DLV-{digest[:12]}-{channel['通道身份']}",
            "batch_id": batch["id"],
            "batch_no": batch["batch_no"],
            "dataset": batch["dataset"],
            "channel_id": channel["id"],
            "channel_no": channel["通道编号"],
            "channel_name": channel.get("通道名称") or channel["输出数据集"],
            "channel_identity": channel["通道身份"],
            "target_system": channel["目标系统"],
            "channel_protocol": channel["传输协议"],
            "encoding": encoding,
            "digest": digest,
            "short_digest": short_digest(digest),
            "byte_size": byte_size,
            "row_count": len(batch["content"]),
            "sealed_at": seed["sealed_at"],
            "sealed_by": seed["sealed_by"],
            "simulation": seed.get("profile", "normal"),
            "sealed_content": copy.deepcopy(batch["content"]),
            "payload_text": payload_text,
            "sealed_batch_rows": len(batch["content"]),
            "receiver_landed": bool(seed.get("receiver_landed")),
            "landing_ref": seed.get("landing_ref"),
            "attempts": [copy.deepcopy(item) for item in seed.get("attempts", [])],
            "receipts": [],
            "actions": [],
            "seal_note": "封存快照与已确认落盘批次绑定，后续追加内容不进入本快照",
        }
        for receipt in seed.get("receipts", []):
            self._append_receipt(envelope, {
                **copy.deepcopy(receipt),
                "evidence": self._fill_evidence(receipt.get("evidence"), digest),
                "channel_identity": receipt.get("channel_identity", channel["通道身份"]),
            })
        self._next_envelope_id += 1
        self._envelopes[envelope["id"]] = envelope
        self.receiver.prime(envelope)

    @staticmethod
    def _fill_evidence(evidence: Any, digest: str) -> dict[str, Any]:
        if not isinstance(evidence, dict):
            return {}
        filled: dict[str, Any] = {}
        for key, value in evidence.items():
            filled[key] = digest if value == "{digest}" else value
        return filled

    def _append_receipt(self, envelope: dict[str, Any], receipt: dict[str, Any]) -> dict[str, Any]:
        row = {
            "id": self._next_receipt_id,
            "stage": receipt["stage"],
            "verdict": receipt["verdict"],
            "attempt_no": receipt.get("attempt_no"),
            "recorded_at": _parse_time(receipt.get("recorded_at")) or _now(),
            "source": receipt.get("source") or "接收端回执",
            "channel_identity": receipt.get("channel_identity") or envelope["channel_identity"],
            "note": receipt.get("note") or "",
            "evidence": receipt.get("evidence") or {},
            "origin": receipt.get("origin") or ("演练种子" if receipt.get("source") != "人工补录" else "人工补录"),
        }
        self._next_receipt_id += 1
        envelope["receipts"].append(row)
        return row

    # ---------- 查询基础 ----------

    def _batch(self, batch_id: int) -> dict[str, Any] | None:
        for batch in self._batches:
            if batch["id"] == batch_id:
                return batch
        return None

    def _channel(self, channel_id: int) -> dict[str, Any] | None:
        for row in store.rows("export"):
            if int(row.get("id", 0)) == channel_id:
                return row
        return None

    def _channel_by_identity(self, identity: str) -> dict[str, Any] | None:
        for row in store.rows("export"):
            if row.get("通道身份") == identity:
                return row
        return None

    def list_channels(self) -> list[dict[str, Any]]:
        return [
            {
                "id": row["id"],
                "通道编号": row["通道编号"],
                "通道名称": row.get("通道名称") or row["输出数据集"],
                "通道身份": row["通道身份"],
                "目标系统": row["目标系统"],
                "分发格式": row.get("分发格式"),
                "传输协议": row.get("传输协议"),
                "状态": row.get("status"),
            }
            for row in store.rows("export")
        ]

    def list_batches(self) -> list[dict[str, Any]]:
        return [
            {
                "id": batch["id"],
                "batch_no": batch["batch_no"],
                "dataset": batch["dataset"],
                "storage_path": batch["storage_path"],
                "file_format": batch["file_format"],
                "encoding": batch["encoding"],
                "confirmed_at": batch["confirmed_at"],
                "confirmed_by": batch["confirmed_by"],
                "status": batch["status"],
                "row_count": len(batch["content"]),
                "digest": digest_of(batch["content"], batch["encoding"])[0],
            }
            for batch in self._batches
            if batch["status"] == "已确认"
        ]

    # ---------- 封存 ----------

    def seal(
        self,
        *,
        batch_id: int,
        channel_id: int,
        encoding: str,
        simulation: str,
        sealed_by: str,
    ) -> tuple[dict[str, Any] | None, str, bool]:
        with self._lock:
            batch = self._batch(batch_id)
            if batch is None:
                return None, f"落盘批次 {batch_id} 不存在", False
            if batch["status"] != "已确认":
                return None, f"落盘批次 {batch['batch_no']} 尚未确认，不能封存分发", False
            channel = self._channel(channel_id)
            if channel is None:
                return None, f"分发通道 {channel_id} 不存在或已归档", False
            if channel.get("status") != "已启用":
                return None, f"分发通道 {channel['通道编号']} 未启用，不能投递", False
            if not channel.get("通道身份"):
                return None, "分发通道缺少通道身份，无法归属回执", False
            if encoding not in ENCODINGS:
                return None, f"编码格式「{encoding}」不在支持范围内：{'、'.join(ENCODINGS)}", False

            simulation_key = _SIMULATION_ALIAS.get(simulation, simulation)
            if simulation_key not in {"normal", "lossy", "rejects", "landed_unconfirmed", "duplicate_accept"}:
                return None, f"接收端演练场景「{simulation}」不在允许范围内", False

            digest, byte_size, payload_text = digest_of(batch["content"], encoding)
            delivery_identity = f"DLV-{digest[:12]}-{channel['通道身份']}"
            for envelope in self._envelopes.values():
                if envelope["delivery_identity"] == delivery_identity and envelope["encoding"] == encoding:
                    return envelope, (
                        f"摘要与既有信封 {envelope['envelope_no']} 相同，沿用同一交付身份 "
                        f"{delivery_identity}，不再新增一份数据"
                    ), False

            envelope_id = self._next_envelope_id
            self._next_envelope_id += 1
            envelope = {
                "id": envelope_id,
                "envelope_no": f"ENV-{datetime.now(timezone.utc):%Y%m%d}-{envelope_id:04d}",
                "delivery_identity": delivery_identity,
                "batch_id": batch["id"],
                "batch_no": batch["batch_no"],
                "dataset": batch["dataset"],
                "channel_id": channel["id"],
                "channel_no": channel["通道编号"],
                "channel_name": channel.get("通道名称") or channel["输出数据集"],
                "channel_identity": channel["通道身份"],
                "target_system": channel["目标系统"],
                "channel_protocol": channel["传输协议"],
                "encoding": encoding,
                "digest": digest,
                "short_digest": short_digest(digest),
                "byte_size": byte_size,
                "row_count": len(batch["content"]),
                "sealed_at": _now(),
                "sealed_by": sealed_by,
                "simulation": simulation_key,
                "sealed_content": copy.deepcopy(batch["content"]),
                "payload_text": payload_text,
                "sealed_batch_rows": len(batch["content"]),
                "receiver_landed": False,
                "landing_ref": None,
                "attempts": [],
                "receipts": [],
                "actions": [{
                    "at": _now(),
                    "by": sealed_by,
                    "action": "封存信封",
                    "detail": f"从落盘批次 {batch['batch_no']} 取内容，按 {encoding} 生成摘要 {short_digest(digest)}",
                }],
                "seal_note": "封存快照与已确认落盘批次绑定，后续追加内容不进入本快照",
            }
            self._envelopes[envelope["id"]] = envelope
            return envelope, f"信封 {envelope['envelope_no']} 已封存，交付身份 {delivery_identity}", True

    # ---------- 投递 ----------

    def _simulation_for_attempt(self, envelope: dict[str, Any], attempt_no: int) -> str:
        simulation = envelope["simulation"]
        if simulation == "lossy" and attempt_no == 1:
            return "lossy"
        if simulation == "lossy":
            return "normal"
        if simulation == "rejects":
            return "rejects"
        if simulation == "landed_unconfirmed":
            return "landed_unconfirmed"
        return "normal"

    def send(self, envelope: dict[str, Any], operator: str) -> tuple[dict[str, Any] | None, str]:
        reconciled = self.reconcile(envelope)
        if reconciled["status"] == STATUS_REJECTED:
            return None, "信封已被接收端拒收，拒收结论不可被重送推翻；请按拒收阶段重新处理数据"
        if reconciled["status"] == STATUS_ACCEPTED:
            return None, "本交付身份已取得有效接收确认，无需再次投递"
        with self._lock:
            attempt_no = len(envelope["attempts"]) + 1
            sent_at = _now()
            transport_id = f"TRN-{envelope['envelope_no'][-4:]}-{attempt_no:02d}"
            envelope["attempts"].append({
                "attempt_no": attempt_no,
                "transport_id": transport_id,
                "sent_at": sent_at,
                "sender_status": 200,
                "note": "发送端 200 仅代表载荷已交给传输层，不代表接收端已交付",
            })
            behavior = self._simulation_for_attempt(envelope, attempt_no)
            receipts, landed_ref = self._deliver(envelope, attempt_no, behavior)
            if landed_ref:
                envelope["receiver_landed"] = True
                envelope["landing_ref"] = landed_ref
            for receipt in receipts:
                self._append_receipt(envelope, receipt)
            envelope["actions"].append({
                "at": _now(),
                "by": operator,
                "action": "原样重送" if attempt_no > 1 else "投递接收端",
                "detail": f"传输 {transport_id}，摘要未变（{short_digest(envelope['digest'])}），交付身份不变",
            })
            message = (
                f"传输 {transport_id} 已交给本地模拟接收端；发送端返回 200，交付结论以回执对账为准"
            )
        return envelope, message

    def _deliver(
        self, envelope: dict[str, Any], attempt_no: int, behavior: str
    ) -> tuple[list[dict[str, Any]], str | None]:
        received_at = _now()
        base = {
            "attempt_no": attempt_no,
            "recorded_at": received_at,
            "source": "接收端回执",
            "channel_identity": envelope["channel_identity"],
        }
        if behavior == "lossy":
            return ([{
                **base,
                "stage": "送达证明",
                "verdict": "遗失",
                "note": "本地模拟接收端确认未收到载荷，传输链路遗失",
                "evidence": {"lost": True, "transport_id": envelope["attempts"][-1]["transport_id"]},
            }], None)

        if behavior == "rejects":
            wrong_digest = "d41d8cd98f00b204e9800998ecf8427e"
            return ([
                {
                    **base,
                    "stage": "送达证明",
                    "verdict": "已送达",
                    "note": "载荷到达接收端，但传输层改写了内容",
                    "evidence": {"received_digest": wrong_digest, "landed_content": True},
                },
                {
                    **base,
                    "stage": "完整性验证",
                    "verdict": "拒收",
                    "note": "重算摘要与封存摘要不一致，在完整性验证阶段明确拒收；被拒内容不计入数据份数",
                    "evidence": {
                        "expected_digest": envelope["digest"],
                        "received_digest": wrong_digest,
                        "reason": "摘要不一致",
                    },
                },
            ], None)

        intake = self.receiver.intake(
            channel_identity=envelope["channel_identity"],
            digest=envelope["digest"],
            row_count=envelope["row_count"],
            byte_size=envelope["byte_size"],
            attempt_no=attempt_no,
            received_at=received_at,
            corrupted=False,
        )
        landing_ref = intake["landing_ref"]
        duplicate = intake.get("duplicate_intakes", 0) > 0 and len(intake["attempts"]) > 1
        landed_note = (
            f"接收端已落下内容，沿用既有落盘编号 {landing_ref}，不产生新的数据份数"
            if duplicate
            else f"接收端已落下内容，落盘编号 {landing_ref}"
        )
        receipts: list[dict[str, Any]] = [
            {
                **base,
                "stage": "送达证明",
                "verdict": "已送达",
                "note": landed_note,
                "evidence": {"received_digest": envelope["digest"], "landed_content": True, "landing_ref": landing_ref},
            },
            {
                **base,
                "stage": "完整性验证",
                "verdict": "摘要一致",
                "note": "接收端重算摘要与封存摘要一致",
                "evidence": {"received_digest": envelope["digest"], "landing_ref": landing_ref},
            },
        ]
        if behavior != "landed_unconfirmed":
            receipts.append({
                **base,
                "stage": "接收确认",
                "verdict": "已接收",
                "note": (
                    f"重复投递命中既有落盘编号 {landing_ref}，确认仍归属同一交付身份"
                    if duplicate
                    else f"接收端确认已入库，落盘编号 {landing_ref}"
                ),
                "evidence": {"landed_content": True, "landing_ref": landing_ref, "duplicate_intake": duplicate},
            })
        return receipts, landing_ref

    # ---------- 补证 ----------

    def request_supplement(self, envelope: dict[str, Any], operator: str) -> tuple[dict[str, Any] | None, str]:
        reconciled = self.reconcile(envelope)
        if reconciled["status"] == STATUS_REJECTED:
            return None, (
                f"对方已在「{reconciled['rejection_stage']}」阶段拒收，补证不能推翻最终拒收"
            )
        if reconciled["status"] == STATUS_ACCEPTED:
            return None, "本通道已收到有效接收确认，无需补证"
        missing_stages = {item["stage"] for item in reconciled["missing_proofs"]}
        if "接收确认" not in missing_stages:
            return None, "当前缺少的不是接收确认，补证请求不适用"
        intake = self.receiver.lookup(envelope["channel_identity"], envelope["digest"])
        if intake is None:
            return None, "接收端没有这份摘要的落盘记录，补证无法成立；请原样重送"
        attempt_no = envelope["attempts"][-1]["attempt_no"] if envelope["attempts"] else None
        with self._lock:
            self._append_receipt(envelope, {
                "stage": "接收确认",
                "verdict": "已接收",
                "attempt_no": attempt_no,
                "recorded_at": _now(),
                "source": "接收端补证",
                "channel_identity": envelope["channel_identity"],
                "note": f"按原信封要求接收端补发确认，落盘编号 {intake['landing_ref']}，摘要未变",
                "evidence": {"landed_content": True, "landing_ref": intake["landing_ref"], "supplement": True},
            })
            envelope["actions"].append({
                "at": _now(),
                "by": operator,
                "action": "要求补证",
                "detail": f"凭原信封 {envelope['envelope_no']} 要求接收端补发接收确认",
            })
        return envelope, f"接收端已补发接收确认，落盘编号 {intake['landing_ref']}"

    # ---------- 人工回执 ----------

    def record_receipt(
        self,
        envelope: dict[str, Any],
        *,
        stage: str,
        verdict: str,
        note: str,
        source: str,
        operator: str,
        attempt_no: int | None,
        channel_identity: str | None,
    ) -> tuple[dict[str, Any] | None, str]:
        if stage not in STAGE_VERDICTS:
            return None, f"证明阶段「{stage}」不在 {'、'.join(STAGE_ORDER)} 之列"
        if verdict not in STAGE_VERDICTS[stage]:
            return None, f"阶段「{stage}」不接受结论「{verdict}」，可选：{'、'.join(STAGE_VERDICTS[stage])}"
        with self._lock:
            self._append_receipt(envelope, {
                "stage": stage,
                "verdict": verdict,
                "attempt_no": attempt_no,
                "recorded_at": _now(),
                "source": source or "人工补录",
                "channel_identity": channel_identity or envelope["channel_identity"],
                "note": note,
                "evidence": {},
                "origin": "人工补录",
            })
            envelope["actions"].append({
                "at": _now(),
                "by": operator,
                "action": "补录回执",
                "detail": f"{stage} / {verdict}：{note or '无说明'}",
            })
        return envelope, f"已补录 {stage} 回执：{verdict}"

    # ---------- 对账 ----------

    def reconcile(self, envelope: dict[str, Any]) -> dict[str, Any]:
        channel_identity = envelope["channel_identity"]
        attempts_by_no = {item["attempt_no"]: item for item in envelope.get("attempts", [])}
        receipts = sorted(
            envelope.get("receipts", []),
            key=lambda item: (item.get("recorded_at") or "", item["id"]),
        )

        prepared: list[dict[str, Any]] = []
        for receipt in receipts:
            flags: list[dict[str, str]] = []
            valid = True
            if receipt.get("channel_identity") and receipt["channel_identity"] != channel_identity:
                valid = False
                flags.append({
                    "code": "channel_mismatch",
                    "label": "跨通道回执",
                    "detail": (
                        f"该回执属于通道 {receipt['channel_identity']}，"
                        f"不能抵扣通道 {channel_identity} 的证明"
                    ),
                })
            attempt_no = receipt.get("attempt_no")
            if attempt_no not in (None, "") and int(attempt_no) not in attempts_by_no:
                valid = False
                flags.append({
                    "code": "unknown_attempt",
                    "label": "指向不存在的传输",
                    "detail": f"回执引用的传输批次 {attempt_no} 不存在",
                })
            prepared.append({**receipt, "valid": valid, "flags": flags, "role": "evidence" if valid else "ignored"})

        rejections = [
            item for item in prepared
            if item["valid"] and item["verdict"] in REJECT_VERDICTS
        ]
        rejection = min(rejections, key=lambda item: (item["recorded_at"], item["id"])) if rejections else None
        rejection_stage = rejection["stage"] if rejection else None

        for item in prepared:
            if not item["valid"] or rejection is None:
                continue
            if item["id"] == rejection["id"]:
                item["role"] = "effective"
                item["flags"].append({
                    "code": "rejection",
                    "label": "最终拒收",
                    "detail": f"在「{item['stage']}」阶段拒收，结论为最终结论",
                })
                continue
            if (
                (item["recorded_at"], item["id"]) > (rejection["recorded_at"], rejection["id"])
                and STAGE_RANK[item["stage"]] < STAGE_RANK[rejection_stage]
            ):
                item["valid"] = False
                item["role"] = "ignored"
                item["flags"].append({
                    "code": "late_low_stage",
                    "label": "迟到低阶段证明",
                    "detail": (
                        f"在「{rejection_stage}」拒收之后才到达的「{item['stage']}」证明，"
                        "阶段低于拒收阶段，不参与对账"
                    ),
                })

        if rejection is not None:
            effective = {
                "status": STATUS_REJECTED,
                "stage": rejection["stage"],
                "verdict": rejection["verdict"],
                "receipt_id": rejection["id"],
                "recorded_at": rejection["recorded_at"],
                "source": rejection["source"],
                "landing_ref": (
                    rejection.get("evidence", {}).get("landing_ref")
                    or envelope.get("landing_ref")
                ),
                "reason": (
                    f"接收端在「{rejection['stage']}」阶段明确拒收；"
                    "拒收具有明确阶段，之后迟到的低阶段证明不能推翻该结论"
                ),
            }
            stage_reached = STAGE_RANK[rejection_stage]
            missing: list[dict[str, Any]] = []
            status = STATUS_REJECTED
            status_reason = effective["reason"]
            copy_count = 0
        else:
            delivery_ok = [
                item for item in prepared
                if item["valid"] and item["stage"] == "送达证明" and item["verdict"] == "已送达"
            ]
            delivery_lost = [
                item for item in prepared
                if item["valid"] and item["stage"] == "送达证明" and item["verdict"] == "遗失"
            ]
            integrity_ok = [
                item for item in prepared
                if item["valid"] and item["stage"] == "完整性验证" and item["verdict"] == "摘要一致"
            ]
            confirmations = [
                item for item in prepared
                if item["valid"] and item["stage"] == "接收确认" and item["verdict"] == "已接收"
            ]
            cross_channel_confirm = [
                item for item in prepared
                if item["stage"] == "接收确认"
                and item["verdict"] == "已接收"
                and any(flag["code"] == "channel_mismatch" for flag in item["flags"])
            ]

            valid_items = [item for item in prepared if item["valid"]]
            stage_reached = max((STAGE_RANK[item["stage"]] for item in valid_items), default=0)
            landed = bool(envelope.get("receiver_landed"))

            missing = []
            if not delivery_ok:
                missing.append({
                    "stage": "送达证明",
                    "rank": STAGE_RANK["送达证明"],
                    "reason": (
                        "接收端确认遗失，需原样重送或由对方补证"
                        if delivery_lost
                        else "尚无本通道有效送达证明"
                    ),
                })
            if not integrity_ok:
                missing.append({
                    "stage": "完整性验证",
                    "rank": STAGE_RANK["完整性验证"],
                    "reason": "接收端已落盘，尚缺摘要核验回执" if landed else "尚无完整性验证回执",
                })
            if not confirmations:
                if cross_channel_confirm:
                    identities = "、".join(sorted({item["channel_identity"] for item in cross_channel_confirm}))
                    reason = f"仅收到通道 {identities} 的接收确认，通道之间的确认不能互相抵扣"
                elif landed:
                    reason = "接收端已落下内容，但接收确认回执未送达，不能认定交付完成"
                else:
                    reason = "尚无本通道接收确认"
                missing.append({
                    "stage": "接收确认",
                    "rank": STAGE_RANK["接收确认"],
                    "reason": reason,
                })

            if delivery_ok and integrity_ok and confirmations:
                confirmations.sort(key=lambda item: (item["recorded_at"], item["id"]))
                effective_receipt = confirmations[0]
                effective_receipt["role"] = "effective"
                for duplicate in confirmations[1:]:
                    duplicate["role"] = "duplicate"
                    duplicate["flags"].append({
                        "code": "duplicate_confirmation",
                        "label": "重复确认",
                        "detail": "同一交付身份下的重复接收确认，只保留唯一有效结果，不增加数据份数",
                    })
                status = STATUS_ACCEPTED
                effective = {
                    "status": status,
                    "stage": "接收确认",
                    "verdict": "已接收",
                    "receipt_id": effective_receipt["id"],
                    "recorded_at": effective_receipt["recorded_at"],
                    "source": effective_receipt["source"],
                    "landing_ref": (
                        effective_receipt.get("evidence", {}).get("landing_ref")
                        or envelope.get("landing_ref")
                    ),
                    "reason": (
                        f"送达证明与完整性验证均成立，接收确认以回执 #{effective_receipt['id']} 为准；"
                        "其余同身份确认视为重复，不增加数据份数"
                    ),
                }
                status_reason = effective["reason"]
                copy_count = 1
            else:
                copy_count = 1 if landed else 0
                if not envelope.get("attempts") and not prepared:
                    status = STATUS_PENDING_SEND
                    status_reason = "信封已封存，尚未向接收端投递"
                elif delivery_lost and not delivery_ok:
                    status = STATUS_LOST
                    status_reason = "接收端确认遗失，本次交付没有有效送达；需要原样重送或补证"
                elif landed and integrity_ok and not confirmations:
                    status = STATUS_LANDED_UNCONFIRMED
                    status_reason = (
                        "内容已落盘且摘要一致，但本通道接收确认未送达；"
                        "发送端 200 不能替代接收确认，交付尚未完成"
                    )
                else:
                    status = STATUS_PENDING_PROOF
                    status_reason = "已发起投递，但回执尚不完整，需按缺失证明补证或重送"
                effective = self._latest_positive(envelope, prepared, missing)

        # 被接纳/重复确认之外的辅助回执：标记是否参与对账
        for item in prepared:
            if item["role"] == "evidence":
                item["role"] = "supporting"

        ignored = [item for item in prepared if not item["valid"]]
        duplicates = [item for item in prepared if item["role"] == "duplicate"]
        confirmation_count = len([
            item for item in prepared
            if item["valid"] and item["stage"] == "接收确认" and item["verdict"] == "已接收"
        ])
        transmission_count = len(envelope.get("attempts", []))

        return {
            "status": status,
            "status_reason": status_reason,
            "stage_reached": {
                "stage": STAGE_ORDER[stage_reached - 1] if stage_reached else None,
                "rank": stage_reached,
            },
            "missing_proofs": missing,
            "effective_result": effective,
            "copy_count": copy_count,
            "transmission_count": transmission_count,
            "confirmation_count": confirmation_count,
            "ignored_receipts": ignored,
            "duplicate_receipts": duplicates,
            "rejection_stage": rejection_stage,
            "rejection_receipt_id": rejection["id"] if rejection else None,
            "sender_ack": {
                "values": [item.get("sender_status") for item in envelope.get("attempts", [])],
                "note": "发送端 200 只说明传输层接收了载荷，不能作为交付完成依据",
            },
            "receipts": prepared,
        }

    def _latest_positive(
        self,
        envelope: dict[str, Any],
        prepared: list[dict[str, Any]],
        missing: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        positives = [
            item for item in prepared
            if item["valid"] and item["verdict"] in {"已送达", "摘要一致", "已接收"}
        ]
        if not positives:
            if missing:
                return {
                    "status": "待补证",
                    "stage": missing[0]["stage"],
                    "receipt_id": None,
                    "recorded_at": None,
                    "source": None,
                    "landing_ref": None,
                    "reason": missing[0]["reason"],
                }
            return None
        latest = max(positives, key=lambda item: (STAGE_RANK[item["stage"]], item["recorded_at"], item["id"]))
        landed_ref = latest.get("evidence", {}).get("landing_ref")
        reasons = "；".join(item["reason"] for item in missing)
        return {
            "status": "进行中",
            "stage": latest["stage"],
            "receipt_id": latest["id"],
            "recorded_at": latest["recorded_at"],
            "source": latest["source"],
            "landing_ref": landed_ref or envelope.get("landing_ref"),
            "reason": f"已取得「{latest['stage']}」有效回执；但{reasons or '回执仍不完整'}",
        }

    # ---------- 明细 ----------

    def detail(self, envelope: dict[str, Any]) -> dict[str, Any]:
        reconciled = self.reconcile(envelope)
        batch = self._batch(envelope["batch_id"])
        current_content = batch["content"] if batch else []
        snapshot_rows = envelope["sealed_content"]
        drift_rows = max(len(current_content) - len(snapshot_rows), 0)
        prefix_unchanged = current_content[: len(snapshot_rows)] == snapshot_rows
        recomputed, _, _ = digest_of(snapshot_rows, envelope["encoding"])
        intakes = sorted(
            envelope.get("receipts", []),
            key=lambda item: (STAGE_RANK[item["stage"]], item["recorded_at"], item["id"]),
        )
        timeline = sorted(
            envelope.get("receipts", []),
            key=lambda item: (item["recorded_at"], item["id"]),
        )
        return {
            **{
                key: envelope[key]
                for key in (
                    "id", "envelope_no", "delivery_identity", "batch_id", "batch_no", "dataset",
                    "channel_id", "channel_no", "channel_name", "channel_identity", "target_system",
                    "channel_protocol", "encoding", "digest", "short_digest", "byte_size", "row_count",
                    "sealed_at", "sealed_by", "simulation", "receiver_landed", "landing_ref",
                    "attempts", "actions", "seal_note",
                )
            },
            "snapshot": {
                "rows": snapshot_rows,
                "row_count": len(snapshot_rows),
                "byte_size": envelope["byte_size"],
                "digest": envelope["digest"],
                "digest_recomputed": recomputed,
                "digest_matches": recomputed == envelope["digest"],
                "batch_current_rows": len(current_content),
                "batch_drift_rows": drift_rows,
                "batch_prefix_unchanged": prefix_unchanged,
                "note": (
                    "封存后落盘批次新增了内容，但未混入本快照" if drift_rows
                    else "封存快照与落盘内容一致"
                ),
            },
            "receipts": intakes,
            "receipt_timeline": timeline,
            "reconciliation": reconciled,
        }

    def envelope_by_id(self, envelope_id: int) -> dict[str, Any] | None:
        return self._envelopes.get(envelope_id)

    def list_envelopes(
        self,
        *,
        channel: str | None = None,
        status: str | None = None,
        missing: str | None = None,
        sort: str = "stage_desc",
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        items: list[dict[str, Any]] = []
        for envelope in self._envelopes.values():
            reconciled = self.reconcile(envelope)
            record = {
                "id": envelope["id"],
                "envelope_no": envelope["envelope_no"],
                "delivery_identity": envelope["delivery_identity"],
                "dataset": envelope["dataset"],
                "batch_no": envelope["batch_no"],
                "channel_no": envelope["channel_no"],
                "channel_name": envelope["channel_name"],
                "channel_identity": envelope["channel_identity"],
                "target_system": envelope["target_system"],
                "encoding": envelope["encoding"],
                "digest": envelope["digest"],
                "short_digest": envelope["short_digest"],
                "sealed_at": envelope["sealed_at"],
                "status": reconciled["status"],
                "status_reason": reconciled["status_reason"],
                "stage_reached": reconciled["stage_reached"],
                "missing_stages": [item["stage"] for item in reconciled["missing_proofs"]],
                "missing_proofs": reconciled["missing_proofs"],
                "rejection_stage": reconciled["rejection_stage"],
                "transmission_count": reconciled["transmission_count"],
                "copy_count": reconciled["copy_count"],
                "receiver_landed": envelope.get("receiver_landed", False),
                "confirmations": reconciled["confirmation_count"],
            }
            if channel and channel not in (record["channel_identity"], record["channel_no"]):
                continue
            if status and record["status"] != status:
                continue
            if missing and missing not in record["missing_stages"]:
                continue
            items.append(record)

        if sort == "stage_asc":
            items.sort(key=lambda item: (item["stage_reached"]["rank"], item["sealed_at"], item["id"]))
        elif sort == "stage_desc":
            items.sort(key=lambda item: (-item["stage_reached"]["rank"], item["sealed_at"], item["id"]))
        elif sort == "time_asc":
            items.sort(key=lambda item: (item["sealed_at"], item["id"]))
        else:
            items.sort(key=lambda item: (item["sealed_at"], item["id"]), reverse=True)

        total = len(items)
        start = max(page - 1, 0) * size
        return items[start: start + size], total

    def stats(self) -> dict[str, Any]:
        counts = {status: 0 for status in STATUSES}
        pending_missing = 0
        transmissions = 0
        copies = 0
        for envelope in self._envelopes.values():
            reconciled = self.reconcile(envelope)
            counts[reconciled["status"]] = counts.get(reconciled["status"], 0) + 1
            transmissions += reconciled["transmission_count"]
            copies += reconciled["copy_count"]
            if reconciled["missing_proofs"] and reconciled["status"] != STATUS_REJECTED:
                pending_missing += 1
        return {
            "total": len(self._envelopes),
            "status_counts": counts,
            "pending_missing": pending_missing,
            "transmissions": transmissions,
            "copies": copies,
            "channels": len(self.list_channels()),
            "batches": len(self.list_batches()),
        }

    def append_batch_rows(self, batch_id: int, rows: int = 1) -> tuple[dict[str, Any] | None, str]:
        """演练：往已确认落盘批次追加内容，已封存信封的快照保持不变。"""
        with self._lock:
            batch = self._batch(batch_id)
            if batch is None:
                return None, f"落盘批次 {batch_id} 不存在"
            rows = max(1, min(rows, 10))
            base = len(batch["content"])
            for offset in range(rows):
                batch["content"].append({
                    "url": f"https://late.example.local/a/{9000 + base + offset}",
                    "title": f"封存后追加内容 {base + offset + 1}",
                    "region": "追加",
                    "price": 0.0,
                    "captured_at": _now(),
                    "source_site": "late.example.local",
                })
            return {
                "batch_id": batch["id"],
                "batch_no": batch["batch_no"],
                "row_count": len(batch["content"]),
                "digest": digest_of(batch["content"], batch["encoding"])[0],
            }, (
                f"落盘批次 {batch['batch_no']} 已追加 {rows} 行；"
                "已封存信封仍按原摘要对账，追加内容不会混入快照"
            )


service = DeliveryService()
