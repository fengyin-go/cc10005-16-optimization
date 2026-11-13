"""交付信封对账规则测试：验证封存快照、回执阶段、拒收终局与交付身份口径。

只依赖标准库，用 `python -m unittest discover -s tests` 运行。
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.export import (  # noqa: E402
    STATUS_ACCEPTED,
    STATUS_LANDED_UNCONFIRMED,
    STATUS_LOST,
    STATUS_REJECTED,
    DeliveryService,
)


class DeliveryReconciliationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.service = DeliveryService()

    def _seal(self, batch_id: int = 1, channel_id: int = 1, encoding: str = "UTF-8",
              simulation: str = "正常签收") -> dict:
        envelope, _, _ = self.service.seal(
            batch_id=batch_id,
            channel_id=channel_id,
            encoding=encoding,
            simulation=simulation,
            sealed_by="值班管理员",
        )
        assert envelope is not None
        return envelope

    def test_seal_freezes_snapshot_against_later_landing_rows(self) -> None:
        envelope = self._seal()
        before = list(envelope["sealed_content"])
        self.service.append_batch_rows(1, 3)
        detail = self.service.detail(envelope)
        self.assertEqual(detail["snapshot"]["rows"], before)
        self.assertEqual(detail["snapshot"]["batch_drift_rows"], 3)
        self.assertTrue(detail["snapshot"]["digest_matches"])

    def test_seal_rejects_unsupported_encoding(self) -> None:
        envelope, message, _ = self.service.seal(
            batch_id=1, channel_id=1, encoding="ROT13", simulation="正常签收", sealed_by="值班管理员"
        )
        self.assertIsNone(envelope)
        self.assertIn("ROT13", message)

    def test_seal_rejects_missing_batch(self) -> None:
        envelope, message, _ = self.service.seal(
            batch_id=99, channel_id=1, encoding="UTF-8", simulation="正常签收", sealed_by="值班管理员"
        )
        self.assertIsNone(envelope)
        self.assertIn("不存在", message)

    def test_same_digest_reuses_delivery_identity(self) -> None:
        first = self._seal()
        second, message, created = self.service.seal(
            batch_id=1, channel_id=1, encoding="UTF-8", simulation="正常签收", sealed_by="值班管理员"
        )
        self.assertFalse(created)
        self.assertEqual(second["delivery_identity"], first["delivery_identity"])
        self.assertIn("沿用同一交付身份", message)

    def test_same_content_different_encoding_is_distinct_copy(self) -> None:
        first = self._seal(encoding="UTF-8")
        second = self._seal(encoding="UTF-16LE")
        self.assertNotEqual(first["digest"], second["digest"])

    def test_lost_delivery_is_not_delivered_despite_sender_200(self) -> None:
        envelope = self._seal(batch_id=1, channel_id=2, simulation="传输遗失")
        self.service.send(envelope, "值班管理员")
        reconciled = self.service.reconcile(envelope)
        self.assertEqual(reconciled["status"], STATUS_LOST)
        self.assertEqual(reconciled["sender_ack"]["values"], [200])
        self.assertIn("送达证明", [item["stage"] for item in reconciled["missing_proofs"]])

    def test_resend_after_loss_keeps_identity_and_counts_once(self) -> None:
        envelope = self._seal(batch_id=1, channel_id=2, simulation="传输遗失")
        self.service.send(envelope, "值班管理员")
        self.service.send(envelope, "值班管理员")
        reconciled = self.service.reconcile(envelope)
        self.assertEqual(reconciled["status"], STATUS_ACCEPTED)
        self.assertEqual(reconciled["transmission_count"], 2)
        self.assertEqual(reconciled["copy_count"], 1)
        self.assertIn("同身份", reconciled["effective_result"]["reason"])
        self.assertEqual(len({attempt["transport_id"] for attempt in envelope["attempts"]}), 2)

    def test_cross_channel_confirmation_does_not_offset(self) -> None:
        envelope = self._seal(batch_id=1, channel_id=2, simulation="传输遗失")
        self.service.send(envelope, "值班管理员")
        self.service.record_receipt(
            envelope, stage="接收确认", verdict="已接收", note="其他通道确认",
            source="人工补录", operator="值班管理员", attempt_no=1, channel_identity="CH-EAST-7F3C",
        )
        reconciled = self.service.reconcile(envelope)
        self.assertEqual(len(reconciled["ignored_receipts"]), 1)
        self.assertEqual(reconciled["status"], STATUS_LOST)
        acceptance = next(item for item in reconciled["missing_proofs"] if item["stage"] == "接收确认")
        self.assertIn("不能互相抵扣", acceptance["reason"])

    def test_late_low_stage_proof_cannot_overturn_rejection(self) -> None:
        envelope = self._seal(batch_id=2, channel_id=3, simulation="完整性拒收")
        self.service.send(envelope, "值班管理员")
        self.service.record_receipt(
            envelope, stage="送达证明", verdict="已送达", note="迟到的证明",
            source="人工补录", operator="值班管理员", attempt_no=1, channel_identity=None,
        )
        reconciled = self.service.reconcile(envelope)
        self.assertEqual(reconciled["status"], STATUS_REJECTED)
        self.assertEqual(reconciled["rejection_stage"], "完整性验证")
        late = [item for item in reconciled["receipts"] if item["role"] == "ignored"]
        self.assertIn("late_low_stage", [flag["code"] for flag in late[0]["flags"]])
        self.assertEqual(reconciled["copy_count"], 0)

    def test_rejected_envelope_cannot_be_resent_or_supplemented(self) -> None:
        envelope = self._seal(batch_id=2, channel_id=3, simulation="完整性拒收")
        self.service.send(envelope, "值班管理员")
        updated, message = self.service.send(envelope, "值班管理员")
        self.assertIsNone(updated)
        self.assertIn("拒收", message)
        updated, message = self.service.request_supplement(envelope, "值班管理员")
        self.assertIsNone(updated)
        self.assertIn("完整性验证", message)

    def test_landed_without_confirmation_stays_incomplete_until_supplement(self) -> None:
        envelope = self._seal(batch_id=5, channel_id=1, simulation="已落盘未确认")
        self.service.send(envelope, "值班管理员")
        reconciled = self.service.reconcile(envelope)
        self.assertEqual(reconciled["status"], STATUS_LANDED_UNCONFIRMED)
        self.assertEqual(reconciled["copy_count"], 1)
        self.assertEqual([item["stage"] for item in reconciled["missing_proofs"]], ["接收确认"])

        updated, message = self.service.request_supplement(envelope, "值班管理员")
        self.assertIsNotNone(updated)
        reconciled = self.service.reconcile(envelope)
        self.assertEqual(reconciled["status"], STATUS_ACCEPTED)
        self.assertEqual(reconciled["copy_count"], 1)
        self.assertEqual(len(reconciled["duplicate_receipts"]), 0)

    def test_duplicate_confirmations_keep_single_effective_result(self) -> None:
        envelope = self.service.envelope_by_id(5)
        reconciled = self.service.reconcile(envelope)
        self.assertEqual(reconciled["status"], STATUS_ACCEPTED)
        self.assertEqual(reconciled["transmission_count"], 2)
        self.assertEqual(reconciled["copy_count"], 1)
        self.assertGreaterEqual(reconciled["confirmation_count"], 2)
        self.assertTrue(reconciled["duplicate_receipts"])
        effective_id = reconciled["effective_result"]["receipt_id"]
        self.assertTrue(any(item["id"] == effective_id and item["role"] == "effective"
                            for item in reconciled["receipts"]))

    def test_seeded_scenarios_have_expected_statuses(self) -> None:
        expected = {
            1: STATUS_ACCEPTED, 2: STATUS_LOST, 3: STATUS_REJECTED,
            4: STATUS_LANDED_UNCONFIRMED, 5: STATUS_ACCEPTED,
        }
        for envelope_id, status in expected.items():
            envelope = self.service.envelope_by_id(envelope_id)
            self.assertEqual(self.service.reconcile(envelope)["status"], status, envelope_id)


if __name__ == "__main__":
    unittest.main()
