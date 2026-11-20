from contextlib import closing
from dataclasses import replace
import sqlite3
from tempfile import TemporaryDirectory
from pathlib import Path
import unittest

from tests import initialize_source
from examples.transfer_case import complete_shift, prepare_shift, transfer_report
from warehouse.catalog.repository import CatalogRepository
from warehouse.close.freeze import freeze_partition
from warehouse.facts.amendments import amend_row
from warehouse.facts.batches import post_batch
from warehouse.facts.repository import FactRepository
from warehouse.facts.revisions import read_current_revision, read_fact_revision
from warehouse.facts.rows import RowIdentity
from warehouse.facts.transfers import transfer_row
from warehouse.mirror.admission import install_release
from warehouse.mirror.read import audit_partition, audit_release, read_transfer_status
from warehouse.mirror.repository import MirrorRepository
from warehouse.release.format import decode_v1, decode_v2, encode_v2
from warehouse.release.issue import issue_release
from warehouse.release.manifest import build_transfer_manifest
from warehouse.release.transfers import evidence_to_data


def source_rows(connection, partition):
    return read_current_revision(CatalogRepository(connection), FactRepository(connection), partition).rows


def publish(connection, partition, revision, release_id):
    snapshot = freeze_partition(connection, f"fixed-{release_id}", partition, revision)
    return issue_release(connection, release_id, snapshot.snapshot_id)[1]


class TransferSourceTests(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(":memory:")
        self.addCleanup(self.connection.close)
        initialize_source(self.connection)
        post_batch(self.connection, "A", "meter-A", "first", [("cooling", 7), ("storm", 2)])
        self.catalog = CatalogRepository(self.connection)
        self.facts = FactRepository(self.connection)
        self.first, self.second = read_current_revision(self.catalog, self.facts, "A").rows

    def transfer_first(self, transfer_id="T1", row=None):
        return transfer_row(self.connection, transfer_id, "A", "B", self.first.identity, row or self.first)

    def test_transfer_moves_row_and_advances_both_partitions(self):
        receipt = self.transfer_first()
        self.assertEqual((receipt.from_revision, receipt.to_revision), (2, 1))
        self.assertEqual(receipt.row, self.first)
        self.assertEqual(receipt.identity, self.first.identity)
        alpha = read_current_revision(self.catalog, self.facts, "A")
        beta = read_current_revision(self.catalog, self.facts, "B")
        self.assertEqual([row.identity for row in alpha.rows], [self.second.identity])
        self.assertEqual([(row.identity, row.liters) for row in beta.rows], [(self.first.identity, 7)])
        # History before the transfer still shows the original arrangement.
        self.assertEqual([row.identity for row in read_fact_revision(self.catalog, self.facts, "A", 1).rows],
                         [self.first.identity, self.second.identity])

    def test_identity_and_provenance_are_preserved(self):
        receipt = self.transfer_first()
        moved = read_current_revision(self.catalog, self.facts, "B").rows[0]
        self.assertEqual(moved.provenance, self.first.provenance)
        self.assertEqual(moved.provenance.partition, "A")
        self.assertEqual(moved.provenance.accepted_revision, 1)
        self.assertEqual(moved.identity, RowIdentity("A", "meter-A", "first", 1))

    def test_replay_same_arguments_returns_first_receipt(self):
        receipt = self.transfer_first()
        corrected = amend_row(self.connection, "B", receipt.identity, receipt.row, "cooling", 9)
        self.assertEqual(corrected.revision, 2)
        self.assertEqual(self.transfer_first(), receipt)
        self.assertEqual(read_current_revision(self.catalog, self.facts, "B").rows[0].liters, 9)

    def test_replay_with_different_arguments_is_rejected(self):
        receipt = self.transfer_first()
        before = (read_current_revision(self.catalog, self.facts, "A"),
                  read_current_revision(self.catalog, self.facts, "B"))
        with self.assertRaisesRegex(ValueError, "^transfer_conflict$"):
            transfer_row(self.connection, "T1", "A", "B", self.first.identity,
                         replace(self.first, liters=5))
        with self.assertRaisesRegex(ValueError, "^transfer_conflict$"):
            transfer_row(self.connection, "T1", "A", "B", self.first.identity,
                         replace(self.first, category="storm"))
        self.assertEqual((read_current_revision(self.catalog, self.facts, "A"),
                          read_current_revision(self.catalog, self.facts, "B")), before)
        self.assertEqual(self.facts.find_transfer("T1").from_revision, receipt.from_revision)

    def test_stale_old_image_is_rejected(self):
        before = read_current_revision(self.catalog, self.facts, "A")
        with self.assertRaisesRegex(ValueError, "^stale_old_image$"):
            transfer_row(self.connection, "T1", "A", "B", self.first.identity,
                         replace(self.first, liters=6))
        with self.assertRaisesRegex(ValueError, "^stale_old_image$"):
            transfer_row(self.connection, "T1", "A", "B", self.second.identity, self.first)
        self.assertEqual(read_current_revision(self.catalog, self.facts, "A"), before)
        self.assertIsNone(self.facts.find_transfer("T1"))

    def test_second_identifier_for_same_row_is_rejected(self):
        self.transfer_first("T1")
        with self.assertRaisesRegex(ValueError, "^row_already_transferred$"):
            transfer_row(self.connection, "T2", "A", "B", self.first.identity, self.first)
        self.assertIsNone(self.facts.find_transfer("T2"))
        self.assertEqual(read_current_revision(self.catalog, self.facts, "B").revision, 1)

    def test_business_input_validation_leaves_no_transfer(self):
        missing = RowIdentity("A", "meter-A", "absent", 1)
        cases = (
            (("T1", "A", "A", self.first.identity, self.first), "^transfer_partition_conflict$"),
            (("T1", "A", "C", self.first.identity, self.first), "^unknown_partition$"),
            (("T1", "A", "B", missing, self.first), "^unknown_row$"),
            (("T1", "B", "A", self.first.identity, self.first), "^transfer_identity_mismatch$"),
            ((" ", "A", "B", self.first.identity, self.first), "^invalid_transfer_id$"),
        )
        for arguments, error in cases:
            with self.subTest(error=error):
                with self.assertRaisesRegex(ValueError, error):
                    transfer_row(self.connection, *arguments)
                self.assertEqual(self.facts.head_revision("A"), 1)
                self.assertEqual(self.facts.head_revision("B"), 0)
                self.assertIsNone(self.facts.find_transfer(arguments[0]))

    def test_unknown_target_category_is_rejected(self):
        # ``storm`` is registered in A but not in B; the old image is accurate,
        # so the failure is about the target classification.
        with self.assertRaisesRegex(ValueError, "^unknown_partition_category$"):
            transfer_row(self.connection, "T1", "A", "B", self.second.identity, self.second)
        self.assertIsNone(self.facts.find_transfer("T1"))

    def test_amend_after_transfer_only_in_new_partition(self):
        receipt = self.transfer_first()
        moved = read_current_revision(self.catalog, self.facts, "B").rows[0]
        with self.assertRaisesRegex(ValueError, "^unknown_row$"):
            amend_row(self.connection, "A", moved.identity, moved, "cooling", 9)
        self.assertEqual(amend_row(self.connection, "B", moved.identity, moved, "cooling", 9).revision, 2)
        self.assertEqual(read_current_revision(self.catalog, self.facts, "B").revision, 2)

    def test_empty_partition_accepts_transfer(self):
        post_batch(self.connection, "B", "meter-B", "early", [("effluent", 4)])
        third = publish(self.connection, "B", 1, "pre")
        receipt = transfer_row(self.connection, "T1", "A", "B", self.first.identity, self.first)
        self.assertEqual(receipt.to_revision, 2)
        self.assertEqual([(row.liters, row.provenance.partition) for row in
                          read_current_revision(self.catalog, self.facts, "B").rows],
                         [(4, "B"), (7, "A")])
        self.assertEqual(third, publish(self.connection, "B", 1, "pre"))


class TransferReleaseTests(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(":memory:")
        self.addCleanup(self.connection.close)
        initialize_source(self.connection)
        post_batch(self.connection, "A", "meter-A", "first", [("cooling", 7), ("storm", 2)])
        self.catalog = CatalogRepository(self.connection)
        self.facts = FactRepository(self.connection)
        self.first, self.second = read_current_revision(self.catalog, self.facts, "A").rows
        self.a1 = publish(self.connection, "A", 1, "A1")

    def do_transfer(self):
        return transfer_row(self.connection, "T1", "A", "B", self.first.identity, self.first)

    def test_untouched_history_still_issues_v1(self):
        manifest, records = decode_v1(publish(self.connection, "A", 1, "A1-again"))
        self.assertEqual((manifest.partition, manifest.revision), ("A", 1))

    def test_move_out_release_binds_v2_with_original_row(self):
        self.do_transfer()
        artifact = publish(self.connection, "A", 2, "A2")
        manifest, records, transfers = decode_v2(artifact)
        self.assertEqual((manifest.partition, manifest.revision, len(transfers)), ("A", 2, 1))
        evidence = transfers[0]
        self.assertEqual((evidence.transfer_id, evidence.from_partition, evidence.to_partition),
                         ("T1", "A", "B"))
        self.assertEqual((evidence.row, evidence.current), (self.first, None))
        self.assertNotIn(self.first.identity, {row.identity for row in records.rows})

    def test_move_in_release_carries_current_value(self):
        self.do_transfer()
        amend_row(self.connection, "B", self.first.identity, self.first, "cooling", 9)
        post_batch(self.connection, "B", "meter-B", "Y", [("effluent", 4)])
        manifest, records, transfers = decode_v2(publish(self.connection, "B", 3, "B3"))
        evidence = transfers[0]
        self.assertEqual(evidence.row, self.first)
        self.assertEqual(evidence.current.liters, 9)
        self.assertEqual(evidence.current, next(row for row in records.rows
                                                if row.identity == self.first.identity))

    def test_v2_issue_is_byte_reproducible_across_databases(self):
        self.do_transfer()
        freeze_partition(self.connection, "fixed-A", "A", 2)
        first = issue_release(self.connection, "A2", "fixed-A")
        with TemporaryDirectory() as directory:
            path = Path(directory) / "source.sqlite3"
            with closing(sqlite3.connect(path)) as other:
                initialize_source(other)
                post_batch(other, "A", "meter-A", "first", [("cooling", 7), ("storm", 2)])
                catalog = CatalogRepository(other)
                facts = FactRepository(other)
                row = read_current_revision(catalog, facts, "A").rows[0]
                transfer_row(other, "T1", "A", "B", row.identity, row)
                freeze_partition(other, "fixed-A", "A", 2)
                self.assertEqual(issue_release(other, "A2", "fixed-A"), first)

    def test_same_release_reproduces_bytes_and_rejects_other_binding(self):
        self.do_transfer()
        freeze_partition(self.connection, "fixed-A2", "A", 2)
        first = issue_release(self.connection, "A2", "fixed-A2")
        self.assertEqual(issue_release(self.connection, "A2", "fixed-A2"), first)
        freeze_partition(self.connection, "fixed-A1", "A", 1)
        with self.assertRaisesRegex(ValueError, "^issue_binding_conflict$"):
            issue_release(self.connection, "A2", "fixed-A1")
        self.assertEqual(issue_release(self.connection, "A2", "fixed-A2"), first)

    def test_v2_digest_covers_transfer_evidence(self):
        self.do_transfer()
        manifest, records, transfers = decode_v2(publish(self.connection, "A", 2, "A2"))
        tampered = (replace(transfers[0], transfer_id="T-other"),)
        with self.assertRaisesRegex(ValueError, "^invalid_transfer_digest$"):
            encode_v2(manifest, records, tampered)
        # Keep the original manifest digest while swapping the evidence on the wire.
        from dataclasses import asdict
        from warehouse.release.records import encode_ledger_records
        from warehouse.release.transfers import canonical_json_bytes, evidence_to_data
        forged = canonical_json_bytes({
            "version": 2,
            "manifest": asdict(manifest),
            "ledger": encode_ledger_records(records),
            "transfers": [evidence_to_data(tampered[0])],
        })
        with self.assertRaisesRegex(ValueError, "^transfers_digest_mismatch$"):
            install_release(self.mirror(), forged)
        # A self-consistent digest does not make a contradictory binding legal:
        # the move-out side may not claim a current value.
        bad_binding = (replace(transfers[0], current=transfers[0].row),)
        bad_manifest = build_transfer_manifest("A2", manifest.snapshot_id, "A", 2, records, bad_binding)
        with self.assertRaisesRegex(ValueError, "^invalid_transfer_binding$"):
            install_release(self.mirror(), encode_v2(bad_manifest, records, bad_binding))

    def mirror(self):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        return MirrorRepository(connection)


class TransferMirrorTests(unittest.TestCase):
    """Two-order delivery, partial-side retention and conflicting evidence."""

    def setUp(self):
        self.connection = sqlite3.connect(":memory:")
        self.addCleanup(self.connection.close)
        initialize_source(self.connection)
        post_batch(self.connection, "A", "meter-A", "X", [("cooling", 7)])
        post_batch(self.connection, "A", "meter-A", "M", [("storm", 2)])
        self.a2 = publish(self.connection, "A", 2, "A2")
        self.b0 = publish(self.connection, "B", 0, "B0")
        self.stale_x, self.ordinary = read_current_revision(
            CatalogRepository(self.connection), FactRepository(self.connection), "A").rows
        amend_row(self.connection, "A", self.stale_x.identity, self.stale_x, "cooling", 8)
        self.expected_x = read_current_revision(
            CatalogRepository(self.connection), FactRepository(self.connection), "A").rows[0]
        self.transfer = transfer_row(self.connection, "T-X", "A", "B",
                                     self.expected_x.identity, self.expected_x)
        amend_row(self.connection, "A", self.ordinary.identity, self.ordinary, "storm", 3)
        moved = read_current_revision(
            CatalogRepository(self.connection), FactRepository(self.connection), "B").rows[0]
        amend_row(self.connection, "B", moved.identity, moved, "cooling", 9)
        post_batch(self.connection, "B", "meter-B", "Y", [("effluent", 4)])
        self.a5 = publish(self.connection, "A", 5, "A5")
        self.b1 = publish(self.connection, "B", 1, "B1")
        self.b3 = publish(self.connection, "B", 3, "B3")

    def mirror(self):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        return MirrorRepository(connection)

    def base(self, mirror):
        install_release(mirror, self.a2)
        install_release(mirror, self.b0)

    def lit(self, mirror, partition):
        return [row.liters for row in audit_partition(mirror, partition).rows]

    def test_only_new_partition_side_keeps_original_state(self):
        mirror = self.mirror()
        self.base(mirror)
        self.assertIsNone(read_transfer_status(mirror, "T-X"))
        install_release(mirror, self.b3)
        self.assertEqual(read_transfer_status(mirror, "T-X"), "pending")
        self.assertEqual(self.lit(mirror, "A"), [7, 2])
        self.assertEqual(self.lit(mirror, "B"), [4])
        install_release(mirror, self.a5)
        self.assertEqual(read_transfer_status(mirror, "T-X"), "complete")
        self.assertEqual(self.lit(mirror, "A"), [3])
        self.assertEqual(self.lit(mirror, "B"), [9, 4])

    def test_only_original_partition_side_shows_pre_move_final_row(self):
        mirror = self.mirror()
        self.base(mirror)
        install_release(mirror, self.a5)
        self.assertEqual(read_transfer_status(mirror, "T-X"), "pending")
        self.assertEqual(self.lit(mirror, "A"), [8, 3])
        alpha = audit_partition(mirror, "A")
        self.assertEqual(alpha.rows[0].provenance, self.expected_x.provenance)
        self.assertEqual(alpha.tally.liters, 11)
        self.assertEqual(self.lit(mirror, "B"), [])
        install_release(mirror, self.b3)
        self.assertEqual(read_transfer_status(mirror, "T-X"), "complete")
        self.assertEqual(self.lit(mirror, "A"), [3])
        self.assertEqual(self.lit(mirror, "B"), [9, 4])

    def test_old_and_replayed_releases_do_not_regress(self):
        for order in (("B3", "A5"), ("A5", "B3")):
            with self.subTest(order=order):
                mirror = self.mirror()
                self.base(mirror)
                artifacts = {"A5": self.a5, "B3": self.b3}
                install_release(mirror, artifacts[order[0]])
                install_release(mirror, artifacts[order[1]])
                install_release(mirror, self.b1)
                install_release(mirror, self.a2)
                self.assertEqual((self.lit(mirror, "A"), self.lit(mirror, "B")), ([3], [9, 4]))
                self.assertEqual(read_transfer_status(mirror, "T-X"), "complete")
                self.assertEqual(install_release(mirror, self.b3).current_revision, 3)
                self.assertEqual([row.liters for row in audit_release(mirror, "A2").rows], [7, 2])
                self.assertEqual([row.liters for row in audit_release(mirror, "B3").rows], [9, 4])

    def test_reopen_keeps_formal_and_fixed_accounts(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "mirror.sqlite3"
            with closing(sqlite3.connect(path)) as connection:
                mirror = MirrorRepository(connection)
                self.base(mirror)
                install_release(mirror, self.b3)
                install_release(mirror, self.a5)
            with closing(sqlite3.connect(path)) as reopened:
                mirror = MirrorRepository(reopened)
                self.assertEqual((self.lit(mirror, "A"), self.lit(mirror, "B")), ([3], [9, 4]))
                self.assertEqual(read_transfer_status(mirror, "T-X"), "complete")
                self.assertEqual([row.liters for row in audit_release(mirror, "A2").rows], [7, 2])

    def test_missing_transfer_evidence_cannot_advance(self):
        mirror = self.mirror()
        self.base(mirror)
        install_release(mirror, self.b3)
        manifest, records, transfers = decode_v2(self.a5)
        stripped = build_transfer_manifest("A5-naked", manifest.snapshot_id, manifest.partition,
                                           manifest.revision, records, ())
        artifact = encode_v2(stripped, records, ())
        with self.assertRaisesRegex(ValueError, "^missing_transfer_evidence$"):
            install_release(mirror, artifact)
        self.assertEqual(self.lit(mirror, "A"), [7, 2])
        self.assertIsNone(mirror.find_receipt("A5-naked"))
        self.assertEqual(read_transfer_status(mirror, "T-X"), "pending")

    def test_contradictory_binding_is_rejected(self):
        mirror = self.mirror()
        self.base(mirror)
        install_release(mirror, self.b3)
        manifest, records, transfers = decode_v2(self.a5)
        evidence = transfers[0]
        wrong_revisions = (replace(evidence, from_revision=evidence.from_revision + 1),)
        wrong_revision_manifest = build_transfer_manifest(
            "A5-x", manifest.snapshot_id, "A", 5, records, wrong_revisions)
        with self.assertRaisesRegex(ValueError, "^transfer_binding_conflict$"):
            install_release(mirror, encode_v2(wrong_revision_manifest, records, wrong_revisions))
        other_binding = (replace(evidence, row=replace(evidence.row, liters=1)),)
        other_manifest = build_transfer_manifest(
            "A5-y", manifest.snapshot_id, "A", 5, records, other_binding)
        with self.assertRaisesRegex(ValueError, "^transfer_binding_conflict$"):
            install_release(mirror, encode_v2(other_manifest, records, other_binding))
        self.assertEqual(read_transfer_status(mirror, "T-X"), "pending")
        self.assertEqual(self.lit(mirror, "A"), [7, 2])
        self.assertIsNone(mirror.find_receipt("A5-x"))
        self.assertIsNone(mirror.find_receipt("A5-y"))

    def test_second_transfer_for_same_row_is_rejected(self):
        mirror = self.mirror()
        self.base(mirror)
        install_release(mirror, self.b3)
        manifest, records, transfers = decode_v2(self.a5)
        evidence = transfers[0]
        # A second id claiming the same original row is a contradiction, even
        # if it points back to the original partition.
        rebound = (replace(evidence, transfer_id="T-Y"),)
        rebound_manifest = build_transfer_manifest("A5-z", manifest.snapshot_id, "A", 5, records, rebound)
        with self.assertRaisesRegex(ValueError, "^transfer_binding_conflict$"):
            install_release(mirror, encode_v2(rebound_manifest, records, rebound))
        self.assertIsNone(mirror.find_receipt("A5-z"))

    def test_commit_failure_leaves_no_progress_then_resend_completes(self):
        def fail():
            raise sqlite3.OperationalError("mirror commit interrupted")

        with TemporaryDirectory() as directory:
            path = Path(directory) / "mirror.sqlite3"
            with closing(sqlite3.connect(path)) as connection:
                mirror = MirrorRepository(connection)
                self.base(mirror)
                install_release(mirror, self.b3)
            with closing(sqlite3.connect(path)) as connection:
                mirror = MirrorRepository(connection, before_commit=fail)
                with self.assertRaisesRegex(sqlite3.OperationalError, "mirror commit interrupted"):
                    install_release(mirror, self.a5)
            with closing(sqlite3.connect(path)) as connection:
                mirror = MirrorRepository(connection)
                self.assertEqual(read_transfer_status(mirror, "T-X"), "pending")
                self.assertEqual(self.lit(mirror, "A"), [7, 2])
                self.assertEqual(self.lit(mirror, "B"), [4])
                self.assertIsNone(mirror.find_receipt("A5"))
                result = install_release(mirror, self.a5)
                self.assertEqual((result.receipt.release_id, result.current_revision), ("A5", 5))
                self.assertEqual(read_transfer_status(mirror, "T-X"), "complete")
                self.assertEqual((self.lit(mirror, "A"), self.lit(mirror, "B")), ([3], [9, 4]))

    def test_same_revision_cannot_mix_moved_and_native_rows(self):
        mirror = self.mirror()
        self.base(mirror)
        manifest, records, transfers = decode_v2(self.b3)
        from dataclasses import replace as dc_replace
        from warehouse.close.tally import tally_rows
        from warehouse.release.manifest import build_transfer_manifest as build
        from warehouse.release.records import LedgerRecords
        # Duplicate a native row into the moved row's revision and shift the
        # moved row's evidence revision so both claim revision 3.
        moved = next(row for row in records.rows if row.provenance.partition != "B")
        native = next(row for row in records.rows if row.provenance.partition == "B"
                      and row.provenance.accepted_revision == 3)
        forged_rows = (moved, dc_replace(native, provenance=dc_replace(native.provenance, accepted_revision=3)))
        forged_records = LedgerRecords(forged_rows, tally_rows(forged_rows))
        evidence = (dc_replace(transfers[0], to_revision=3, current=moved),)
        forged_manifest = build("B3-mixed", "fixed-forged", "B", 3, forged_records, evidence)
        with self.assertRaisesRegex(ValueError, "^invalid_row_order$"):
            install_release(mirror, encode_v2(forged_manifest, forged_records, evidence))
        self.assertIsNone(mirror.find_receipt("B3-mixed"))

    def test_bad_artifact_and_same_revision_conflict_leave_mirror_untouched(self):
        mirror = self.mirror()
        self.base(mirror)
        install_release(mirror, self.b3)
        install_release(mirror, self.a5)
        before = (audit_partition(mirror, "A"), audit_partition(mirror, "B"))
        with self.assertRaisesRegex(ValueError, "^invalid_artifact_json$"):
            install_release(mirror, b"\xff")
        manifest, records, transfers = decode_v2(self.a5)
        # A different business account for the same partition revision conflicts
        # even when its own digest and tally are self-consistent.
        from dataclasses import replace as dc_replace
        from warehouse.close.tally import tally_rows
        from warehouse.release.manifest import build_transfer_manifest as build
        from warehouse.release.records import LedgerRecords
        changed_rows = (dc_replace(records.rows[0], liters=99),)
        changed = LedgerRecords(changed_rows, tally_rows(changed_rows))
        changed_manifest = build("A5-other", "fixed-other", "A", 5, changed, transfers)
        with self.assertRaisesRegex(ValueError, "^received_revision_conflict$"):
            install_release(mirror, encode_v2(changed_manifest, changed, transfers))
        broken = LedgerRecords((dc_replace(records.rows[0], liters=99),), records.tally)
        broken_manifest = build("A5-broken", "fixed-broken", "A", 5, broken, transfers)
        with self.assertRaisesRegex(ValueError, "^bad_tally$"):
            install_release(mirror, encode_v2(broken_manifest, broken, transfers))
        self.assertEqual((audit_partition(mirror, "A"), audit_partition(mirror, "B")), before)
        self.assertIsNone(mirror.find_receipt("A5-other"))
        self.assertIsNone(mirror.find_receipt("A5-broken"))

    def test_release_without_transfer_status_is_none(self):
        mirror = self.mirror()
        self.base(mirror)
        self.assertIsNone(read_transfer_status(mirror, "T-unknown"))


class TransferShiftTests(unittest.TestCase):
    """The published drill, run through ``unittest`` as part of the regression."""

    def test_two_orders_and_retry(self):
        report = transfer_report()
        expected = report["expected"]
        for key in ("new_partition_first", "original_partition_first"):
            case = report[key]
            self.assertEqual(case["after_both"]["甲"]["rows"], expected["complete"]["甲"])
            self.assertEqual(case["after_both"]["乙"]["rows"], expected["complete"]["乙"])
            self.assertEqual(case["after_reopen"]["status"], "complete")
            self.assertEqual(case["a2_account"]["rows"], [7, 2])
            self.assertEqual(case["b3_account"]["rows"], [9, 4])
        self.assertEqual(report["original_partition_first"]["after_first"]["甲"]["rows"], [8, 3])
        self.assertEqual(report["new_partition_first"]["after_first"]["甲"]["rows"], [7, 2])
        retry = report["commit_failure_retry"]
        self.assertEqual(retry["status_after_failure"], "pending")
        self.assertEqual(retry["resend"], ["A5", 5])
        self.assertEqual(retry["after_resend"]["甲"]["rows"], [3])

    def test_prepare_shift_exposes_old_images(self):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        case = prepare_shift(connection)
        self.assertEqual(case.stale_x.liters, 7)
        self.assertEqual(case.expected_x.liters, 8)
        self.assertEqual(case.stale_x.identity, case.expected_x.identity)
        completed = complete_shift(connection, case)
        self.assertIsNotNone(completed.a5)


if __name__ == "__main__":
    unittest.main()