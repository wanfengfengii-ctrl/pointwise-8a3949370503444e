"""Cross-partition transfer hand-off drill.

Follows MOVE_MIRROR.md.  ``prepare_shift`` builds partitions 甲 (A) and 乙 (B)
and returns the initial V1 hand-off artifacts plus the old images needed for the
transfer.  ``complete_shift`` then performs the transfer and the follow-up
corrections and issues the later artifacts.

``transfer_report`` runs the two delivery orders offline in separate mirrors,
drops the source connection, reopens each mirror, and finally demonstrates a
commit failure followed by a successful resend.  Any mismatch raises, so
``main`` exits non-zero.
"""

from contextlib import closing
from dataclasses import dataclass
import json
import sqlite3
from tempfile import TemporaryDirectory

from warehouse.catalog.bootstrap import initialize_registry
from warehouse.catalog.classification import Classification
from warehouse.catalog.partitions import PartitionDefinition
from warehouse.catalog.repository import CatalogRepository
from warehouse.catalog.sources import SourceRegistration
from warehouse.close.freeze import freeze_partition
from warehouse.facts.amendments import amend_row
from warehouse.facts.batches import post_batch
from warehouse.facts.repository import FactRepository
from warehouse.facts.revisions import read_current_revision
from warehouse.facts.rows import MeasurementRow
from warehouse.facts.transfers import transfer_row
from warehouse.mirror.admission import install_release
from warehouse.mirror.read import audit_partition, audit_release, read_transfer_status
from warehouse.mirror.repository import MirrorRepository
from warehouse.release.issue import issue_release

TRANSFER_ID = "T-X"


@dataclass(frozen=True)
class ShiftInputs:
    a2: bytes
    b0: bytes
    stale_x: MeasurementRow
    expected_x: MeasurementRow
    ordinary_a: MeasurementRow


@dataclass(frozen=True)
class CompletedShift:
    a5: bytes
    b1: bytes
    b3: bytes


class CommitGate:
    def __init__(self):
        self.reject_next = False

    def __call__(self):
        if self.reject_next:
            self.reject_next = False
            raise sqlite3.OperationalError("fixture: mirror commit interrupted")


def publish(connection, partition, revision, release_id):
    snapshot = freeze_partition(connection, f"fixed-{release_id}", partition, revision)
    return issue_release(connection, release_id, snapshot.snapshot_id)[1]


def prepare_shift(connection):
    initialize_registry(
        connection,
        (PartitionDefinition("A"), PartitionDefinition("B")),
        (SourceRegistration("meter-A", "A"), SourceRegistration("meter-B", "B")),
        (Classification("A", "cooling"), Classification("A", "storm"),
         Classification("B", "cooling"), Classification("B", "effluent")),
    )
    post_batch(connection, "A", "meter-A", "X", [("cooling", 7)])
    post_batch(connection, "A", "meter-A", "M", [("storm", 2)])
    preview = freeze_partition(connection, "fixed-A2", "A", 2)
    a2 = issue_release(connection, "A2", preview.snapshot_id)[1]
    b0 = publish(connection, "B", 0, "B0")
    stale_x, ordinary_a = preview.rows
    correction = amend_row(connection, "A", stale_x.identity, stale_x, "cooling", 8)
    return ShiftInputs(a2, b0, stale_x, correction.row, ordinary_a)


def complete_shift(connection, case: ShiftInputs) -> CompletedShift:
    catalog = CatalogRepository(connection)
    facts = FactRepository(connection)
    transfer_row(connection, TRANSFER_ID, "A", "B", case.expected_x.identity, case.expected_x)
    amend_row(connection, "A", case.ordinary_a.identity, case.ordinary_a, "storm", 3)
    moved = read_current_revision(catalog, facts, "B").rows[0]
    amend_row(connection, "B", moved.identity, moved, "cooling", 9)
    post_batch(connection, "B", "meter-B", "Y", [("effluent", 4)])
    return CompletedShift(
        a5=publish(connection, "A", 5, "A5"),
        b1=publish(connection, "B", 1, "B1"),
        b3=publish(connection, "B", 3, "B3"),
    )


def _summary(audit):
    return {
        "revision": audit.manifest.revision,
        "release_id": audit.manifest.release_id,
        "rows": [row.liters for row in audit.rows],
        "count": audit.tally.count,
        "liters": audit.tally.liters,
        "categories": [[item.category, item.count, item.liters] for item in audit.tally.categories],
        "origins": [[row.provenance.partition, row.provenance.source, row.provenance.batch,
                     row.provenance.position, row.provenance.accepted_revision] for row in audit.rows],
    }


def _observe(mirror):
    return {
        "甲": _summary(audit_partition(mirror, "A")),
        "乙": _summary(audit_partition(mirror, "B")),
        "status": read_transfer_status(mirror, TRANSFER_ID),
    }


def _shape(observation):
    return {
        "甲": observation["甲"]["rows"],
        "乙": observation["乙"]["rows"],
        "status": observation["status"],
    }


def _run_order(directory, case, completed, order):
    path = f"{directory}/mirror-{order[0]}-{order[-1]}.sqlite3"
    artifacts = {"A2": case.a2, "B0": case.b0, "A5": completed.a5, "B1": completed.b1, "B3": completed.b3}
    with closing(sqlite3.connect(path)) as target:
        mirror = MirrorRepository(target)
        install_release(mirror, artifacts["A2"])
        install_release(mirror, artifacts["B0"])
        before = _observe(mirror)
        install_release(mirror, artifacts[order[0]])
        after_first = _observe(mirror)
        install_release(mirror, artifacts[order[1]])
        after_both = _observe(mirror)
        install_release(mirror, artifacts["B1"])
        after_late_b1 = _observe(mirror)
        b3_account = _summary(audit_release(mirror, "B3"))
        a2_account = _summary(audit_release(mirror, "A2"))
    with closing(sqlite3.connect(path)) as reopened:
        mirror = MirrorRepository(reopened)
        after_reopen = _observe(mirror)
        a2_after_reopen = _summary(audit_release(mirror, "A2"))
        b3_after_reopen = _summary(audit_release(mirror, "B3"))
    return {
        "order": list(order),
        "before": before,
        "after_first": after_first,
        "after_both": after_both,
        "after_late_b1": after_late_b1,
        "after_reopen": after_reopen,
        "a2_account": a2_account,
        "b3_account": b3_account,
        "a2_account_after_reopen": a2_after_reopen,
        "b3_account_after_reopen": b3_after_reopen,
    }


def _run_retry(directory, case, completed):
    path = f"{directory}/mirror-retry.sqlite3"
    gate = CommitGate()
    with closing(sqlite3.connect(path)) as target:
        mirror = MirrorRepository(target, before_commit=gate)
        install_release(mirror, case.a2)
        install_release(mirror, case.b0)
        install_release(mirror, completed.b3)
        before = _observe(mirror)
        gate.reject_next = True
        failure = None
        try:
            install_release(mirror, completed.a5)
        except sqlite3.OperationalError as error:
            failure = str(error)
    with closing(sqlite3.connect(path)) as reopened:
        mirror = MirrorRepository(reopened)
        after_failure = _observe(mirror)
        status_after_failure = read_transfer_status(mirror, TRANSFER_ID)
        result = install_release(mirror, completed.a5)
        after_resend = _observe(mirror)
        resend = [result.receipt.release_id, result.current_revision]
    return {
        "failure": failure,
        "before": before,
        "after_failure": after_failure,
        "status_after_failure": status_after_failure,
        "resend": resend,
        "after_resend": after_resend,
    }


_EXPECTED = {
    "initial": {"甲": [7, 2], "乙": [], "status": None},
    "new_partition_only": {"甲": [7, 2], "乙": [4], "status": "pending"},
    "original_partition_only": {"甲": [8, 3], "乙": [], "status": "pending"},
    "complete": {"甲": [3], "乙": [9, 4], "status": "complete"},
}


def _check(condition, message):
    if not condition:
        raise RuntimeError(message)


def transfer_report():
    with closing(sqlite3.connect(":memory:")) as source:
        case = prepare_shift(source)
        completed = complete_shift(source, case)
    with TemporaryDirectory() as directory:
        new_first = _run_order(directory, case, completed, ("B3", "A5"))
        original_first = _run_order(directory, case, completed, ("A5", "B3"))
        retry = _run_retry(directory, case, completed)

    _check(_shape(new_first["before"]) == _EXPECTED["initial"],
           f"initial mirror wrong: {_shape(new_first['before'])}")
    _check(_shape(new_first["after_first"]) == _EXPECTED["new_partition_only"],
           f"B3-only mirror wrong: {_shape(new_first['after_first'])}")
    _check(_shape(new_first["after_both"]) == _EXPECTED["complete"],
           f"B3 then A5 mirror wrong: {_shape(new_first['after_both'])}")
    _check(_shape(original_first["after_first"]) == _EXPECTED["original_partition_only"],
           f"A5-only mirror wrong: {_shape(original_first['after_first'])}")
    _check(_shape(original_first["after_both"]) == _EXPECTED["complete"],
           f"A5 then B3 mirror wrong: {_shape(original_first['after_both'])}")
    for result in (new_first, original_first):
        _check(_shape(result["after_late_b1"]) == _EXPECTED["complete"],
               "late B1 regressed the formal ledger")
        _check(_shape(result["after_reopen"]) == _EXPECTED["complete"],
               "reopened mirror changed the formal ledger")
        _check(result["b3_account"]["rows"] == [9, 4], "B3 fixed account changed")
        _check(result["b3_account_after_reopen"]["rows"] == [9, 4], "B3 fixed account changed after reopen")
        _check(result["a2_account"]["rows"] == [7, 2], "A2 fixed account changed")
        _check(result["a2_account_after_reopen"]["rows"] == [7, 2], "A2 fixed account changed after reopen")
        _check(result["after_both"]["乙"]["origins"][0][0] == "A", "moved row lost its original partition")
        _check(result["after_both"]["乙"]["origins"][0][4] == 1, "moved row lost its accepted revision")

    _check(retry["failure"] == "fixture: mirror commit interrupted", "commit gate did not fire")
    _check(_shape(retry["after_failure"]) == _EXPECTED["new_partition_only"],
           f"failed commit changed the mirror: {_shape(retry['after_failure'])}")
    _check(retry["status_after_failure"] == "pending", "failed commit advanced transfer status")
    _check(retry["resend"] == ["A5", 5], f"resend receipt wrong: {retry['resend']}")
    _check(_shape(retry["after_resend"]) == _EXPECTED["complete"], "resend did not complete the transfer")

    return {
        "case_id": "move-mirror-v1",
        "new_partition_first": new_first,
        "original_partition_first": original_first,
        "commit_failure_retry": retry,
        "expected": _EXPECTED,
        "fixed_accounts": {"A2": [7, 2], "B3": [9, 4]},
    }


def main():
    report = transfer_report()
    print("POINTWISE_TRANSFER " + json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
