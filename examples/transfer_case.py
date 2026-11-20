from contextlib import closing
from dataclasses import dataclass
import json
import sqlite3

from warehouse.catalog.bootstrap import initialize_registry
from warehouse.catalog.classification import Classification
from warehouse.catalog.partitions import PartitionDefinition
from warehouse.catalog.sources import SourceRegistration
from warehouse.close.freeze import freeze_partition
from warehouse.facts.amendments import amend_row
from warehouse.facts.batches import post_batch
from warehouse.facts.rows import MeasurementRow
from warehouse.mirror.admission import install_release
from warehouse.mirror.read import audit_partition
from warehouse.mirror.repository import MirrorRepository
from warehouse.release.issue import issue_release

DELIVERY_ORDERS = (("B3", "A5"), ("A5", "B3"))


@dataclass(frozen=True)
class ShiftInputs:
    a2: bytes
    b0: bytes
    stale_x: MeasurementRow
    expected_x: MeasurementRow
    ordinary_a: MeasurementRow


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


def main():
    with closing(sqlite3.connect(":memory:")) as source:
        case = prepare_shift(source)
    with closing(sqlite3.connect(":memory:")) as target:
        mirror = MirrorRepository(target)
        install_release(mirror, case.a2)
        install_release(mirror, case.b0)
        observed = {}
        for partition in ("A", "B"):
            audit = audit_partition(mirror, partition)
            observed[partition] = [audit.manifest.revision, audit.tally.count, audit.tally.liters]
        if observed != {"A": [2, 2, 9], "B": [0, 0, 0]} or case.expected_x.liters != 8:
            raise RuntimeError("Unexpected V1 transfer fixture input")
        print(json.dumps(observed, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
