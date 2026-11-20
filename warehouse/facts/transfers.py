"""Cross-partition row transfers on the source side.

A transfer moves one measurement row from its original partition to another
partition.  Both partitions gain a revision in a single transaction, together
with the first execution record.  The row keeps its original ``RowIdentity``,
its original provenance and the original accepted revision forever; only the
partition that owns it changes.  Subsequent corrections happen in the new
partition with ``amend_row``.

Replaying ``transfer_row`` with the same transfer id and the same arguments
returns the first receipt, even if the row has since been corrected in the new
partition.  A different argument set, a stale old image, or a second transfer
id bound to the same row is rejected without reserving an id or changing either
partition.
"""

from dataclasses import dataclass

from warehouse.catalog.classification import require_partition_category
from warehouse.catalog.partitions import require_identifier, require_partition
from warehouse.catalog.repository import CatalogRepository, require_registry
from warehouse.facts.repository import FactRepository, StoredTransfer
from warehouse.facts.revisions import read_current_revision, read_fact_revision
from warehouse.facts.rows import MeasurementRow, RowIdentity
from warehouse.release.transfers import TransferEvidence


@dataclass(frozen=True)
class TransferReceipt:
    transfer_id: str
    identity: RowIdentity
    from_partition: str
    to_partition: str
    from_revision: int
    to_revision: int
    row: MeasurementRow


def _validate_arguments(connection, transfer_id, from_partition, to_partition, identity, expected):
    require_identifier(transfer_id, 'transfer_id')
    require_identifier(from_partition, 'partition')
    require_identifier(to_partition, 'partition')
    if from_partition == to_partition:
        raise ValueError('transfer_partition_conflict')
    if not isinstance(identity, RowIdentity):
        raise ValueError('invalid_row_identity')
    if identity.partition != from_partition:
        raise ValueError('transfer_identity_mismatch')
    if not isinstance(expected, MeasurementRow):
        raise ValueError('invalid_old_image')
    catalog = CatalogRepository(connection)
    registry = require_registry(catalog)
    require_partition(registry.partition_index(), from_partition)
    require_partition(registry.partition_index(), to_partition)
    return catalog, registry


def transfer_row(connection, transfer_id, from_partition, to_partition, identity, expected) -> TransferReceipt:
    catalog, registry = _validate_arguments(connection, transfer_id, from_partition, to_partition,
                                            identity, expected)
    facts = FactRepository(connection)
    with facts.transaction():
        stored = facts.find_transfer(transfer_id)
        if stored is not None:
            if (stored.from_partition, stored.to_partition, stored.identity, stored.row) != (
                    from_partition, to_partition, identity, expected):
                raise ValueError('transfer_conflict')
            return _receipt(stored)

        if facts.find_transfer_by_identity(identity) is not None:
            raise ValueError('row_already_transferred')

        view = read_current_revision(catalog, facts, from_partition)
        current = next((row for row in view.rows if row.identity == identity), None)
        if current is None:
            raise ValueError('unknown_row')
        if current != expected:
            raise ValueError('stale_old_image')
        require_partition_category(registry.classification_index(), to_partition, expected.category)

        from_revision = view.revision + 1
        to_revision = facts.head_revision(to_partition) + 1
        evidence = TransferEvidence(transfer_id, identity, from_partition, to_partition,
                                    from_revision, to_revision, expected, expected)
        facts.append_transfer(evidence)
    return TransferReceipt(transfer_id, identity, from_partition, to_partition,
                           from_revision, to_revision, expected)


def _receipt(stored: StoredTransfer) -> TransferReceipt:
    return TransferReceipt(stored.transfer_id, stored.identity, stored.from_partition,
                           stored.to_partition, stored.from_revision, stored.to_revision, stored.row)


def release_transfer_evidence(connection, partition: str, revision: int) -> tuple[TransferEvidence, ...]:
    """Evidence that a fixed account of ``partition`` at ``revision`` must carry.

    Both the move-out side and the move-in side carry the immutable pre-move
    row, identity, both partitions and both transfer revisions.  On the
    move-in side the record additionally states the complete current value of
    the transfer fact at that revision; on the move-out side the row is gone,
    so ``current`` is ``None``.

    Monotonic transfers mean every qualifying record already exists when the
    account is frozen, so issuing the same release twice reproduces the same
    bytes.
    """
    catalog = CatalogRepository(connection)
    facts = FactRepository(connection)
    require_partition(require_registry(catalog).partition_index(), partition)
    if type(revision) is not int or revision < 0 or revision > facts.head_revision(partition):
        raise ValueError('unknown_revision')
    stored = facts.transfers_for_partition(partition, revision)
    evidence = []
    current_rows = None
    for record in stored:
        if record.to_partition == partition:
            if current_rows is None:
                current_rows = {row.identity: row for row in
                                read_fact_revision(catalog, facts, partition, revision).rows}
            current = current_rows.get(record.identity)
        else:
            current = None
        evidence.append(TransferEvidence(record.transfer_id, record.identity, record.from_partition,
                                         record.to_partition, record.from_revision, record.to_revision,
                                         record.row, current))
    return tuple(evidence)
