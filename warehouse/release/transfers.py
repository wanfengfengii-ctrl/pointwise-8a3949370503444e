"""Transfer evidence carried by V2 hand-off artifacts.

A transfer evidence record binds one transfer id to the original row identity,
the two partitions, and the revision each partition reached when the transfer
was first executed.  It also carries two complete rows:

``row``
    The final row as it stood in the original partition immediately before the
    move out.  This value is immutable and identical on every artifact that
    mentions the transfer; it is what lets an original-partition artifact show
    the row at its pre-move position while only that side has been received.

``current``
    The complete current value of the transfer fact inside the artifact's own
    partition revision.  On a new-partition artifact this is the row present in
    the ledger at that revision; on an original-partition artifact the row has
    moved out, so this is ``None``.

Encoding: every record is a JSON object with exactly the keys
``transfer_id, identity, from_partition, to_partition, from_revision,
to_revision, row, current``.  ``identity`` has the four identity fields;
``row``/``current`` use the same detail record shape as the ledger
(``category``, integer ``liters``, ``provenance``) or ``null`` for ``current``.
The artifact digest covers the ledger together with the transfer records sorted
by ``(transfer_id, from_partition, to_partition)``, so list order is irrelevant
while any content change alters the digest.
"""

from dataclasses import asdict, dataclass
from hashlib import sha256

from warehouse.catalog.partitions import require_identifier
from warehouse.facts.rows import MeasurementRow, RowIdentity, row_from_data
from warehouse.release.records import canonical_json_bytes

_EVIDENCE_FIELDS = {
    'transfer_id', 'identity', 'from_partition', 'to_partition',
    'from_revision', 'to_revision', 'row', 'current',
}
_IDENTITY_FIELDS = {'partition', 'source', 'batch', 'position'}


@dataclass(frozen=True)
class TransferEvidence:
    transfer_id: str
    identity: RowIdentity
    from_partition: str
    to_partition: str
    from_revision: int
    to_revision: int
    row: MeasurementRow
    current: MeasurementRow | None

    def __post_init__(self):
        require_identifier(self.transfer_id, 'transfer_id')
        if not isinstance(self.identity, RowIdentity):
            raise ValueError('invalid_transfer_record')
        require_identifier(self.from_partition, 'partition')
        require_identifier(self.to_partition, 'partition')
        if self.identity.partition != self.from_partition:
            raise ValueError('invalid_transfer_record')
        if self.from_partition == self.to_partition:
            raise ValueError('invalid_transfer_record')
        for revision in (self.from_revision, self.to_revision):
            if type(revision) is not int or revision < 1:
                raise ValueError('invalid_transfer_record')
        if not isinstance(self.row, MeasurementRow) or self.row.identity != self.identity:
            raise ValueError('invalid_transfer_record')
        if self.current is not None and (
                not isinstance(self.current, MeasurementRow) or self.current.identity != self.identity):
            raise ValueError('invalid_transfer_record')

    def binding(self) -> tuple:
        """The parts of a transfer that must agree across every artifact."""
        return (self.transfer_id, self.identity, self.from_partition, self.to_partition,
                self.from_revision, self.to_revision, self.row)


def evidence_to_data(evidence: TransferEvidence) -> dict:
    return {
        'transfer_id': evidence.transfer_id,
        'identity': asdict(evidence.identity),
        'from_partition': evidence.from_partition,
        'to_partition': evidence.to_partition,
        'from_revision': evidence.from_revision,
        'to_revision': evidence.to_revision,
        'row': asdict(evidence.row),
        'current': None if evidence.current is None else asdict(evidence.current),
    }


def evidence_from_data(data) -> TransferEvidence:
    if not isinstance(data, dict) or set(data) != _EVIDENCE_FIELDS:
        raise ValueError('invalid_transfer_record')
    identity = data['identity']
    if not isinstance(identity, dict) or set(identity) != _IDENTITY_FIELDS:
        raise ValueError('invalid_transfer_record')
    try:
        row = row_from_data(data['row'])
        current = None if data['current'] is None else row_from_data(data['current'])
        return TransferEvidence(
            data['transfer_id'],
            RowIdentity(**identity),
            data['from_partition'],
            data['to_partition'],
            data['from_revision'],
            data['to_revision'],
            row,
            current,
        )
    except (TypeError, KeyError) as error:
        raise ValueError('invalid_transfer_record') from error


def sort_transfers(transfers):
    return tuple(sorted(transfers, key=lambda item: (
        item.transfer_id, item.from_partition, item.to_partition)))


def encode_transfers(transfers) -> list:
    return [evidence_to_data(item) for item in sort_transfers(transfers)]


def transfer_digest(transfers) -> str:
    return sha256(canonical_json_bytes(encode_transfers(transfers))).hexdigest()


def canonical_business_bytes(records, transfers) -> bytes:
    """Canonical bytes for both the ledger and the applicable transfer evidence.

    Used by the receiver to compare two artifacts bound to the same partition
    revision.  V1 artifacts contribute an empty transfer list.
    """
    from warehouse.release.records import encode_ledger_records

    return canonical_json_bytes({
        'ledger': encode_ledger_records(records),
        'transfers': encode_transfers(transfers),
    })
