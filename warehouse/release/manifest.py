from dataclasses import dataclass
from hashlib import sha256

from warehouse.catalog.partitions import require_identifier
from warehouse.release.records import LedgerRecords, canonical_ledger_bytes


def _valid_digest(digest) -> bool:
    return type(digest) is str and len(digest) == 64 and all(ch in '0123456789abcdef' for ch in digest)


@dataclass(frozen=True)
class ReleaseManifest:
    release_id: str
    snapshot_id: str
    partition: str
    revision: int
    ledger_sha256: str

    def __post_init__(self):
        require_identifier(self.release_id, 'release_id')
        require_identifier(self.snapshot_id, 'snapshot_id')
        require_identifier(self.partition, 'partition')
        if type(self.revision) is not int or not 0 <= self.revision < 2**63:
            raise ValueError('invalid_release_revision')
        if not _valid_digest(self.ledger_sha256):
            raise ValueError('invalid_ledger_digest')


@dataclass(frozen=True)
class TransferManifest(ReleaseManifest):
    """Manifest of a V2 artifact, which also binds its transfer evidence."""

    transfers_sha256: str

    def __post_init__(self):
        super().__post_init__()
        if not _valid_digest(self.transfers_sha256):
            raise ValueError('invalid_transfer_digest')


def ledger_digest(records: LedgerRecords) -> str:
    return sha256(canonical_ledger_bytes(records)).hexdigest()


def build_manifest(release_id, snapshot_id, partition, revision, records: LedgerRecords) -> ReleaseManifest:
    return ReleaseManifest(release_id, snapshot_id, partition, revision, ledger_digest(records))


def build_transfer_manifest(release_id, snapshot_id, partition, revision, records: LedgerRecords,
                            transfers) -> TransferManifest:
    from warehouse.release.transfers import transfer_digest

    return TransferManifest(release_id, snapshot_id, partition, revision,
                            ledger_digest(records), transfer_digest(transfers))


def parse_manifest(data) -> ReleaseManifest:
    if not isinstance(data, dict) or set(data) != {'release_id', 'snapshot_id', 'partition', 'revision', 'ledger_sha256'}:
        raise ValueError('invalid_release_manifest')
    return ReleaseManifest(**data)


def parse_transfer_manifest(data) -> TransferManifest:
    fields = {'release_id', 'snapshot_id', 'partition', 'revision', 'ledger_sha256', 'transfers_sha256'}
    if not isinstance(data, dict) or set(data) != fields:
        raise ValueError('invalid_release_manifest')
    return TransferManifest(**data)
