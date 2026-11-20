from dataclasses import dataclass

from warehouse.catalog.partitions import require_identifier
from warehouse.close.tally import LedgerTally, tally_rows
from warehouse.mirror.repository import MirrorRepository, ReceivedArtifact
from warehouse.release.format import parse_artifact
from warehouse.release.manifest import ReleaseManifest
from warehouse.release.records import DetailRecord
from warehouse.release.transfers import evidence_from_data


@dataclass(frozen=True)
class AuditResult:
    manifest: ReleaseManifest
    rows: tuple[DetailRecord, ...]
    tally: LedgerTally


def _decode(received: ReceivedArtifact):
    return parse_artifact(received.artifact)


def _read_artifact(received: ReceivedArtifact) -> AuditResult:
    _, manifest, records, _ = _decode(received)
    return AuditResult(manifest, records.rows, records.tally)


def _known_evidence(repository: MirrorRepository, partition: str, transfers) -> dict:
    """All transfer records the mirror knows for a partition, by transfer id.

    Evidence carried by the artifact at hand overrides anything learned earlier.
    """
    combined = {}
    for data in repository.list_transfers(partition):
        evidence = evidence_from_data(data)
        combined[evidence.transfer_id] = evidence
    for evidence in transfers:
        combined[evidence.transfer_id] = evidence
    return combined


def _order_key(row):
    return (row.provenance.accepted_revision, row.provenance.source,
            row.provenance.batch, row.provenance.position)


def _arrivals(repository: MirrorRepository, transfer_id: str):
    return (repository.transfer_side_arrived(transfer_id, 'from'),
            repository.transfer_side_arrived(transfer_id, 'to'))


def audit_partition(repository: MirrorRepository, partition: str) -> AuditResult:
    require_identifier(partition, 'partition')
    current = repository.get_current(partition)
    if current is None:
        raise ValueError('unknown_partition')
    _, manifest, records, transfers = _decode(current)
    known = _known_evidence(repository, partition, transfers)
    rows = list(records.rows)
    present = {row.identity for row in rows}

    restored = []
    held = set()
    for evidence in known.values():
        arrived_from, arrived_to = _arrivals(repository, evidence.transfer_id)
        if evidence.from_partition == partition:
            if arrived_from and not arrived_to and evidence.identity not in present:
                # Only the move-out side has arrived: keep the row visible at
                # its pre-move final value and original position in this
                # partition until the move-in side is received.
                restored.append(evidence.row)
        elif evidence.to_partition == partition:
            if arrived_to and not arrived_from and evidence.identity in present:
                # Only the move-in side has arrived: the row keeps waiting.
                # Retain the mirror-known original-partition state instead of
                # showing the row here.
                held.add(evidence.identity)

    if restored:
        rows.extend(restored)
        rows.sort(key=_order_key)
    if held:
        rows = [row for row in rows if row.identity not in held]

    return AuditResult(manifest, tuple(rows), tally_rows(rows))


def audit_release(repository: MirrorRepository, release_id: str) -> AuditResult:
    require_identifier(release_id, 'release_id')
    received = repository.find_receipt(release_id)
    if received is None:
        raise ValueError('unreceived_release')
    return _read_artifact(received)


def read_transfer_status(repository: MirrorRepository, transfer_id: str):
    require_identifier(transfer_id, 'transfer_id')
    if repository.find_transfer(transfer_id) is None:
        return None
    arrived_from, arrived_to = _arrivals(repository, transfer_id)
    if arrived_from and arrived_to:
        return 'complete'
    if arrived_from or arrived_to:
        return 'pending'
    return None
