from dataclasses import dataclass

from warehouse.mirror.repository import MirrorRepository, Receipt
from warehouse.mirror.verification import VerifiedLedger
from warehouse.release.transfers import evidence_to_data


@dataclass(frozen=True)
class InstallResult:
    receipt: Receipt
    current_revision: int


def _side(partition: str, evidence) -> str | None:
    if partition == evidence.from_partition:
        return 'from'
    if partition == evidence.to_partition:
        return 'to'
    return None


def _known_binding(data: dict) -> tuple:
    return (data['transfer_id'], tuple(sorted(data['identity'].items())),
            data['from_partition'], data['to_partition'],
            data['from_revision'], data['to_revision'], data['row'])


def _check_known_bindings(repository: MirrorRepository, manifest, transfers) -> None:
    """Reject contradictory or missing transfer evidence without changing state."""
    incoming_ids = set()
    for evidence in transfers:
        incoming_ids.add(evidence.transfer_id)
        data = evidence_to_data(evidence)
        known = repository.find_transfer(evidence.transfer_id)
        if known is not None and _known_binding(known) != _known_binding(data):
            raise ValueError('transfer_binding_conflict')
        bound_id = repository.find_transfer_by_identity(evidence.identity)
        if bound_id is not None and bound_id != evidence.transfer_id:
            raise ValueError('transfer_binding_conflict')

    for known in repository.list_transfers(manifest.partition):
        transfer_id = known['transfer_id']
        side_revision = (known['from_revision'] if known['from_partition'] == manifest.partition
                         else known['to_revision'])
        if manifest.revision >= side_revision and transfer_id not in incoming_ids:
            raise ValueError('missing_transfer_evidence')


def _record_evidence(repository: MirrorRepository, manifest, transfers) -> None:
    for evidence in transfers:
        data = evidence_to_data(evidence)
        if repository.find_transfer(evidence.transfer_id) is None:
            repository.insert_transfer(data)
        side = _side(manifest.partition, evidence)
        repository.record_transfer_arrival(evidence.transfer_id, side, manifest.release_id)


def apply_release(repository: MirrorRepository, verified: VerifiedLedger) -> InstallResult:
    manifest = verified.manifest
    transfers = verified.transfers
    with repository.transaction():
        existing = repository.find_receipt(manifest.release_id)
        if existing is not None:
            if existing.artifact != verified.artifact:
                raise ValueError('received_issue_conflict')
            highest = repository.highest_received(manifest.partition)
            return InstallResult(existing.receipt, highest.receipt.revision)

        same_revision = repository.find_revision_ledger(manifest.partition, manifest.revision)
        if same_revision is not None and same_revision != verified.canonical_business:
            raise ValueError('received_revision_conflict')

        _check_known_bindings(repository, manifest, transfers)

        receipt = Receipt(manifest.release_id, manifest.partition, manifest.revision)
        repository.insert_receipt(receipt, verified.artifact, verified.canonical_business)
        _record_evidence(repository, manifest, transfers)
        current = repository.get_current(manifest.partition)
        if current is None or receipt.revision > current.receipt.revision:
            repository.set_current(receipt)
        highest = repository.highest_received(manifest.partition)
        return InstallResult(receipt, highest.receipt.revision)
