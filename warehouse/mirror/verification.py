from dataclasses import dataclass
from hashlib import sha256

from warehouse.close.tally import tally_rows
from warehouse.release.format import V1_FORMAT, parse_artifact
from warehouse.release.manifest import ReleaseManifest
from warehouse.release.records import LedgerRecords, canonical_ledger_bytes
from warehouse.release.transfers import canonical_business_bytes, transfer_digest


@dataclass(frozen=True)
class VerifiedLedger:
    manifest: ReleaseManifest
    artifact: bytes
    canonical_ledger: bytes
    canonical_business: bytes
    transfers: tuple = ()


def _verify_transfer_items(manifest: ReleaseManifest, records: LedgerRecords, transfers) -> None:
    ledger_by_identity = {row.identity: row for row in records.rows}
    seen_ids = set()
    seen_identities = set()
    for evidence in transfers:
        if evidence.transfer_id in seen_ids:
            raise ValueError('duplicate_transfer_evidence')
        if evidence.identity in seen_identities:
            raise ValueError('duplicate_transfer_identity')
        seen_ids.add(evidence.transfer_id)
        seen_identities.add(evidence.identity)
        origin = evidence.row.provenance
        if origin.partition != evidence.from_partition:
            raise ValueError('invalid_transfer_binding')
        if origin.accepted_revision > evidence.from_revision:
            raise ValueError('invalid_transfer_binding')
        if manifest.partition == evidence.from_partition:
            if manifest.revision < evidence.from_revision:
                raise ValueError('invalid_transfer_binding')
            if evidence.identity in ledger_by_identity:
                raise ValueError('invalid_transfer_binding')
            if evidence.current is not None:
                raise ValueError('invalid_transfer_binding')
        elif manifest.partition == evidence.to_partition:
            if manifest.revision < evidence.to_revision:
                raise ValueError('invalid_transfer_binding')
            if evidence.identity not in ledger_by_identity:
                raise ValueError('transfer_row_missing')
            if evidence.current != ledger_by_identity[evidence.identity]:
                raise ValueError('transfer_row_mismatch')
        else:
            raise ValueError('transfer_partition_mismatch')


def _entry(manifest: ReleaseManifest, row, evidence_by_identity):
    """Return ``(entry_revision, moved_in)`` for a ledger row of this partition."""
    if row.provenance.partition == manifest.partition:
        revision = row.provenance.accepted_revision
        if revision > manifest.revision:
            raise ValueError('provenance_revision_mismatch')
        return revision, False
    evidence = evidence_by_identity.get(row.identity)
    if evidence is None or evidence.to_partition != manifest.partition:
        raise ValueError('provenance_partition_mismatch')
    revision = evidence.to_revision
    if revision > manifest.revision:
        raise ValueError('provenance_revision_mismatch')
    return revision, True


def _verify_row_order(manifest: ReleaseManifest, records: LedgerRecords, transfers) -> None:
    evidence_by_identity = {evidence.identity: evidence for evidence in transfers}
    moved_out = {}
    for evidence in transfers:
        if evidence.from_partition == manifest.partition:
            key = (evidence.identity.source, evidence.identity.batch)
            moved_out.setdefault(key, set()).add(evidence.identity.position)

    seen_rows = set()
    last_key = None
    batches = {}
    revision_kinds = {}
    for row in records.rows:
        identity = row.identity
        if identity in seen_rows:
            raise ValueError('duplicate_row_identity')
        seen_rows.add(identity)
        revision, moved_in = _entry(manifest, row, evidence_by_identity)
        # Within a revision, native batch rows keep ascending positions and a
        # moved-in row sits alone.  Revisions are one event each.
        if revision_kinds.setdefault(revision, moved_in) != moved_in:
            raise ValueError('invalid_row_order')
        event_key = (revision, 0 if moved_in else row.provenance.position)
        if last_key is not None and event_key <= last_key:
            raise ValueError('invalid_row_order')
        last_key = event_key
        if not moved_in:
            batch_key = (row.provenance.source, row.provenance.batch)
            entry = batches.setdefault(batch_key, {'revision': revision, 'positions': []})
            if entry['revision'] != revision:
                raise ValueError('invalid_row_order')
            entry['positions'].append(row.provenance.position)

    for batch_key, entry in batches.items():
        positions = entry['positions']
        if positions != sorted(set(positions)):
            raise ValueError('invalid_row_order')
        removed = moved_out.get(batch_key, set())
        if removed & set(positions):
            raise ValueError('invalid_row_order')
        covered = sorted(set(positions) | removed)
        if covered != list(range(1, len(covered) + 1)):
            raise ValueError('invalid_row_order')


def verify_artifact(artifact: bytes) -> VerifiedLedger:
    version, manifest, records, transfers = parse_artifact(artifact)
    ledger = canonical_ledger_bytes(records)
    if sha256(ledger).hexdigest() != manifest.ledger_sha256:
        raise ValueError('ledger_digest_mismatch')
    if version == V1_FORMAT:
        if transfers:
            raise ValueError('invalid_transfer_binding')
    else:
        if transfer_digest(transfers) != manifest.transfers_sha256:
            raise ValueError('transfers_digest_mismatch')
    _verify_transfer_items(manifest, records, transfers)
    _verify_row_order(manifest, records, transfers)
    if tally_rows(records.rows) != records.tally:
        raise ValueError('bad_tally')
    return VerifiedLedger(manifest, artifact, ledger,
                          canonical_business_bytes(records, transfers), transfers)
