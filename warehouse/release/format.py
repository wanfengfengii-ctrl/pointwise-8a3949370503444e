from dataclasses import asdict
import json

from warehouse.release.manifest import (
    ReleaseManifest, TransferManifest, parse_manifest, parse_transfer_manifest,
)
from warehouse.release.records import LedgerRecords, canonical_json_bytes, decode_ledger_records, encode_ledger_records
from warehouse.release.transfers import (
    TransferEvidence, encode_transfers, evidence_from_data, transfer_digest,
)


V1_FORMAT = 1
V2_FORMAT = 2


def encode_v1(manifest: ReleaseManifest, records: LedgerRecords) -> bytes:
    return canonical_json_bytes({
        'version': V1_FORMAT,
        'manifest': asdict(manifest),
        'ledger': encode_ledger_records(records),
    })


def encode_v2(manifest: TransferManifest, records: LedgerRecords,
              transfers: tuple[TransferEvidence, ...]) -> bytes:
    if transfer_digest(transfers) != manifest.transfers_sha256:
        raise ValueError('invalid_transfer_digest')
    return canonical_json_bytes({
        'version': V2_FORMAT,
        'manifest': asdict(manifest),
        'ledger': encode_ledger_records(records),
        'transfers': encode_transfers(transfers),
    })


def _unique_fields(pairs):
    fields = {}
    for key, value in pairs:
        if key in fields:
            raise ValueError('duplicate_json_field')
        fields[key] = value
    return fields


def _reject_constant(value):
    raise ValueError(f'invalid_json_constant: {value}')


def parse_artifact(artifact: bytes):
    """Return ``(version, manifest, records, transfers)`` after envelope checks only.

    Digest and business verification happen in ``warehouse.mirror.verification``.
    """
    if type(artifact) is not bytes:
        raise ValueError('invalid_artifact_bytes')
    try:
        data = json.loads(artifact.decode('utf-8'), object_pairs_hook=_unique_fields, parse_constant=_reject_constant)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as error:
        raise ValueError('invalid_artifact_json') from error
    if not isinstance(data, dict) or 'version' not in data:
        raise ValueError('invalid_artifact_envelope')
    version = data['version']
    if type(version) is not int:
        raise ValueError('unsupported_artifact_version')
    if version == V1_FORMAT:
        if set(data) != {'version', 'manifest', 'ledger'}:
            raise ValueError('invalid_artifact_envelope')
        return V1_FORMAT, parse_manifest(data['manifest']), decode_ledger_records(data['ledger']), ()
    if version == V2_FORMAT:
        if set(data) != {'version', 'manifest', 'ledger', 'transfers'}:
            raise ValueError('invalid_artifact_envelope')
        if not isinstance(data['transfers'], list):
            raise ValueError('invalid_artifact_envelope')
        manifest = parse_transfer_manifest(data['manifest'])
        records = decode_ledger_records(data['ledger'])
        transfers = tuple(evidence_from_data(item) for item in data['transfers'])
        return V2_FORMAT, manifest, records, transfers
    raise ValueError('unsupported_artifact_version')


def decode_v1(artifact: bytes) -> tuple[ReleaseManifest, LedgerRecords]:
    version, manifest, records, transfers = parse_artifact(artifact)
    if version != V1_FORMAT:
        raise ValueError('unsupported_artifact_version')
    return manifest, records


def decode_v2(artifact: bytes) -> tuple[TransferManifest, LedgerRecords, tuple[TransferEvidence, ...]]:
    version, manifest, records, transfers = parse_artifact(artifact)
    if version != V2_FORMAT:
        raise ValueError('unsupported_artifact_version')
    return manifest, records, transfers
