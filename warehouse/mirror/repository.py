from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
import json
import sqlite3


@dataclass(frozen=True)
class Receipt:
    release_id: str
    partition: str
    revision: int


@dataclass(frozen=True)
class ReceivedArtifact:
    receipt: Receipt
    artifact: bytes


@dataclass(frozen=True)
class StoredReceipt:
    receipt: Receipt
    artifact: bytes
    canonical_business: bytes


class MirrorRepository:
    def __init__(self, connection: sqlite3.Connection, *, before_commit: Callable[[], None] | None = None):
        self.connection = connection
        self.before_commit = before_commit
        connection.execute('''CREATE TABLE IF NOT EXISTS mirror_received (
            release_id TEXT PRIMARY KEY NOT NULL,
            partition TEXT NOT NULL,
            revision INTEGER NOT NULL,
            artifact BLOB NOT NULL,
            canonical_ledger BLOB NOT NULL
        )''')
        connection.execute('''CREATE INDEX IF NOT EXISTS mirror_received_revision
            ON mirror_received (partition, revision)''')
        connection.execute('''CREATE TABLE IF NOT EXISTS mirror_current (
            partition TEXT PRIMARY KEY NOT NULL,
            release_id TEXT NOT NULL REFERENCES mirror_received (release_id)
        )''')
        # One row per transfer binding known to the mirror.  ``identity`` is
        # unique so one original row can never be bound to two transfers.
        connection.execute('''CREATE TABLE IF NOT EXISTS mirror_transfer (
            transfer_id TEXT PRIMARY KEY NOT NULL,
            identity_partition TEXT NOT NULL,
            identity_source TEXT NOT NULL,
            identity_batch TEXT NOT NULL,
            identity_position INTEGER NOT NULL,
            from_partition TEXT NOT NULL,
            to_partition TEXT NOT NULL,
            from_revision INTEGER NOT NULL,
            to_revision INTEGER NOT NULL,
            evidence_json TEXT NOT NULL,
            UNIQUE (identity_partition, identity_source, identity_batch, identity_position)
        )''')
        # Which side(s) of each transfer have produced received evidence.
        connection.execute('''CREATE TABLE IF NOT EXISTS mirror_transfer_arrival (
            transfer_id TEXT NOT NULL,
            side TEXT NOT NULL CHECK (side IN ('from', 'to')),
            release_id TEXT NOT NULL,
            PRIMARY KEY (transfer_id, side)
        )''')

    @contextmanager
    def transaction(self):
        self.connection.execute('BEGIN IMMEDIATE')
        with self.connection:
            yield
            if self.before_commit is not None:
                self.before_commit()

    def find_receipt(self, release_id: str) -> ReceivedArtifact | None:
        record = self.connection.execute('''SELECT release_id, partition, revision, artifact
            FROM mirror_received WHERE release_id = ?''', (release_id,)).fetchone()
        return self._received(record)

    def find_stored(self, release_id: str) -> StoredReceipt | None:
        record = self.connection.execute('''SELECT release_id, partition, revision, artifact, canonical_ledger
            FROM mirror_received WHERE release_id = ?''', (release_id,)).fetchone()
        if record is None:
            return None
        release_id, partition, revision, artifact, canonical = record
        return StoredReceipt(Receipt(release_id, partition, revision), artifact, canonical)

    def find_revision_ledger(self, partition: str, revision: int) -> bytes | None:
        """Canonical business bytes (ledger plus applicable transfer evidence)."""
        record = self.connection.execute('''SELECT canonical_ledger FROM mirror_received
            WHERE partition = ? AND revision = ? LIMIT 1''', (partition, revision)).fetchone()
        return record[0] if record is not None else None

    def list_receipts(self, partition: str | None = None) -> tuple[ReceivedArtifact, ...]:
        if partition is None:
            records = self.connection.execute('''SELECT release_id, partition, revision, artifact
                FROM mirror_received ORDER BY release_id''')
        else:
            records = self.connection.execute('''SELECT release_id, partition, revision, artifact
                FROM mirror_received WHERE partition = ? ORDER BY revision, release_id''', (partition,))
        return tuple(self._received(record) for record in records)

    def highest_received(self, partition: str) -> ReceivedArtifact | None:
        record = self.connection.execute('''SELECT release_id, partition, revision, artifact
            FROM mirror_received WHERE partition = ? ORDER BY revision DESC, release_id LIMIT 1''',
            (partition,)).fetchone()
        return self._received(record)

    def get_current(self, partition: str) -> ReceivedArtifact | None:
        record = self.connection.execute('''SELECT r.release_id, r.partition, r.revision, r.artifact
            FROM mirror_current AS c JOIN mirror_received AS r ON r.release_id = c.release_id
            WHERE c.partition = ?''', (partition,)).fetchone()
        return self._received(record)

    def insert_receipt(self, receipt: Receipt, artifact: bytes, canonical_ledger: bytes) -> None:
        self.connection.execute('INSERT INTO mirror_received VALUES (?, ?, ?, ?, ?)',
                                (receipt.release_id, receipt.partition, receipt.revision, artifact, canonical_ledger))

    def set_current(self, receipt: Receipt) -> None:
        self.connection.execute('''INSERT INTO mirror_current (partition, release_id) VALUES (?, ?)
            ON CONFLICT (partition) DO UPDATE SET release_id = excluded.release_id''',
            (receipt.partition, receipt.release_id))

    def find_transfer(self, transfer_id: str):
        record = self.connection.execute('''SELECT evidence_json FROM mirror_transfer
            WHERE transfer_id = ?''', (transfer_id,)).fetchone()
        return json.loads(record[0]) if record is not None else None

    def find_transfer_by_identity(self, identity):
        record = self.connection.execute('''SELECT transfer_id FROM mirror_transfer
            WHERE identity_partition = ? AND identity_source = ? AND identity_batch = ?
              AND identity_position = ?''',
            (identity.partition, identity.source, identity.batch, identity.position)).fetchone()
        return record[0] if record is not None else None

    def insert_transfer(self, evidence_data: dict) -> None:
        self.connection.execute('''INSERT INTO mirror_transfer VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (
            evidence_data['transfer_id'],
            evidence_data['identity']['partition'], evidence_data['identity']['source'],
            evidence_data['identity']['batch'], evidence_data['identity']['position'],
            evidence_data['from_partition'], evidence_data['to_partition'],
            evidence_data['from_revision'], evidence_data['to_revision'],
            json.dumps(evidence_data, ensure_ascii=False, sort_keys=True),
        ))

    def record_transfer_arrival(self, transfer_id: str, side: str, release_id: str) -> None:
        self.connection.execute('''INSERT OR IGNORE INTO mirror_transfer_arrival
            (transfer_id, side, release_id) VALUES (?, ?, ?)''', (transfer_id, side, release_id))

    def transfer_side_arrived(self, transfer_id: str, side: str) -> bool:
        record = self.connection.execute('''SELECT 1 FROM mirror_transfer_arrival
            WHERE transfer_id = ? AND side = ?''', (transfer_id, side)).fetchone()
        return record is not None

    def list_transfers(self, partition: str):
        records = self.connection.execute('''SELECT evidence_json FROM mirror_transfer
            WHERE from_partition = ? OR to_partition = ?
            ORDER BY transfer_id''', (partition, partition))
        return tuple(json.loads(record[0]) for record in records)

    @staticmethod
    def _received(record) -> ReceivedArtifact | None:
        if record is None:
            return None
        release_id, partition, revision, artifact = record
        return ReceivedArtifact(Receipt(release_id, partition, revision), artifact)
