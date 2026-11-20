from contextlib import contextmanager
from dataclasses import asdict, dataclass
import json
import sqlite3

from warehouse.facts.rows import MeasurementRow, RowIdentity, row_from_data

# Event kinds stored in fact_event.payload.  Batch events are recognised by the
# non-null batch_source column; the remaining kinds share a null source.
AMENDMENT_KIND = 'amendment'
TRANSFER_OUT_KIND = 'transfer_out'
TRANSFER_IN_KIND = 'transfer_in'


@dataclass(frozen=True)
class BatchEvent:
    revision: int
    source: str
    batch: str
    readings: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class AmendmentEvent:
    revision: int
    identity: RowIdentity
    category: str
    liters: int


@dataclass(frozen=True)
class TransferOutEvent:
    revision: int
    transfer_id: str
    identity: RowIdentity


@dataclass(frozen=True)
class TransferInEvent:
    revision: int
    transfer_id: str
    row: MeasurementRow


@dataclass(frozen=True)
class StoredTransfer:
    transfer_id: str
    identity: RowIdentity
    from_partition: str
    to_partition: str
    from_revision: int
    to_revision: int
    row: MeasurementRow


class FactRepository:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection
        connection.execute('''CREATE TABLE IF NOT EXISTS fact_event (
            partition TEXT NOT NULL,
            revision INTEGER NOT NULL CHECK (revision > 0),
            batch_source TEXT,
            batch_no TEXT,
            payload TEXT NOT NULL,
            PRIMARY KEY (partition, revision),
            UNIQUE (partition, batch_source, batch_no),
            CHECK ((batch_source IS NULL) = (batch_no IS NULL))
        )''')
        connection.execute('''CREATE TABLE IF NOT EXISTS fact_transfer (
            transfer_id TEXT PRIMARY KEY NOT NULL,
            identity_partition TEXT NOT NULL,
            identity_source TEXT NOT NULL,
            identity_batch TEXT NOT NULL,
            identity_position INTEGER NOT NULL,
            from_partition TEXT NOT NULL,
            to_partition TEXT NOT NULL,
            from_revision INTEGER NOT NULL,
            to_revision INTEGER NOT NULL,
            row_json TEXT NOT NULL,
            UNIQUE (identity_partition, identity_source, identity_batch, identity_position)
        )''')

    @contextmanager
    def transaction(self):
        self.connection.execute('BEGIN IMMEDIATE')
        with self.connection:
            yield

    def find_batch(self, partition: str, source: str, batch: str) -> BatchEvent | None:
        record = self.connection.execute('''SELECT revision, batch_source, batch_no, payload
            FROM fact_event WHERE partition = ? AND batch_source = ? AND batch_no = ?''',
            (partition, source, batch)).fetchone()
        return self._decode_event(record) if record is not None else None

    def append_batch(self, partition, source, batch, revision, rows) -> None:
        readings = [(row.category, row.liters) for row in rows]
        self.connection.execute('INSERT INTO fact_event VALUES (?, ?, ?, ?, ?)',
                                (partition, revision, source, batch, json.dumps(readings)))

    def append_amendment(self, partition, revision, row) -> None:
        payload = {'kind': AMENDMENT_KIND, 'identity': asdict(row.identity),
                   'category': row.category, 'liters': row.liters}
        self.connection.execute('INSERT INTO fact_event VALUES (?, ?, NULL, NULL, ?)',
                                (partition, revision, json.dumps(payload)))

    def append_transfer(self, evidence) -> None:
        self.connection.execute('INSERT INTO fact_transfer VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (
            evidence.transfer_id,
            evidence.identity.partition, evidence.identity.source,
            evidence.identity.batch, evidence.identity.position,
            evidence.from_partition, evidence.to_partition,
            evidence.from_revision, evidence.to_revision,
            json.dumps(asdict(evidence.row), ensure_ascii=False),
        ))
        out_payload = {'kind': TRANSFER_OUT_KIND, 'transfer_id': evidence.transfer_id,
                       'identity': asdict(evidence.identity)}
        self.connection.execute('INSERT INTO fact_event VALUES (?, ?, NULL, NULL, ?)',
                                (evidence.from_partition, evidence.from_revision, json.dumps(out_payload)))
        in_payload = {'kind': TRANSFER_IN_KIND, 'transfer_id': evidence.transfer_id,
                      'row': asdict(evidence.row)}
        self.connection.execute('INSERT INTO fact_event VALUES (?, ?, NULL, NULL, ?)',
                                (evidence.to_partition, evidence.to_revision, json.dumps(in_payload)))

    def head_revision(self, partition: str) -> int:
        return self.connection.execute('SELECT COALESCE(MAX(revision), 0) FROM fact_event WHERE partition = ?',
                                       (partition,)).fetchone()[0]

    def read_revision_events(self, partition: str, revision: int):
        records = self.connection.execute('''SELECT revision, batch_source, batch_no, payload FROM fact_event
            WHERE partition = ? AND revision <= ? ORDER BY revision''', (partition, revision))
        return tuple(self._decode_event(record) for record in records)

    def find_transfer(self, transfer_id: str) -> StoredTransfer | None:
        record = self.connection.execute('''SELECT transfer_id, identity_partition, identity_source,
                identity_batch, identity_position, from_partition, to_partition, from_revision,
                to_revision, row_json FROM fact_transfer WHERE transfer_id = ?''',
            (transfer_id,)).fetchone()
        return self._decode_transfer(record) if record is not None else None

    def find_transfer_by_identity(self, identity: RowIdentity) -> StoredTransfer | None:
        record = self.connection.execute('''SELECT transfer_id, identity_partition, identity_source,
                identity_batch, identity_position, from_partition, to_partition, from_revision,
                to_revision, row_json FROM fact_transfer
            WHERE identity_partition = ? AND identity_source = ? AND identity_batch = ?
              AND identity_position = ?''',
            (identity.partition, identity.source, identity.batch, identity.position)).fetchone()
        return self._decode_transfer(record) if record is not None else None

    def transfers_for_partition(self, partition: str, revision: int):
        records = self.connection.execute('''SELECT transfer_id, identity_partition, identity_source,
                identity_batch, identity_position, from_partition, to_partition, from_revision,
                to_revision, row_json FROM fact_transfer
            WHERE (from_partition = ? AND from_revision <= ?) OR (to_partition = ? AND to_revision <= ?)
            ORDER BY from_revision, to_revision, transfer_id''',
            (partition, revision, partition, revision))
        return tuple(self._decode_transfer(record) for record in records)

    @staticmethod
    def _decode_transfer(record) -> StoredTransfer:
        (transfer_id, identity_partition, identity_source, identity_batch, identity_position,
         from_partition, to_partition, from_revision, to_revision, row_json) = record
        return StoredTransfer(
            transfer_id,
            RowIdentity(identity_partition, identity_source, identity_batch, identity_position),
            from_partition, to_partition, from_revision, to_revision,
            row_from_data(json.loads(row_json)),
        )

    @staticmethod
    def _decode_event(record):
        revision, source, batch, payload = record
        data = json.loads(payload)
        if source is not None:
            return BatchEvent(revision, source, batch, tuple(tuple(reading) for reading in data))
        kind = data.get('kind', AMENDMENT_KIND) if isinstance(data, dict) else AMENDMENT_KIND
        if kind == TRANSFER_OUT_KIND:
            return TransferOutEvent(revision, data['transfer_id'], RowIdentity(**data['identity']))
        if kind == TRANSFER_IN_KIND:
            return TransferInEvent(revision, data['transfer_id'], row_from_data(data['row']))
        return AmendmentEvent(revision, RowIdentity(**data['identity']), data['category'], data['liters'])
