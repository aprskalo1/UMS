import os
import csv
import pyodbc
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Dict, List, Iterable, Optional
from config import build_default_conn_str


class MappingStore(ABC):
    @abstractmethod
    def initialize(self) -> None:
        pass

    @abstractmethod
    def add(self, vector_id: int, filename: str, timestamp: str = None) -> None:
        pass

    @abstractmethod
    def load(self) -> Dict[int, str]:
        pass


class CSVMappingStore(MappingStore):
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        folder = os.path.dirname(csv_path)
        if folder and not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)

    def initialize(self) -> None:
        if not os.path.exists(self.csv_path):
            with open(self.csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["faiss_id", "db_id", "timestamp"])

    def add(self, vector_id: int, filename: str, timestamp: str = None) -> None:
        if timestamp is None:
            timestamp = datetime.now(timezone.utc).isoformat()
        with open(self.csv_path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([vector_id, filename, timestamp])

    def load(self) -> Dict[int, str]:
        mapping = {}
        with open(self.csv_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                mapping[int(row["faiss_id"])] = row["filename"]
        return mapping


class SQLMappingStore(MappingStore):
    def __init__(self, conn_str: Optional[str] = None, table: str = "dbo.VectorMap"):
        self.conn_str = conn_str or build_default_conn_str()
        self.table = table

    def _get_conn(self):
        return pyodbc.connect(self.conn_str, autocommit=False)

    def initialize(self) -> None:
        create_sql = f"""
        IF OBJECT_ID('{self.table}', 'U') IS NULL
        BEGIN
            CREATE TABLE {self.table}(
                faiss_id  BIGINT NOT NULL PRIMARY KEY,
                track_id  UNIQUEIDENTIFIER NOT NULL UNIQUE,
                added_at  DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
            );
            CREATE UNIQUE INDEX IX_{self.table.replace('.', '_')}_TrackId ON {self.table}(track_id);
        END
        """
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute(create_sql)
            conn.commit()

    def add(self, vector_id: int, db_id: str, timestamp: str = None) -> None:
        with self._get_conn() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    f"INSERT INTO {self.table}(faiss_id, track_id) VALUES (?, ?)",
                    int(vector_id), db_id
                )
            except pyodbc.IntegrityError:
                cur.execute(
                    f"UPDATE {self.table} SET track_id = ? WHERE faiss_id = ?",
                    db_id, int(vector_id)
                )
            conn.commit()

    def load(self) -> Dict[int, str]:
        mapping: Dict[int, str] = {}
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT faiss_id, CONVERT(varchar(36), track_id) FROM {self.table}")
            for faiss_id, guid_str in cur.fetchall():
                mapping[int(faiss_id)] = str(guid_str)
        return mapping


class CompositeMappingStore(MappingStore):
    def __init__(self, stores: Iterable[MappingStore]):
        self.stores: List[MappingStore] = list(stores)

    def initialize(self) -> None:
        for s in self.stores:
            s.initialize()

    def add(self, vector_id: int, db_id: str, timestamp: str = None) -> None:
        for s in self.stores:
            s.add(vector_id, db_id, timestamp)

    def load(self) -> Dict[int, str]:
        merged: Dict[int, str] = {}
        for s in self.stores:
            try:
                m = s.load()
                merged.update(m)
            except Exception:
                continue
        return merged
