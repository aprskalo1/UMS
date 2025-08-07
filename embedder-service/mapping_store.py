import os
import csv
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Dict


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
                writer.writerow(["faiss_id", "filename", "added_at"])

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
