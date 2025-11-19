from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List


@dataclass
class RentalRecord:
    """Shallow representation of a DVDRental row."""

    customer_id: int
    film_id: int
    rental_duration: int

    @classmethod
    def from_row(cls, row: dict[str, str]) -> "RentalRecord":
        return cls(
            customer_id=int(row.get("customer_id", 0)),
            film_id=int(row.get("film_id", 0)),
            rental_duration=int(row.get("rental_duration", 0)),
        )


class RentalEnricher:
    """Ingests raw rental rows and calculates per-customer stats."""

    def __init__(self, rentals: Iterable[RentalRecord]) -> None:
        self._rentals = list(rentals)

    def summarize(self) -> List[dict[str, int]]:
        totals: dict[int, dict[str, int]] = {}
        for rec in self._rentals:
            stats = totals.setdefault(
                rec.customer_id,
                {"customer_id": rec.customer_id, "total_duration": 0, "rentals": 0},
            )
            stats["total_duration"] += rec.rental_duration
            stats["rentals"] += 1
        return sorted(totals.values(), key=lambda row: row["customer_id"])

    @staticmethod
    def load_csv(path: Path) -> List[RentalRecord]:
        if not path.is_file():
            return []
        with path.open(newline="") as handle:
            return [RentalRecord.from_row(row) for row in csv.DictReader(handle)]

    @staticmethod
    def write_csv(rows: Iterable[dict[str, int]], path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = list(rows)
        if not rows:
            return
        with path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
