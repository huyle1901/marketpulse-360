"""Stable manifest model for generated datasets."""

from __future__ import annotations

from dataclasses import dataclass

from .writers import FileStat


@dataclass(frozen=True, slots=True)
class Manifest:
    generator_version: str
    synthetic: bool
    profile: str
    seed: int
    start_date: str
    end_date: str
    configuration: dict[str, object]
    schema_versions: dict[str, int]
    datasets: dict[str, int]
    anomalies: dict[str, int]
    totals: dict[str, int]
    files: tuple[FileStat, ...]

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-ready representation with stable field order."""

        return {
            "generator_version": self.generator_version,
            "synthetic": self.synthetic,
            "profile": self.profile,
            "seed": self.seed,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "configuration": self.configuration,
            "schema_versions": self.schema_versions,
            "datasets": self.datasets,
            "anomalies": self.anomalies,
            "totals": self.totals,
            "files": [
                {
                    "dataset": stat.dataset,
                    "relative_path": stat.relative_path,
                    "format": stat.format,
                    "row_count": stat.row_count,
                    "byte_size": stat.byte_size,
                    "sha256": stat.sha256,
                    "min_event_time": stat.min_event_time,
                    "max_event_time": stat.max_event_time,
                }
                for stat in self.files
            ],
        }

