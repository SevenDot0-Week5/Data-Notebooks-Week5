"""Standalone NOAA JSON loader.

This module is separate from the existing helpers and is designed to:
- Load NOAA JSON files from a folder (including a folder named "NOAA Json Data")
- Build one DataFrame per file
- Build one combined DataFrame across all files
- Work cleanly in scripts, APIs, and Jupyter Lab
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_DATA_DIR_CANDIDATES = [
    Path(__file__).parent / "NOAA Json Data",
    Path(__file__).parent / "data" / "daily_summaries",
]


@dataclass
class LoadedNoaaJsonData:
    source_dir: Path
    file_dataframes: dict[str, pd.DataFrame]
    combined_dataframe: pd.DataFrame


class NoaaJsonLoader:
    """Loads NOAA JSON payload files into pandas DataFrames."""

    def __init__(self, data_dir: str | Path | None = None):
        self.data_dir = self._resolve_data_dir(data_dir)

    def _resolve_data_dir(self, data_dir: str | Path | None) -> Path:
        if data_dir is not None:
            resolved = Path(data_dir)
            if not resolved.exists():
                raise FileNotFoundError(f"Data directory not found: {resolved}")
            return resolved

        for candidate in DEFAULT_DATA_DIR_CANDIDATES:
            if candidate.exists():
                return candidate

        raise FileNotFoundError(
            "No NOAA JSON data directory found. Expected one of: "
            + ", ".join(str(path) for path in DEFAULT_DATA_DIR_CANDIDATES)
        )

    def list_json_files(self) -> list[Path]:
        files = sorted(path for path in self.data_dir.glob("*.json") if path.name != ".gitkeep")
        if not files:
            raise FileNotFoundError(f"No JSON files found in {self.data_dir}")
        return files

    def _payload_to_records(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, dict) and isinstance(payload.get("results"), list):
            return [row for row in payload["results"] if isinstance(row, dict)]
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict) and {"date", "datatype", "value"}.issubset(payload.keys()):
            return [payload]
        return []

    def load_file_dataframe(self, file_path: str | Path) -> pd.DataFrame:
        file_path = Path(file_path)
        with open(file_path, "r") as handle:
            payload = json.load(handle)

        records = self._payload_to_records(payload)
        if not records:
            return pd.DataFrame()

        frame = pd.DataFrame(records)
        frame["__source_file__"] = file_path.name
        return frame

    def load_all(self) -> LoadedNoaaJsonData:
        file_dataframes: dict[str, pd.DataFrame] = {}
        combined_rows: list[pd.DataFrame] = []

        for file_path in self.list_json_files():
            frame = self.load_file_dataframe(file_path)
            file_dataframes[file_path.name] = frame
            if not frame.empty:
                combined_rows.append(frame)

        combined = pd.concat(combined_rows, ignore_index=True) if combined_rows else pd.DataFrame()

        return LoadedNoaaJsonData(
            source_dir=self.data_dir,
            file_dataframes=file_dataframes,
            combined_dataframe=combined,
        )


def load_noaa_json_dataframes(data_dir: str | Path | None = None) -> LoadedNoaaJsonData:
    """Convenience function for scripts and notebooks.

    Example (Jupyter):
        from noaa_json_loader import load_noaa_json_dataframes
        loaded = load_noaa_json_dataframes()
        df = loaded.combined_dataframe
    """

    loader = NoaaJsonLoader(data_dir=data_dir)
    return loader.load_all()
