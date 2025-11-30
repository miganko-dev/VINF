from pathlib import Path
from typing import Optional


class FileManager:
    @staticmethod
    def ensure_directory(path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def get_json_files(directory: Path) -> list[Path]:
        if not directory.exists():
            return []
        return list(directory.glob("**/*.json"))
