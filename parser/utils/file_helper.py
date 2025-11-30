import json
from pathlib import Path
from typing import Set, Any, Dict
from loguru import logger
class FileManager:
    @staticmethod
    def load_json_as_set(file_path: Path) -> Set[str]:
        if not file_path.exists():
            return set()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return set(data) if isinstance(data, list) else set()
        except Exception as e:
            logger.error(f"Error loading {file_path}: {e}")
            return set()
    @staticmethod
    def save_set_as_json(data: Set[str], file_path: Path) -> bool:
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(list(data), f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Error saving to {file_path}: {e}")
            return False
    @staticmethod
    def load_json(file_path: Path) -> Any:
        if not file_path.exists():
            return None
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading {file_path}: {e}")
            return None
    @staticmethod
    def save_json(data: Any, file_path: Path) -> bool:
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Error saving to {file_path}: {e}")
            return False
    @staticmethod
    def load_fetched_data(file_path: Path) -> Dict[str, Dict]:
        if not file_path.exists():
            return {}
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading {file_path}: {e}")
            return {}
    @staticmethod
    def save_fetched_data(data: Dict[str, Dict], file_path: Path) -> bool:
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            logger.error(f"Error saving to {file_path}: {e}")
            return False
    @staticmethod
    def save_html(content: str, file_path: Path) -> bool:
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return True
        except Exception as e:
            logger.error(f"Error saving HTML to {file_path}: {e}")
            return False
    @staticmethod
    def ensure_directory(path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)