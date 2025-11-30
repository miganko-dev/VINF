from pathlib import Path
from typing import Dict, Any
import json
from loguru import logger
from urllib.parse import urlparse
class FileManager:
    @staticmethod
    def directory(path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
    @staticmethod
    def load_json(path: Path) -> Dict:
        if not path.exists():
            return None
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading {path}: {e}")
            return None
    @staticmethod
    def save_html(content, save_path: Path, url: str) -> str:
        parsed = urlparse(url)
        relative_path = (parsed.path + ('#' + parsed.fragment if parsed.fragment else '')).strip('/')
        if not relative_path or relative_path.endswith('/'):
            relative_path += 'index.html'
        elif not relative_path.endswith('.html'):
            relative_path += '.html'
        full_path = save_path / relative_path
        FileManager.directory(full_path)
        full_path.write_text(content, encoding='utf-8')
        return str(full_path)
    @staticmethod
    def save_json(data: Any, file_path: Path) -> bool:
        try:
            FileManager.directory(path=file_path.parent)
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Error saving to {file_path}: {e}")
            return False