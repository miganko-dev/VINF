from utils.file_helper import FileManager
from config import PARSER_LOG_DIR, PARSER_LOG_FILE, LOG_ROTATION
from utils.extract_manager import ExtractManager
from pathlib import Path
import json
from loguru import logger
class Parser:
    def _setup_logger(self) -> None:
        FileManager.ensure_directory(PARSER_LOG_DIR)
        logger.add(PARSER_LOG_FILE, rotation=LOG_ROTATION)
    def start(self, root_url: str):
        domain = root_url
        self._setup_logger()
        manager = ExtractManager()
        metadata_path = Path("data", domain, "metadata") / "links.json"
        if not metadata_path.exists():
            raise FileNotFoundError(f"Metadata file not found: {metadata_path}")
        with open(metadata_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)
        for url, info in metadata.items():
            if not info["visited"]:
                continue
            html_path = Path(info["path"])
            if not html_path.exists():
                logger.warning(f"HTML file not found for {url}: {html_path}")
                continue
            html = html_path.read_text(encoding="utf-8", errors="ignore")
            parsed_path = Path("data", domain, "parsed")
            FileManager.ensure_directory(parsed_path)
            parsed_path_sets = parsed_path / "sets"
            FileManager.ensure_directory(parsed_path_sets)
            parsed_path_cards = parsed_path / "cards"
            FileManager.ensure_directory(parsed_path_cards)

            if "\\set\\" in info["path"]:
                parsed_set = manager.parse_set(html)
                if parsed_set:
                    parsed_set_dict = parsed_set.to_dict()
                    parsed_set_dict["Source"] = url
                    set_name = parsed_set_dict.get("Name", "unknown")
                    set_file_path = parsed_path_sets / f"{set_name}.json"
                    with open(set_file_path, "w", encoding="utf-8") as f:
                        json.dump(parsed_set_dict, f, indent=2, ensure_ascii=False)
                else:
                    logger.warning(f"Failed to parse set from {url}")

            if "\\card\\" in info["path"]:
                parsed_card = manager.parse_card(html)
                if parsed_card:
                    parsed_card_dict = parsed_card.to_dict()
                    parsed_card_dict["Source"] = url
                    card_name = parsed_card_dict.get("Name", "unknown")
                    card_id = parsed_card_dict.get("Id", "0")
                    card_set = parsed_card_dict.get("Set", "unknown")
                    safe_filename = f"{card_name}_{card_id}_{card_set}".replace("?", "")
                    card_file_path = parsed_path_cards / f"{safe_filename}.json"
                    with open(card_file_path, "w", encoding="utf-8") as f:
                        json.dump(parsed_card_dict, f, indent=2, ensure_ascii=False)
                else:
                    logger.warning(f"Failed to parse card from {url}")