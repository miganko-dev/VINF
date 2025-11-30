import re
import html
from typing import Optional, Tuple
from utils.set_manager import PokeSet
from utils.card_manager import PokeCard
from config import REGEX
from loguru import logger

class ExtractManager:
    def __init__(self):
        self.regexes = REGEX
    def _search_group(self, pattern: str, html_content: str, group: int = 1) -> Optional[str]:
        match = re.search(pattern, html_content)
        return match.group(group) if match and match.lastindex >= group else None
    def _findall_unescape(self, pattern: str, html_content: str) -> list[str]:
        matches = re.findall(pattern, html_content)
        return [html.unescape(m) for m in matches] if matches else []
    def parse_set(self, html_content: str) -> Optional[PokeSet]:
        name = self._search_group(self.regexes["SET_NAME"], html_content)
        if not name:
            return None
        release = self._search_group(self.regexes["SET_RELEASE"], html_content)
        total_cards = self._search_group(self.regexes["SET_TOTAL_CARDS"], html_content)
        series_symbol_matches = self._findall_unescape(
            self.regexes["SET_SERIES_SYMBOL"], 
            html_content
        )
        series = series_symbol_matches[0] if len(series_symbol_matches) > 0 else None
        symbol = series_symbol_matches[1] if len(series_symbol_matches) > 1 else None
        if not all([name, total_cards]):
            return None
        return PokeSet(
            name=name,
            release=release,
            series=series,
            symbol=symbol,
            total_cards=int(total_cards)
        )
    def parse_card(self, html_content: str) -> Optional[PokeCard]:
        name = self._search_group(self.regexes["CARD_NAME"], html_content)
        image = self._search_group(self.regexes["CARD_IMAGE"], html_content)
        card_id = self._search_group(self.regexes["CARD_ID"], html_content)
        price = self._findall_unescape(self.regexes["CARD_PRICE"], html_content)
        if price:
            price = price[min(len(price) - 1, 2)]
        card_set = self._findall_unescape(self.regexes["CARD_SET"], html_content)[1]
        if not all([name, card_id, card_set]):
            return None
        return PokeCard(
            name=name,
            image=image,
            id=card_id,
            price=price,
            card_set=card_set
        )