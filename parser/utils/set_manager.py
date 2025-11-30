import html
from typing import Optional
class PokeSet:
    def __init__(
        self, 
        name: str, 
        release: Optional[str], 
        series: Optional[str], 
        symbol: Optional[str], 
        total_cards: Optional[int]
    ):
        self.name = html.unescape(name) if name else name
        self.release = release
        self.series = series
        self.symbol = symbol
        self.total_cards = total_cards
    def to_dict(self) -> dict:
        return {
            "Name": self.name,
            "Release": self.release,
            "Series": self.series,
            "Symbol": self.symbol,
            "Total cards": self.total_cards
        }