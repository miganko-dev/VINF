import html
import re
from typing import Optional, Tuple
from decimal import Decimal
class PokeCard:
    SUFFIX_RARITIES = [
        'Holiday Calendar',
        'Reverse Holo',
        'Delta Species',
        'Secret Rare',
        'Rainbow Rare',
        'Gold Secret',
        'Ultra Rare',
        'Hyper Rare',
        'Holo',
        'VMAX',
        'VSTAR',
        'MEGA',
        'BREAK',
        'Prime',
        'LEGEND',
        'Shining',
        'Radiant',
        'Lv.X',
        'Gold Star',
        'Prism Star',
        'Star',
        'GX',
        'EX',
        'ex',
        'V',
        'Secret',
        'Gamestop'
        'V-Union',
        'Prelease'

    ]
    PREFIX_RARITIES = [
        'Full Art',
        'Secret',
        'Rainbow',
        'Future',

    ]
    SET_SUFFIXES = [
        '1st Edition',
        'Cracked Ice',
        'Shadowless',
        'Prerelease',
        'Unlimited',
        'Holofoil',
        'Cosmos',
        'Stamped',
        'Staff',
    ]
    FORM_PREFIXES = [
        "Team Magma's",
        "Team Aqua's",
        'Origin Forme',
        'Altered Forme',
        'Therian Forme',
        'Incarnate Forme',
        'Single Strike',
        'Rapid Strike',
        'Sky Forme',
        'Land Forme',
        "Rocket's",
        'Gigantamax',
        'Galarian',
        'Hisuian',
        'Paldean',
        'Alolan',
        'Shadow',
        'Primal',
        'Light',
        'Dark',
        'Mega',
        'Basic',
    ]
    def __init__(
        self,
        name: str,
        image: Optional[str],
        id: str,
        price: Optional[str],
        card_set: str
    ):
        self.full_name = name
        self.prefix, self.pokemon_name, self.rarity = self._parse_name(name)
        self.image = image
        self.id = id
        self.price: Decimal = price.replace("$", "") if price else 0
        self.card_set = html.unescape(card_set) if card_set else card_set
    def _remove_pattern(self, text: str, pattern: str, flags: int = re.IGNORECASE) -> str:
        return re.sub(pattern, '', text, flags=flags).strip()
    def _find_and_remove_rarity(self, name: str) -> Tuple[str, Optional[str]]:
        for rarity in self.PREFIX_RARITIES:
            pattern = rf'^{re.escape(rarity)}\s+'
            if re.search(pattern, name, re.IGNORECASE):
                cleaned = self._remove_pattern(name, pattern)
                return cleaned, rarity
        for rarity in self.SUFFIX_RARITIES:
            pattern = rf'\s+{re.escape(rarity)}$'
            if re.search(pattern, name, re.IGNORECASE):
                cleaned = self._remove_pattern(name, pattern)
                return cleaned, rarity
        return name, None
    def _remove_set_suffixes(self, name: str) -> str:
        for suffix in self.SET_SUFFIXES:
            pattern = rf'\s+{re.escape(suffix)}$'
            name = self._remove_pattern(name, pattern)
        return name
    def _extract_form_prefix(self, name: str) -> Tuple[Optional[str], str]:
        for prefix in self.FORM_PREFIXES:
            pattern = rf'^{re.escape(prefix)}\s+'
            if re.search(pattern, name, re.IGNORECASE):
                cleaned = self._remove_pattern(name, pattern)
                return prefix, cleaned
        return None, name
    def _parse_name(self, name: str) -> Tuple[Optional[str], str, Optional[str]]:
        if not name:
            return None, name, None
        name = html.unescape(name).strip()
        name, rarity = self._find_and_remove_rarity(name)
        name = self._remove_set_suffixes(name)
        prefix, pokemon_name = self._extract_form_prefix(name)
        return prefix, pokemon_name, rarity
    def to_dict(self) -> dict:
        return {
            "Name": self.full_name,
            "Prefix": self.prefix,
            "Pokemon": self.pokemon_name,
            "Rarity": self.rarity,
            "Id": self.id,
            "Price": self.price,
            "Set": self.card_set,
            "Image": self.image
        }
    def __repr__(self) -> str:
        return f"PokeCard({self.pokemon_name}, Set={self.card_set}, ID={self.id})"
    def __str__(self) -> str:
        parts = []
        if self.prefix:
            parts.append(self.prefix)
        parts.append(self.pokemon_name)
        if self.rarity:
            parts.append(self.rarity)
        return " ".join(parts)