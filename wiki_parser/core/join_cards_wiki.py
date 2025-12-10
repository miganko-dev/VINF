import re
import json
from typing import Dict, List, Set
from dataclasses import dataclass

from loguru import logger

from wiki_parser.config import (
    CARDS_DIR, SETS_DIR, WIKI_SPARK_OUTPUT_DIR, WIKI_PARSED_DIR,
    JOINED_OUTPUT_DIR, OutputFiles, ensure_directories
)


@dataclass
class JoinStats:
    total_pokemon: int
    total_cards: int
    total_wiki_pages: int
    pokemon_with_wiki: int
    pokemon_without_wiki: int
    match_rate: float


PREFIXES = ['detective', 'dark', 'light', 'shiny', 'shadow', 'team',
            'alolan', 'galarian', 'hisuian', 'paldean',
            'primal', 'mega', 'gigantamax', 'origin forme', 'radiant']

SUFFIXES = ['v', 'vmax', 'vstar', 'ex', 'gx', 'lv.x', 'lvx', 'lv x',
            'prime', 'legend', 'break', 'tag team', 'star',
            'g', 'gl', 'fb', 'e4', 'c', '4', 'sp', 'delta species', 'crystal type']

NORMALIZE_PATTERN = re.compile(r'[^a-z0-9]')
WORD_PATTERN = re.compile(r'[a-z]+')


def normalize(name: str) -> str:
    return NORMALIZE_PATTERN.sub('', name.lower()) if name else ""


def extract_base_pokemon(name: str) -> str:
    if not name:
        return ""
    name_lower = name.lower().strip()
    for prefix in PREFIXES:
        if name_lower.startswith(prefix + ' '):
            name_lower = name_lower[len(prefix):].strip()
            break
    for suffix in SUFFIXES:
        if name_lower.endswith(' ' + suffix):
            name_lower = name_lower[:-len(suffix)].strip()
            break
    return name_lower.title() if name_lower else name


def load_sets() -> Dict[str, Dict]:
    logger.info(f"Loading sets from {SETS_DIR}...")
    sets_info = {}
    if not SETS_DIR.exists():
        return sets_info

    for json_file in SETS_DIR.glob("*.json"):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                set_data = json.load(f)
            set_name = set_data.get('Name', '')
            if set_name:
                sets_info[set_name] = {
                    'release': set_data.get('Release', ''),
                    'series': set_data.get('Series', ''),
                    'total_cards': set_data.get('Total cards', 0),
                    'source': set_data.get('Source', '')
                }
        except Exception as e:
            logger.debug(f"Error loading set {json_file}: {e}")

    logger.info(f"Loaded {len(sets_info)} sets")
    return sets_info


def load_cards(sets_info: Dict[str, Dict] = None) -> Dict[str, List[Dict]]:
    logger.info(f"Loading cards from {CARDS_DIR}...")
    pokemon_cards: Dict[str, List[Dict]] = {}

    for json_file in CARDS_DIR.glob("*.json"):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                card = json.load(f)

            pokemon_name = card.get('Pokemon') or card.get('Name', '')
            if not pokemon_name:
                continue

            pokemon_clean = pokemon_name.strip()
            if pokemon_clean not in pokemon_cards:
                pokemon_cards[pokemon_clean] = []

            set_name = card.get('Set', '')
            card_data = {
                'name': card.get('Name', ''),
                'set': set_name,
                'id': card.get('Id', ''),
                'rarity': card.get('Rarity'),
                'price': card.get('Price', '0'),
                'image': card.get('Image', ''),
                'source': card.get('Source', '')
            }

            if sets_info and set_name in sets_info:
                card_data['set_info'] = sets_info[set_name]

            pokemon_cards[pokemon_clean].append(card_data)
        except Exception as e:
            logger.debug(f"Error loading {json_file}: {e}")

    total_cards = sum(len(cards) for cards in pokemon_cards.values())
    logger.info(f"Loaded {total_cards} cards for {len(pokemon_cards)} unique Pokemon")
    return pokemon_cards


def load_wiki_titles() -> List[str]:
    titles_file = WIKI_SPARK_OUTPUT_DIR / OutputFiles.WIKI_PAGES_JSON
    if not titles_file.exists():
        logger.error(f"Wiki titles not found: {titles_file}")
        return []

    with open(titles_file, 'r', encoding='utf-8') as f:
        titles = json.load(f)
    logger.info(f"Loaded {len(titles)} wiki titles")
    return titles


def load_wiki_info() -> Dict[str, Dict]:
    info_file = WIKI_PARSED_DIR / OutputFiles.WIKI_INFO_LOOKUP_JSON
    if not info_file.exists():
        logger.warning("Wiki info not found, will skip enrichment")
        return {}

    with open(info_file, 'r', encoding='utf-8') as f:
        info = json.load(f)
    logger.info(f"Loaded wiki info for {len(info)} pages")
    return info


def load_wiki_pages_with_text() -> Dict[str, str]:
    text_file = WIKI_SPARK_OUTPUT_DIR / OutputFiles.WIKI_PAGES_WITH_TEXT_JSON
    if not text_file.exists():
        logger.warning("Wiki pages with text not found")
        return {}

    with open(text_file, 'r', encoding='utf-8') as f:
        pages = json.load(f)

    wiki_text = {p['title']: p.get('text', '')[:50000] for p in pages}
    logger.info(f"Loaded text for {len(wiki_text)} wiki pages")
    return wiki_text


def build_text_index(wiki_text: Dict[str, str], pokemon_names: List[str]) -> Dict[str, Set[str]]:
    logger.info("Building text index for Pokemon names...")

    base_to_pokemon: Dict[str, List[str]] = {}
    for name in pokemon_names:
        base = extract_base_pokemon(name).lower()
        if base not in base_to_pokemon:
            base_to_pokemon[base] = []
        base_to_pokemon[base].append(name)

    base_names_set = set(base_to_pokemon.keys())
    pokemon_to_wikis: Dict[str, Set[str]] = {name: set() for name in pokemon_names}

    for title, text in wiki_text.items():
        words_in_text = set(WORD_PATTERN.findall(text.lower()))
        for base in words_in_text & base_names_set:
            for pokemon_name in base_to_pokemon[base]:
                pokemon_to_wikis[pokemon_name].add(title)

    matched = sum(1 for wikis in pokemon_to_wikis.values() if wikis)
    logger.info(f"Text index: {matched} Pokemon found in wiki text")
    return pokemon_to_wikis


def build_title_index(wiki_titles: List[str]) -> Dict[str, Set[str]]:
    word_to_titles: Dict[str, Set[str]] = {}
    for title in wiki_titles:
        for word in set(WORD_PATTERN.findall(title.lower())):
            if word not in word_to_titles:
                word_to_titles[word] = set()
            word_to_titles[word].add(title)
    return word_to_titles


def join_pokemon_wiki(pokemon_cards: Dict[str, List[Dict]], wiki_titles: List[str],
                      wiki_info: Dict[str, Dict], text_index: Dict[str, Set[str]] = None) -> List[Dict]:
    logger.info("Joining Pokemon with wiki pages...")

    wiki_exact: Dict[str, str] = {}
    wiki_normalized: Dict[str, str] = {}

    for title in wiki_titles:
        wiki_exact[title.lower()] = title
        wiki_normalized[normalize(title)] = title

        if title.endswith("(Pokémon)") or title.endswith("(Pokemon)"):
            base_name = title.rsplit("(", 1)[0].strip()
            wiki_exact[base_name.lower()] = title
            wiki_normalized[normalize(base_name)] = title

    title_word_index = build_title_index(wiki_titles)
    results = []
    matched_count = 0

    for pokemon_name, cards in pokemon_cards.items():
        pokemon_lower = pokemon_name.lower()
        pokemon_norm = normalize(pokemon_name)
        base_pokemon = extract_base_pokemon(pokemon_name)
        base_lower = base_pokemon.lower()
        base_norm = normalize(base_pokemon)

        matched_wikis: Set[str] = set()

        if base_lower != pokemon_lower and base_lower in wiki_exact:
            matched_wikis.add(wiki_exact[base_lower])
        if pokemon_lower in wiki_exact:
            matched_wikis.add(wiki_exact[pokemon_lower])
        if base_norm != pokemon_norm and base_norm in wiki_normalized:
            matched_wikis.add(wiki_normalized[base_norm])
        if pokemon_norm in wiki_normalized:
            matched_wikis.add(wiki_normalized[pokemon_norm])

        for norm in [base_norm, pokemon_norm]:
            if norm in title_word_index:
                for title in title_word_index[norm]:
                    if not title.startswith("List of") and len(title) < 50:
                        matched_wikis.add(title)

        if text_index and pokemon_name in text_index:
            matched_wikis.update(text_index[pokemon_name])

        def sort_key(title: str) -> int:
            tl = title.lower()
            if base_lower != pokemon_lower:
                if tl == base_lower:
                    return 0
                if tl in [f"{base_lower} (pokémon)", f"{base_lower} (pokemon)"]:
                    return 1
            if tl == pokemon_lower:
                return 2
            if tl in [f"{pokemon_lower} (pokémon)", f"{pokemon_lower} (pokemon)"]:
                return 3
            if base_lower != pokemon_lower and tl.startswith(base_lower):
                return 4
            if tl.startswith(pokemon_lower):
                return 5
            return 6

        sorted_wikis = sorted(matched_wikis, key=sort_key)
        pokemon_wiki_info = None
        best_wiki_page = None

        for wiki_title in sorted_wikis:
            if wiki_title in wiki_info:
                page_info = wiki_info[wiki_title]
                if page_info.get('has_pokemon_info'):
                    best_wiki_page = wiki_title
                    pokemon_wiki_info = {
                        'types': page_info.get('types'),
                        'species': page_info.get('species'),
                        'generation': page_info.get('generation'),
                        'abilities': page_info.get('abilities'),
                        'evolves_from': page_info.get('evolves_from'),
                        'evolves_to': page_info.get('evolves_to'),
                        'height': page_info.get('height'),
                        'weight': page_info.get('weight'),
                        'japanese_name': page_info.get('japanese_name'),
                        'pokedex_number': page_info.get('pokedex_number'),
                        'first_game': page_info.get('first_game'),
                        'created_by': page_info.get('created_by'),
                        'description': page_info.get('description')
                    }
                    break

        results.append({
            'pokemon': pokemon_name,
            'card_count': len(cards),
            'cards': cards,
            'wiki_pages': sorted_wikis,
            'best_wiki_page': best_wiki_page,
            'wiki_info': pokemon_wiki_info
        })

        if sorted_wikis:
            matched_count += 1

    logger.info(f"Matched {matched_count}/{len(results)} Pokemon with wiki pages")
    return results


def save_results(results: List[Dict], stats: JoinStats) -> None:
    ensure_directories()
    results = sorted(results, key=lambda x: x['pokemon'])

    with open(JOINED_OUTPUT_DIR / OutputFiles.POKEMON_WITH_WIKI_JSON, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    matched = [r for r in results if r['wiki_pages']]
    with open(JOINED_OUTPUT_DIR / OutputFiles.POKEMON_MATCHED_WIKI_JSON, 'w', encoding='utf-8') as f:
        json.dump(matched, f, ensure_ascii=False, indent=2)

    stats_dict = {
        'total_pokemon': stats.total_pokemon,
        'total_cards': stats.total_cards,
        'total_wiki_pages': stats.total_wiki_pages,
        'pokemon_with_wiki': stats.pokemon_with_wiki,
        'pokemon_without_wiki': stats.pokemon_without_wiki,
        'match_rate': f"{stats.match_rate:.1f}%"
    }
    with open(JOINED_OUTPUT_DIR / OutputFiles.JOIN_STATS_JSON, 'w', encoding='utf-8') as f:
        json.dump(stats_dict, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {len(results)} Pokemon, {len(matched)} matched")


def run_join() -> JoinStats:
    logger.info("=" * 60)
    logger.info("POKEMON CARD - WIKI JOIN")
    logger.info("=" * 60)

    sets_info = load_sets()
    pokemon_cards = load_cards(sets_info)
    wiki_titles = load_wiki_titles()
    wiki_info = load_wiki_info()
    wiki_text = load_wiki_pages_with_text()

    if not pokemon_cards:
        raise ValueError("No cards loaded!")
    if not wiki_titles:
        raise ValueError("No wiki titles loaded!")

    text_index = build_text_index(wiki_text, list(pokemon_cards.keys())) if wiki_text else None
    results = join_pokemon_wiki(pokemon_cards, wiki_titles, wiki_info, text_index)

    total_cards = sum(r['card_count'] for r in results)
    matched = sum(1 for r in results if r['wiki_pages'])

    stats = JoinStats(
        total_pokemon=len(results),
        total_cards=total_cards,
        total_wiki_pages=len(wiki_titles),
        pokemon_with_wiki=matched,
        pokemon_without_wiki=len(results) - matched,
        match_rate=matched / len(results) * 100 if results else 0
    )

    save_results(results, stats)

    logger.info("-" * 40)
    logger.info(f"JOIN COMPLETE: {stats.total_pokemon} Pokemon, {stats.pokemon_with_wiki} matched ({stats.match_rate:.1f}%)")
    logger.info("-" * 40)

    return stats


class CardWikiJoiner:
    def run(self):
        return run_join()


class WikiToPokemonJoiner:
    def run(self):
        joined_file = JOINED_OUTPUT_DIR / OutputFiles.POKEMON_WITH_WIKI_JSON
        if not joined_file.exists():
            logger.error("Run join first!")
            return {}

        with open(joined_file, 'r', encoding='utf-8') as f:
            pokemon_data = json.load(f)

        wiki_pokemon: Dict[str, List[str]] = {}
        for p in pokemon_data:
            for wiki in p.get('wiki_pages', []):
                if wiki not in wiki_pokemon:
                    wiki_pokemon[wiki] = []
                wiki_pokemon[wiki].append(p['pokemon'])

        wiki_titles = load_wiki_titles()
        wiki_data = [{'wiki_title': title, 'pokemon_matches': wiki_pokemon.get(title, [])} for title in wiki_titles]

        ensure_directories()

        with open(JOINED_OUTPUT_DIR / OutputFiles.WIKI_WITH_POKEMON_JSON, 'w', encoding='utf-8') as f:
            json.dump(wiki_data, f, ensure_ascii=False, indent=2)

        matched = [w for w in wiki_data if w['pokemon_matches']]
        with open(JOINED_OUTPUT_DIR / OutputFiles.WIKI_MATCHED_POKEMON_JSON, 'w', encoding='utf-8') as f:
            json.dump(matched, f, ensure_ascii=False, indent=2)

        logger.info(f"Wiki pages with Pokemon: {len(matched)}/{len(wiki_data)}")
        return {'total_wiki_pages': len(wiki_data), 'wiki_with_pokemon': len(matched)}


def run_wiki_to_pokemon_join():
    return WikiToPokemonJoiner().run()


if __name__ == "__main__":
    run_join()
