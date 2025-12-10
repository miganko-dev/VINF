import re
import json
from typing import Dict, List, Optional
from loguru import logger

from wiki_parser.config import (
    WIKI_SPARK_OUTPUT_DIR, WIKI_PARSED_DIR, OutputFiles, ensure_directories
)

WIKI_PAGES_WITH_TEXT_FILE = WIKI_SPARK_OUTPUT_DIR / OutputFiles.WIKI_PAGES_WITH_TEXT_JSON
OUTPUT_DIR = WIKI_PARSED_DIR

POKEMON_TYPES = {
    'normal', 'fire', 'water', 'electric', 'grass', 'ice', 'fighting',
    'poison', 'ground', 'flying', 'psychic', 'bug', 'rock', 'ghost',
    'dragon', 'dark', 'steel', 'fairy'
}

HTML_ENTITIES = {
    '&lt;': '<', '&gt;': '>', '&amp;': '&', '&quot;': '"',
    '&apos;': "'", '&nbsp;': ' ', '&#39;': "'", '&#34;': '"',
    '&ndash;': '-', '&mdash;': '-', '&hellip;': '...',
    '&eacute;': 'é', '&Eacute;': 'É'
}

REF_PATTERNS = [
    (re.compile(r'<ref[^>]*>.*?</ref>', re.DOTALL | re.IGNORECASE), ''),
    (re.compile(r'<ref[^>]*/>', re.IGNORECASE), ''),
    (re.compile(r'<ref\s+name\s*=\s*["\'][^"\']*["\']\s*/>', re.IGNORECASE), ''),
    (re.compile(r'<ref\s+name\s*=\s*[^/>\s]+\s*/>', re.IGNORECASE), ''),
    (re.compile(r'<[^>]+/?>'), ''),
    (re.compile(r'\[\[(?:[^\]|]*\|)?([^\]]+)\]\]'), r'\1'),
    (re.compile(r"'{2,5}"), ''),
    (re.compile(r'\[\[|\]\]'), ''),
    (re.compile(r'\{\{|\}\}'), ''),
    (re.compile(r'\[|\]'), ''),
]

TEMPLATE_PATTERN = re.compile(r'\{\{[^{}]*\}\}')
TYPE_PATTERN = re.compile(r'(\w+)-type\s+Pok[eé]mon', re.IGNORECASE)
SPECIES_PATTERN = re.compile(r'the\s+(\w+(?:\s+\w+)?)\s+Pok[eé]mon', re.IGNORECASE)
GENERATION_PATTERN = re.compile(r'(?:introduced\s+in\s+)?Generation\s+([IVX]+|\d+)', re.IGNORECASE)
EVOLUTION_FROM_PATTERN = re.compile(r'evolves?\s+from\s+\[\[([^\]]+)\]\]', re.IGNORECASE)
EVOLUTION_TO_PATTERN = re.compile(r'evolves?\s+into\s+\[\[([^\]]+)\]\]', re.IGNORECASE)
JAPANESE_PATTERN = re.compile(r'\(Japanese:\s*([^)]+)\)')
POKEDEX_PATTERN = re.compile(r'#(\d{3,4})')
INFOBOX_PATTERN = re.compile(r'\{\{[Ii]nfobox[^}]+\}\}', re.DOTALL)
SHORT_DESC_PATTERN = re.compile(r'\{\{[Ss]hort description[^}]+\}\}')

FIRST_GAME_PATTERNS = [
    re.compile(r'first\s+appear(?:ed|s)?\s+in\s+(?:the\s+)?(?:video\s+games?\s+)?["\']?([^"\',.]+(?:Red|Blue|Green|Yellow|Gold|Silver|Crystal|Ruby|Sapphire|Diamond|Pearl|Black|White|Sun|Moon|Sword|Shield|Scarlet|Violet)[^"\',.]*)', re.IGNORECASE),
    re.compile(r'introduced\s+in\s+(?:the\s+)?(?:video\s+games?\s+)?["\']?Pok[eé]mon\s+([^"\',.]+)', re.IGNORECASE),
    re.compile(r'debut(?:ed)?\s+in\s+["\']?Pok[eé]mon\s+([^"\',.]+)', re.IGNORECASE),
    re.compile(r'Pok[eé]mon\s+(Red\s+and\s+Blue|Gold\s+and\s+Silver|Ruby\s+and\s+Sapphire|Diamond\s+and\s+Pearl|Black\s+and\s+White|Sun\s+and\s+Moon|Sword\s+and\s+Shield|Scarlet\s+and\s+Violet)', re.IGNORECASE),
]

CREATOR_PATTERNS = [
    re.compile(r'(?:created|designed|developed)\s+by\s+\[\[([^\]|]+)', re.IGNORECASE),
    re.compile(r'(?:created|designed|developed)\s+by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)', re.IGNORECASE),
    re.compile(r'creator[s]?\s+(?:is|are|was|were)\s+\[\[([^\]|]+)', re.IGNORECASE),
    re.compile(r'designed\s+by\s+([^,.\n]+)', re.IGNORECASE),
]

DESIGN_PATTERNS = [
    re.compile(r'(Standing\s+[\d.]+\s+(?:metres?|meters?|m)\s*\([^)]+\)[^.]*(?:tall)?[^.]*\.[^.]*\.)', re.IGNORECASE),
    re.compile(r'((?:It\s+)?(?:is\s+)?(?:a\s+)?(?:bipedal|quadrupedal)?\s*(?:yellow|red|blue|green|orange|purple|pink|brown|black|white|gray|grey)?\s*(?:rodent|mouse|cat|dog|bird|dragon|lizard|turtle|snake|fish|frog)[^.]*(?:with|has|having)[^.]*\.)', re.IGNORECASE),
    re.compile(r'(\d+(?:\.\d+)?\s*(?:m|cm|ft|in|metres?|meters?|feet|inches?)[^.]*(?:tall|height)[^.]*(?:with|has|having)[^.]*\.)', re.IGNORECASE),
    re.compile(r'((?:It\s+)?has\s+(?:yellow|red|blue|green|orange|purple|pink|brown|black|white|gray|grey)\s+(?:fur|skin|scales|feathers|body)[^.]*\.)', re.IGNORECASE),
    re.compile(r'((?:Its\s+)?design\s+(?:is\s+)?(?:based\s+on|inspired\s+by|resembles)[^.]*\.)', re.IGNORECASE),
]

APPEARANCE_KEYWORDS = ['tall', 'bipedal', 'quadrupedal', 'yellow fur', 'red cheek',
                       'pointed ears', 'lightning bolt', 'tail shaped', 'long ears',
                       'short arms', 'rodent', 'mouse-like']


def clean_wiki_markup(text: str) -> str:
    if not text:
        return ""
    for entity, char in HTML_ENTITIES.items():
        text = text.replace(entity, char)
    for pattern, repl in REF_PATTERNS:
        text = pattern.sub(repl, text)
    for _ in range(5):
        if '{{' not in text:
            break
        text = TEMPLATE_PATTERN.sub('', text)
    return ' '.join(text.split()).strip()


def extract_infobox_field(text: str, field_name: str) -> Optional[str]:
    patterns = [
        re.compile(rf'\|\s*{field_name}\s*=\s*([^\|\n\}}]+)', re.IGNORECASE),
        re.compile(rf'\|\s*{field_name}\s*=\s*\[\[([^\]]+)\]\]', re.IGNORECASE),
    ]
    for pattern in patterns:
        match = pattern.search(text)
        if match:
            value = clean_wiki_markup(match.group(1).strip())
            if value:
                return value
    return None


def extract_pokemon_type(text: str) -> List[str]:
    types = []
    for field in ['type', 'type1', 'type2']:
        type_val = extract_infobox_field(text, field)
        if type_val and type_val.lower() in POKEMON_TYPES:
            types.append(type_val.capitalize())
    if not types:
        for match in TYPE_PATTERN.findall(text):
            if match.lower() in POKEMON_TYPES:
                types.append(match.capitalize())
    return list(dict.fromkeys(types))


def extract_species(text: str) -> Optional[str]:
    species = extract_infobox_field(text, 'species')
    if species:
        return species
    match = SPECIES_PATTERN.search(text)
    return match.group(1) + " Pokémon" if match else None


def extract_generation(text: str) -> Optional[str]:
    gen = extract_infobox_field(text, 'generation')
    if gen:
        return gen
    match = GENERATION_PATTERN.search(text)
    return match.group(1) if match else None


def extract_abilities(text: str) -> List[str]:
    abilities = []
    for field in ['ability', 'ability1', 'ability2', 'abilityd', 'hidden_ability']:
        ability = extract_infobox_field(text, field)
        if ability and ability.lower() not in ['none', 'n/a', '']:
            abilities.append(ability)
    return list(dict.fromkeys(abilities))


def extract_evolution(text: str) -> Dict[str, Optional[str]]:
    evolution = {
        'evolves_from': extract_infobox_field(text, 'evolvesfrom'),
        'evolves_to': extract_infobox_field(text, 'evolvesto')
    }
    if not evolution['evolves_from']:
        match = EVOLUTION_FROM_PATTERN.search(text)
        if match:
            evolution['evolves_from'] = match.group(1).split('|')[-1]
    if not evolution['evolves_to']:
        match = EVOLUTION_TO_PATTERN.search(text)
        if match:
            evolution['evolves_to'] = match.group(1).split('|')[-1]
    return evolution


def extract_physical_stats(text: str) -> Dict[str, Optional[str]]:
    return {
        'height': extract_infobox_field(text, 'height') or extract_infobox_field(text, 'metricheight'),
        'weight': extract_infobox_field(text, 'weight') or extract_infobox_field(text, 'metricweight')
    }


def extract_japanese_name(text: str) -> Optional[str]:
    jname = extract_infobox_field(text, 'jname')
    if jname:
        return jname
    match = JAPANESE_PATTERN.search(text)
    return match.group(1).strip() if match else None


def extract_pokedex_number(text: str) -> Optional[str]:
    for field in ['ndex', 'number']:
        ndex = extract_infobox_field(text, field)
        if ndex:
            return ndex
    match = POKEDEX_PATTERN.search(text)
    return match.group(1) if match else None


def extract_first_game(text: str) -> Optional[str]:
    for field in ['first_game', 'firstgame', 'first game', 'debut']:
        game = extract_infobox_field(text, field)
        if game:
            return game
    for pattern in FIRST_GAME_PATTERNS:
        match = pattern.search(text)
        if match:
            result = clean_wiki_markup(match.group(1))
            if result and len(result) < 100:
                return result
    return None


def extract_created_by(text: str) -> Optional[str]:
    for field in ['creator', 'created_by', 'designer', 'designed_by', 'artist']:
        creator = extract_infobox_field(text, field)
        if creator:
            return creator
    for pattern in CREATOR_PATTERNS:
        match = pattern.search(text)
        if match:
            result = clean_wiki_markup(match.group(1))
            if result and len(result) < 100:
                return result
    return None


def extract_design_description(text: str) -> Optional[str]:
    for pattern in DESIGN_PATTERNS:
        match = pattern.search(text)
        if match:
            result = clean_wiki_markup(match.group(1))
            if 20 < len(result) < 500:
                return result
    for sentence in re.split(r'(?<=[.!?])\s+', text):
        sentence_lower = sentence.lower()
        if any(kw in sentence_lower for kw in APPEARANCE_KEYWORDS):
            result = clean_wiki_markup(sentence)
            if 20 < len(result) < 500:
                return result
    return None


def extract_description(text: str) -> Optional[str]:
    text = INFOBOX_PATTERN.sub('', text)
    text = SHORT_DESC_PATTERN.sub('', text)
    for para in text.split('\n\n'):
        para = clean_wiki_markup(para)
        if len(para) > 50 and not para.startswith('{{') and not para.startswith('|'):
            return para[:500] + ('...' if len(para) > 500 else '')
    return None


def extract_all_info(title: str, text: str) -> Dict:
    if not text:
        return {'title': title, 'has_pokemon_info': False}

    types = extract_pokemon_type(text)
    species = extract_species(text)
    generation = extract_generation(text)
    abilities = extract_abilities(text)
    evolution = extract_evolution(text)
    physical = extract_physical_stats(text)
    japanese_name = extract_japanese_name(text)
    pokedex_number = extract_pokedex_number(text)
    first_game = extract_first_game(text)
    created_by = extract_created_by(text)
    design_description = extract_design_description(text)
    description = extract_description(text)

    has_pokemon_info = bool(types or species or generation or abilities or pokedex_number or first_game or design_description)

    return {
        'title': title,
        'has_pokemon_info': has_pokemon_info,
        'types': types or None,
        'species': species,
        'generation': generation,
        'abilities': abilities or None,
        'evolves_from': evolution['evolves_from'],
        'evolves_to': evolution['evolves_to'],
        'height': physical['height'],
        'weight': physical['weight'],
        'japanese_name': japanese_name,
        'pokedex_number': pokedex_number,
        'first_game': first_game,
        'created_by': created_by,
        'design_description': design_description,
        'description': description
    }


def run_extraction():
    if not WIKI_PAGES_WITH_TEXT_FILE.exists():
        logger.error(f"Wiki pages file not found: {WIKI_PAGES_WITH_TEXT_FILE}")
        return

    logger.info(f"Loading wiki pages from {WIKI_PAGES_WITH_TEXT_FILE}")
    with open(WIKI_PAGES_WITH_TEXT_FILE, 'r', encoding='utf-8') as f:
        pages = json.load(f)

    logger.info(f"Processing {len(pages)} wiki pages...")

    results = []
    pages_with_info = 0

    for page in pages:
        info = extract_all_info(page.get('title', ''), page.get('text', ''))
        results.append(info)
        if info['has_pokemon_info']:
            pages_with_info += 1

    logger.info(f"Extracted Pokemon info from {pages_with_info} pages")

    ensure_directories()

    with open(OUTPUT_DIR / OutputFiles.WIKI_PAGES_INFO_JSON, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    pokemon_pages = [r for r in results if r['has_pokemon_info']]
    with open(OUTPUT_DIR / OutputFiles.POKEMON_WIKI_INFO_JSON, 'w', encoding='utf-8') as f:
        json.dump(pokemon_pages, f, ensure_ascii=False, indent=2)

    with open(OUTPUT_DIR / OutputFiles.WIKI_INFO_LOOKUP_JSON, 'w', encoding='utf-8') as f:
        json.dump({r['title']: r for r in results}, f, ensure_ascii=False, indent=2)

    stats = {
        'total_pages': len(results),
        'pages_with_pokemon_info': len(pokemon_pages),
        'pages_with_types': sum(1 for r in results if r.get('types')),
        'pages_with_abilities': sum(1 for r in results if r.get('abilities')),
        'pages_with_evolution': sum(1 for r in results if r.get('evolves_from') or r.get('evolves_to')),
        'pages_with_first_game': sum(1 for r in results if r.get('first_game')),
        'pages_with_created_by': sum(1 for r in results if r.get('created_by')),
        'pages_with_design_description': sum(1 for r in results if r.get('design_description')),
        'pages_with_description': sum(1 for r in results if r.get('description'))
    }
    with open(OUTPUT_DIR / OutputFiles.EXTRACTION_STATS_JSON, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    logger.info(f"Stats: {stats}")


if __name__ == "__main__":
    run_extraction()
