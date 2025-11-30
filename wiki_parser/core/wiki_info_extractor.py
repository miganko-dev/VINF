"""
Wiki Info Extractor

Extracts Pokemon-related information from Wikipedia markup including:
- Pokemon type (Electric, Fire, etc.)
- Species name (Mouse Pokemon, etc.)
- Generation
- Abilities
- Evolution chain
- Height/Weight
- Japanese name
- First game appearance
- Created by (designer)
- Design description (physical appearance)
- Description (first paragraph)
"""

import re
import json
from pathlib import Path
from typing import Dict, List, Optional
from loguru import logger

from wiki_parser.config import (
    WIKI_SPARK_OUTPUT_DIR, WIKI_PARSED_DIR, OutputFiles,
    POKEMON_CONFIG, ensure_directories
)

# Use centralized config paths
WIKI_PAGES_WITH_TEXT_FILE = WIKI_SPARK_OUTPUT_DIR / OutputFiles.WIKI_PAGES_WITH_TEXT_JSON
OUTPUT_DIR = WIKI_PARSED_DIR


class WikiInfoExtractor:
    """Extracts structured Pokemon information from Wikipedia markup."""

    # Pokemon types for validation
    POKEMON_TYPES = {
        'normal', 'fire', 'water', 'electric', 'grass', 'ice', 'fighting',
        'poison', 'ground', 'flying', 'psychic', 'bug', 'rock', 'ghost',
        'dragon', 'dark', 'steel', 'fairy'
    }

    def __init__(self):
        self.extracted_data = {}

    def extract_infobox_field(self, text: str, field_name: str) -> Optional[str]:
        """Extract a field value from Wikipedia infobox markup."""
        # Pattern for | field_name = value
        patterns = [
            rf'\|\s*{field_name}\s*=\s*([^\|\n\}}]+)',
            rf'\|\s*{field_name}\s*=\s*\[\[([^\]]+)\]\]',
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                value = match.group(1).strip()
                # Clean up wiki markup
                value = self._clean_wiki_markup(value)
                if value:
                    return value
        return None

    def _clean_wiki_markup(self, text: str) -> str:
        """Remove common Wikipedia markup from text."""
        if not text:
            return ""

        # Decode HTML entities first
        html_entities = {
            '&lt;': '<', '&gt;': '>', '&amp;': '&', '&quot;': '"',
            '&apos;': "'", '&nbsp;': ' ', '&#39;': "'", '&#34;': '"',
            '&ndash;': '-', '&mdash;': '-', '&hellip;': '...',
            '&eacute;': 'é', '&Eacute;': 'É'
        }
        for entity, char in html_entities.items():
            text = text.replace(entity, char)

        # Remove ref tags and their content (multiple formats)
        text = re.sub(r'<ref[^>]*>.*?</ref>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<ref[^>]*/>', '', text, flags=re.IGNORECASE)
        text = re.sub(r'<ref\s+name\s*=\s*["\'][^"\']*["\']\s*/>', '', text, flags=re.IGNORECASE)
        text = re.sub(r'<ref\s+name\s*=\s*[^/>\s]+\s*/>', '', text, flags=re.IGNORECASE)

        # Remove all remaining HTML-like tags
        text = re.sub(r'<[^>]+/?>', '', text)

        # Remove [[ ]] links but keep display text
        text = re.sub(r'\[\[(?:[^\]|]*\|)?([^\]]+)\]\]', r'\1', text)

        # Remove {{ }} templates (limit iterations to avoid infinite loops)
        for _ in range(5):
            if '{{' not in text:
                break
            text = re.sub(r'\{\{[^{}]*\}\}', '', text)

        # Remove wiki bold/italic markup
        text = re.sub(r"'{2,5}", '', text)

        # Remove remaining wiki formatting
        text = re.sub(r'\[\[|\]\]', '', text)
        text = re.sub(r'\{\{|\}\}', '', text)

        # Clean up extra brackets that might remain
        text = re.sub(r'\[|\]', '', text)

        # Clean up whitespace
        text = ' '.join(text.split())

        return text.strip()

    def extract_pokemon_type(self, text: str) -> List[str]:
        """Extract Pokemon type(s) from wiki text."""
        types = []

        # Try infobox fields first
        for field in ['type', 'type1', 'type2']:
            type_val = self.extract_infobox_field(text, field)
            if type_val:
                type_lower = type_val.lower()
                if type_lower in self.POKEMON_TYPES:
                    types.append(type_val.capitalize())

        # Also search in text for "X-type Pokémon"
        if not types:
            pattern = r'(\w+)-type\s+Pok[eé]mon'
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if match.lower() in self.POKEMON_TYPES:
                    types.append(match.capitalize())

        return list(dict.fromkeys(types))  # Remove duplicates while preserving order

    def extract_species(self, text: str) -> Optional[str]:
        """Extract species name (e.g., 'Mouse Pokémon')."""
        species = self.extract_infobox_field(text, 'species')
        if species:
            return species

        # Search for "X Pokémon" pattern in text
        pattern = r'the\s+(\w+(?:\s+\w+)?)\s+Pok[eé]mon'
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1) + " Pokémon"
        return None

    def extract_generation(self, text: str) -> Optional[str]:
        """Extract generation number."""
        gen = self.extract_infobox_field(text, 'generation')
        if gen:
            return gen

        # Search for "Generation X" or "introduced in Generation X"
        pattern = r'(?:introduced\s+in\s+)?Generation\s+([IVX]+|\d+)'
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)
        return None

    def extract_abilities(self, text: str) -> List[str]:
        """Extract Pokemon abilities."""
        abilities = []

        for field in ['ability', 'ability1', 'ability2', 'abilityd', 'hidden_ability']:
            ability = self.extract_infobox_field(text, field)
            if ability and ability.lower() not in ['none', 'n/a', '']:
                abilities.append(ability)

        return list(dict.fromkeys(abilities))

    def extract_evolution(self, text: str) -> Dict[str, Optional[str]]:
        """Extract evolution information."""
        evolution = {
            'evolves_from': self.extract_infobox_field(text, 'evolvesfrom'),
            'evolves_to': self.extract_infobox_field(text, 'evolvesto')
        }

        # Also try to find evolution info in text
        if not evolution['evolves_from']:
            pattern = r'evolves?\s+from\s+\[\[([^\]]+)\]\]'
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                evolution['evolves_from'] = match.group(1).split('|')[-1]

        if not evolution['evolves_to']:
            pattern = r'evolves?\s+into\s+\[\[([^\]]+)\]\]'
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                evolution['evolves_to'] = match.group(1).split('|')[-1]

        return evolution

    def extract_physical_stats(self, text: str) -> Dict[str, Optional[str]]:
        """Extract height and weight."""
        return {
            'height': self.extract_infobox_field(text, 'height') or
                     self.extract_infobox_field(text, 'metricheight'),
            'weight': self.extract_infobox_field(text, 'weight') or
                     self.extract_infobox_field(text, 'metricweight')
        }

    def extract_japanese_name(self, text: str) -> Optional[str]:
        """Extract Japanese name."""
        jname = self.extract_infobox_field(text, 'jname')
        if jname:
            return jname

        # Look for Japanese text in parentheses
        pattern = r'\(Japanese:\s*([^)]+)\)'
        match = re.search(pattern, text)
        if match:
            return match.group(1).strip()
        return None

    def extract_pokedex_number(self, text: str) -> Optional[str]:
        """Extract National Pokédex number."""
        ndex = self.extract_infobox_field(text, 'ndex')
        if ndex:
            return ndex

        ndex = self.extract_infobox_field(text, 'number')
        if ndex:
            return ndex

        # Look for "#XXX" pattern
        pattern = r'#(\d{3,4})'
        match = re.search(pattern, text)
        if match:
            return match.group(1)
        return None

    def extract_first_game(self, text: str) -> Optional[str]:
        """Extract first game appearance."""
        # Try infobox fields
        for field in ['first_game', 'firstgame', 'first game', 'debut']:
            game = self.extract_infobox_field(text, field)
            if game:
                return game

        # Search for "first appeared in" or "introduced in" patterns
        patterns = [
            r'first\s+appear(?:ed|s)?\s+in\s+(?:the\s+)?(?:video\s+games?\s+)?["\']?([^"\',.]+(?:Red|Blue|Green|Yellow|Gold|Silver|Crystal|Ruby|Sapphire|Diamond|Pearl|Black|White|Sun|Moon|Sword|Shield|Scarlet|Violet)[^"\',.]*)',
            r'introduced\s+in\s+(?:the\s+)?(?:video\s+games?\s+)?["\']?Pok[eé]mon\s+([^"\',.]+)',
            r'debut(?:ed)?\s+in\s+["\']?Pok[eé]mon\s+([^"\',.]+)',
            r'Pok[eé]mon\s+(Red\s+and\s+Blue|Gold\s+and\s+Silver|Ruby\s+and\s+Sapphire|Diamond\s+and\s+Pearl|Black\s+and\s+White|Sun\s+and\s+Moon|Sword\s+and\s+Shield|Scarlet\s+and\s+Violet)',
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                result = self._clean_wiki_markup(match.group(1))
                if result and len(result) < 100:
                    return result
        return None

    def extract_created_by(self, text: str) -> Optional[str]:
        """Extract creator/designer name."""
        # Try infobox fields
        for field in ['creator', 'created_by', 'designer', 'designed_by', 'artist']:
            creator = self.extract_infobox_field(text, field)
            if creator:
                return creator

        # Search for "created by" or "designed by" patterns
        patterns = [
            r'(?:created|designed|developed)\s+by\s+\[\[([^\]|]+)',
            r'(?:created|designed|developed)\s+by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)',
            r'creator[s]?\s+(?:is|are|was|were)\s+\[\[([^\]|]+)',
            r'designed\s+by\s+([^,.\n]+)',
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                result = self._clean_wiki_markup(match.group(1))
                if result and len(result) < 100:
                    return result
        return None

    def extract_design_description(self, text: str) -> Optional[str]:
        """Extract physical design/appearance description."""
        # Look for design/appearance section or description
        patterns = [
            # Standing X metres/feet tall pattern
            r'(Standing\s+[\d.]+\s+(?:metres?|meters?|m)\s*\([^)]+\)[^.]*(?:tall)?[^.]*\.[^.]*\.)',
            # Physical description patterns
            r'((?:It\s+)?(?:is\s+)?(?:a\s+)?(?:bipedal|quadrupedal)?\s*(?:yellow|red|blue|green|orange|purple|pink|brown|black|white|gray|grey)?\s*(?:rodent|mouse|cat|dog|bird|dragon|lizard|turtle|snake|fish|frog)[^.]*(?:with|has|having)[^.]*\.)',
            # Height and appearance
            r'(\d+(?:\.\d+)?\s*(?:m|cm|ft|in|metres?|meters?|feet|inches?)[^.]*(?:tall|height)[^.]*(?:with|has|having)[^.]*\.)',
            # Color and body description
            r'((?:It\s+)?has\s+(?:yellow|red|blue|green|orange|purple|pink|brown|black|white|gray|grey)\s+(?:fur|skin|scales|feathers|body)[^.]*\.)',
            # General design description
            r'((?:Its\s+)?design\s+(?:is\s+)?(?:based\s+on|inspired\s+by|resembles)[^.]*\.)',
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                result = self._clean_wiki_markup(match.group(1))
                if result and 20 < len(result) < 500:
                    return result

        # Try to find any sentence describing physical appearance
        appearance_keywords = ['tall', 'bipedal', 'quadrupedal', 'yellow fur', 'red cheek',
                              'pointed ears', 'lightning bolt', 'tail shaped', 'long ears',
                              'short arms', 'rodent', 'mouse-like']

        sentences = re.split(r'(?<=[.!?])\s+', text)
        for sentence in sentences:
            sentence_lower = sentence.lower()
            if any(keyword in sentence_lower for keyword in appearance_keywords):
                result = self._clean_wiki_markup(sentence)
                if result and 20 < len(result) < 500:
                    return result

        return None

    def extract_description(self, text: str) -> Optional[str]:
        """Extract the first paragraph as description."""
        # Remove infobox
        text = re.sub(r'\{\{[Ii]nfobox[^}]+\}\}', '', text, flags=re.DOTALL)
        # Remove short description template
        text = re.sub(r'\{\{[Ss]hort description[^}]+\}\}', '', text)

        # Get first substantial paragraph
        paragraphs = text.split('\n\n')
        for para in paragraphs:
            para = self._clean_wiki_markup(para)
            if len(para) > 50 and not para.startswith('{{') and not para.startswith('|'):
                # Limit to first 500 chars
                return para[:500] + ('...' if len(para) > 500 else '')
        return None

    def extract_all_info(self, title: str, text: str) -> Dict:
        """Extract all available Pokemon information from a wiki page."""
        if not text:
            return {'title': title, 'has_pokemon_info': False}

        types = self.extract_pokemon_type(text)
        species = self.extract_species(text)
        generation = self.extract_generation(text)
        abilities = self.extract_abilities(text)
        evolution = self.extract_evolution(text)
        physical = self.extract_physical_stats(text)
        japanese_name = self.extract_japanese_name(text)
        pokedex_number = self.extract_pokedex_number(text)
        first_game = self.extract_first_game(text)
        created_by = self.extract_created_by(text)
        design_description = self.extract_design_description(text)
        description = self.extract_description(text)

        # Check if this is actually a Pokemon species page
        has_pokemon_info = bool(types or species or generation or abilities or pokedex_number or first_game or design_description)

        return {
            'title': title,
            'has_pokemon_info': has_pokemon_info,
            'types': types if types else None,
            'species': species,
            'generation': generation,
            'abilities': abilities if abilities else None,
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

    def process_wiki_pages(self) -> List[Dict]:
        """Process all wiki pages and extract Pokemon info."""
        if not WIKI_PAGES_WITH_TEXT_FILE.exists():
            logger.error(f"Wiki pages file not found: {WIKI_PAGES_WITH_TEXT_FILE}")
            return []

        logger.info(f"Loading wiki pages from {WIKI_PAGES_WITH_TEXT_FILE}")
        with open(WIKI_PAGES_WITH_TEXT_FILE, 'r', encoding='utf-8') as f:
            pages = json.load(f)

        logger.info(f"Processing {len(pages)} wiki pages...")

        results = []
        pages_with_info = 0

        for page in pages:
            title = page.get('title', '')
            text = page.get('text', '')

            info = self.extract_all_info(title, text)
            results.append(info)

            if info['has_pokemon_info']:
                pages_with_info += 1

        logger.info(f"Extracted Pokemon info from {pages_with_info} pages")

        return results

    def save_results(self, results: List[Dict]) -> None:
        """Save extracted information to JSON files."""
        ensure_directories()

        # Save all pages
        all_file = OUTPUT_DIR / OutputFiles.WIKI_PAGES_INFO_JSON
        with open(all_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved all pages info to {all_file}")

        # Save only pages with Pokemon info
        pokemon_pages = [r for r in results if r['has_pokemon_info']]
        pokemon_file = OUTPUT_DIR / OutputFiles.POKEMON_WIKI_INFO_JSON
        with open(pokemon_file, 'w', encoding='utf-8') as f:
            json.dump(pokemon_pages, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(pokemon_pages)} Pokemon pages to {pokemon_file}")

        # Create lookup by title
        lookup = {r['title']: r for r in results}
        lookup_file = OUTPUT_DIR / OutputFiles.WIKI_INFO_LOOKUP_JSON
        with open(lookup_file, 'w', encoding='utf-8') as f:
            json.dump(lookup, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved lookup file to {lookup_file}")

        # Save stats
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
        stats_file = OUTPUT_DIR / OutputFiles.EXTRACTION_STATS_JSON
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
        logger.info(f"Stats: {stats}")


def run_extraction():
    """Run the wiki info extraction process."""
    extractor = WikiInfoExtractor()
    results = extractor.process_wiki_pages()

    if results:
        extractor.save_results(results)

        # Show some examples
        pokemon_pages = [r for r in results if r['has_pokemon_info']]
        logger.info("\nSample extracted Pokemon info:")
        for page in pokemon_pages[:5]:
            logger.info(f"  {page['title']}:")
            if page.get('types'):
                logger.info(f"    Types: {', '.join(page['types'])}")
            if page.get('species'):
                logger.info(f"    Species: {page['species']}")
            if page.get('generation'):
                logger.info(f"    Generation: {page['generation']}")
            if page.get('abilities'):
                logger.info(f"    Abilities: {', '.join(page['abilities'])}")
            if page.get('first_game'):
                logger.info(f"    First Game: {page['first_game']}")
            if page.get('created_by'):
                logger.info(f"    Created By: {page['created_by']}")
            if page.get('design_description'):
                logger.info(f"    Design: {page['design_description'][:100]}...")
    else:
        logger.error("No results to save")


if __name__ == "__main__":
    run_extraction()
