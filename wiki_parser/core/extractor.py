import re
import xml.etree.ElementTree as ET
from wiki_parser.config import WIKI_NS, SKIP_PAGE_PREFIXES

def parse_xml_page(xml_string):
    """
    Parses a single <page> XML chunk.
    Returns (title, text) or (None, None) if invalid/redirect.
    """
    try:
        if not xml_string.strip().endswith('</page>'):
            xml_string += '</page>'
        if not xml_string.strip().startswith('<page>'):
            xml_string = '<page>' + xml_string

        root = ET.fromstring(xml_string)

        ns = WIKI_NS

        redirect = root.find(f'{ns}redirect')
        if redirect is not None:
            return None, None

        title_elem = root.find(f'{ns}title')
        revision = root.find(f'{ns}revision')

        if title_elem is not None and revision is not None:
            text_elem = revision.find(f'{ns}text')
            if text_elem is not None and text_elem.text:
                title = title_elem.text
                text = text_elem.text

                if any(title.startswith(prefix) for prefix in SKIP_PAGE_PREFIXES):
                    return None, None

                return title, text

    except ET.ParseError:
        pass
    except Exception:
        pass

    return None, None

def has_pokemon_mention(title, text):
    """
    Simply checks if 'pokemon' (or 'pokémon') appears in title or text.
    Returns True if found, False otherwise.
    """
    pokemon_pattern = r'pok[eé]mon'

    if re.search(pokemon_pattern, title, re.IGNORECASE):
        return True

    if re.search(pokemon_pattern, text, re.IGNORECASE):
        return True

    return False

def extract_basic_info(title, text):
    """
    Extract basic info from a wiki page.
    """
    # Get a snippet of text (first 500 chars)
    text_snippet = text[:500].replace('\n', ' ').strip() if text else ""

    return {
        "title": title,
        "text_snippet": text_snippet,
        "text_length": len(text) if text else 0
    }
