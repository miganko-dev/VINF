from pathlib import Path
from dataclasses import dataclass, field
from typing import List


PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"

CARDS_DIR = DATA_DIR / "pokedata.io" / "parsed" / "cards"
SETS_DIR = DATA_DIR / "pokedata.io" / "parsed" / "sets"
JOINED_DIR = DATA_DIR / "joined"

INDEXER_DIR = PROJECT_ROOT / "indexer"
INDEX_DATA_DIR = INDEXER_DIR / "data"
TFIDF_INDEX_FILE = INDEX_DATA_DIR / "tfidf_index.json"
LUCENE_INDEX_DIR = INDEXER_DIR / "lucene_index"

LOG_DIR = INDEX_DATA_DIR
LOG_FILE = LOG_DIR / "indexer.log"
LOG_ROTATION = "10 MB"


@dataclass
class TFIDFConfig:
    min_token_length: int = 2
    max_token_length: int = 50
    stopwords: List[str] = field(default_factory=lambda: [
        'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
        'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'been',
        'be', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
        'could', 'should', 'may', 'might', 'must', 'shall', 'can', 'need',
        'it', 'its', 'this', 'that', 'these', 'those', 'i', 'you', 'he', 'she',
        'we', 'they', 'what', 'which', 'who', 'when', 'where', 'why', 'how'
    ])
    idf_smoothing: float = 1.0


@dataclass
class LuceneConfig:
    use_bm25: bool = True
    bm25_b: float = 0.75
    bm25_k1: float = 1.2
    default_top_k: int = 10
    max_top_k: int = 1000
    default_fuzzy_distance: int = 2
    max_fuzzy_distance: int = 3
    searchable_fields: List[str] = field(default_factory=lambda: [
        'card_name', 'pokemon', 'card_set', 'content', 'wiki_page'
    ])
    facet_fields: List[str] = field(default_factory=lambda: ['rarity'])
    numeric_fields: List[str] = field(default_factory=lambda: ['price'])


TFIDF_CONFIG = TFIDFConfig()
LUCENE_CONFIG = LuceneConfig()


@dataclass
class SearchConfig:
    max_snippet_length: int = 200
    highlight_tag: str = "**"
    price_ranges: List[tuple] = field(default_factory=lambda: [
        (0, 0, "Free"),
        (0.01, 0.99, "Under $1"),
        (1, 4.99, "$1-$5"),
        (5, 9.99, "$5-$10"),
        (10, 49.99, "$10-$50"),
        (50, 99.99, "$50-$100"),
        (100, None, "$100+")
    ])


SEARCH_CONFIG = SearchConfig()


class IndexOutputFiles:
    TFIDF_INDEX = "tfidf_index.json"
    TFIDF_STATS = "tfidf_stats.json"
    LUCENE_STATS = "index_stats.json"
    COMPARISON_REPORT = "comparison_report.json"


def ensure_directories() -> None:
    directories = [INDEX_DATA_DIR, LUCENE_INDEX_DIR, LOG_DIR]
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
