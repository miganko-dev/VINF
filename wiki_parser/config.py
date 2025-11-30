"""
Wiki Parser Configuration

Centralized configuration for the wiki parser module.
All paths, Spark settings, and constants are defined here.
"""

import os
import sys
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Optional


# =============================================================================
# PATH CONFIGURATION
# =============================================================================

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"

# Wiki data paths
WIKI_DIR = DATA_DIR / "wiki"
WIKI_DUMP_FILE = WIKI_DIR / "enwiki-latest-pages-articles-multistream.xml.bz2"
WIKI_SPARK_OUTPUT_DIR = WIKI_DIR / "spark_output"
WIKI_PARSED_DIR = WIKI_DIR / "parsed"

# Card data paths
CARDS_DIR = DATA_DIR / "pokedata.io" / "parsed" / "cards"
SETS_DIR = DATA_DIR / "pokedata.io" / "parsed" / "sets"

# Output paths
JOINED_OUTPUT_DIR = DATA_DIR / "joined"
STATISTICS_OUTPUT_DIR = DATA_DIR / "statistics"


# =============================================================================
# SPARK CONFIGURATION
# =============================================================================

@dataclass
class SparkConfig:
    """Apache Spark configuration settings."""

    app_name: str = "PokemonWikiProcessor"
    master: str = "local[*]"
    executor_memory: str = "4g"
    driver_memory: str = "4g"
    shuffle_partitions: int = 8

    # Environment paths (Windows-specific defaults)
    spark_home: str = r"C:\DevTools\spark-3.5.7-bin-hadoop3"
    java_home: str = r"C:\Program Files\Microsoft\jdk-11.0.28.6-hotspot"

    # Spark UI
    ui_enabled: bool = True
    ui_port: int = 4040

    def setup_environment(self) -> None:
        """Configure environment variables for Spark."""
        os.environ['SPARK_HOME'] = self.spark_home
        os.environ['JAVA_HOME'] = self.java_home
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
        os.environ['SPARK_DRIVER_HOST'] = 'localhost'

        if 'HADOOP_HOME' not in os.environ:
            os.environ['HADOOP_HOME'] = self.spark_home

        # Add PySpark to path
        pyspark_path = os.path.join(self.spark_home, 'python')
        py4j_path = os.path.join(self.spark_home, 'python', 'lib', 'py4j-0.10.9.7-src.zip')

        if pyspark_path not in sys.path:
            sys.path.insert(0, pyspark_path)
        if py4j_path not in sys.path:
            sys.path.insert(0, py4j_path)


# Default Spark configuration
SPARK_CONFIG = SparkConfig()


# =============================================================================
# WIKI PROCESSING CONFIGURATION
# =============================================================================

@dataclass
class WikiConfig:
    """Wikipedia processing configuration."""

    # XML namespace for MediaWiki
    namespace: str = '{http://www.mediawiki.org/xml/export-0.11/}'

    # Page prefixes to skip (system/meta pages)
    skip_prefixes: List[str] = field(default_factory=lambda: [
        'Wikipedia:', 'Talk:', 'User:', 'Template:',
        'Category:', 'File:', 'Draft:', 'Portal:',
        'Help:', 'MediaWiki:', 'Module:', 'TimedText:',
        'Book:', 'Education Program:', 'Gadget:', 'Gadget definition:'
    ])

    # Pokemon-related keywords for filtering
    pokemon_keywords: List[str] = field(default_factory=lambda: [
        'pokemon', 'pokémon', 'pikachu', 'charizard',
        'nintendo', 'game freak', 'pokedex', 'pokédex'
    ])

    # Maximum text length to store (chars)
    max_text_length: int = 100000


# Default Wiki configuration
WIKI_CONFIG = WikiConfig()


# =============================================================================
# POKEMON DATA CONFIGURATION
# =============================================================================

@dataclass
class PokemonConfig:
    """Pokemon data processing configuration."""

    # Valid Pokemon types
    types: List[str] = field(default_factory=lambda: [
        'normal', 'fire', 'water', 'electric', 'grass', 'ice',
        'fighting', 'poison', 'ground', 'flying', 'psychic', 'bug',
        'rock', 'ghost', 'dragon', 'dark', 'steel', 'fairy'
    ])

    # Card rarity levels (ordered by rarity)
    rarities: List[str] = field(default_factory=lambda: [
        'Common', 'Uncommon', 'Rare', 'Holo Rare', 'Reverse Holo',
        'Ultra Rare', 'Secret Rare', 'Full Art', 'Rainbow Rare',
        'Gold Rare', 'Illustration Rare', 'Special Art Rare',
        'VMAX', 'VSTAR', 'EX', 'GX', 'V'
    ])


# Default Pokemon configuration
POKEMON_CONFIG = PokemonConfig()


# =============================================================================
# OUTPUT FILE NAMES
# =============================================================================

class OutputFiles:
    """Standard output file names."""

    # Spark job outputs
    WIKI_PAGES_JSON = "pokemon_wiki_pages.json"
    WIKI_PAGES_WITH_TEXT_JSON = "pokemon_wiki_pages_with_text.json"
    SPARK_STATS_JSON = "spark_stats.json"

    # Extracted wiki info
    WIKI_PAGES_INFO_JSON = "wiki_pages_info.json"
    POKEMON_WIKI_INFO_JSON = "pokemon_wiki_info.json"
    WIKI_INFO_LOOKUP_JSON = "wiki_info_lookup.json"
    EXTRACTION_STATS_JSON = "extraction_stats.json"

    # Join outputs
    POKEMON_WITH_WIKI_JSON = "pokemon_with_wiki_and_cards.json"
    POKEMON_MATCHED_WIKI_JSON = "pokemon_matched_wiki.json"
    WIKI_WITH_POKEMON_JSON = "wiki_with_pokemon.json"
    WIKI_MATCHED_POKEMON_JSON = "wiki_matched_pokemon.json"
    JOIN_STATS_JSON = "join_stats.json"

    # Statistics outputs
    CARD_STATISTICS_JSON = "card_statistics.json"
    POKEMON_STATISTICS_JSON = "pokemon_statistics.json"
    WIKI_STATISTICS_JSON = "wiki_statistics.json"
    COMBINED_STATISTICS_JSON = "combined_statistics.json"


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def ensure_directories() -> None:
    """Create all required output directories."""
    directories = [
        WIKI_DIR,
        WIKI_SPARK_OUTPUT_DIR,
        WIKI_PARSED_DIR,
        JOINED_OUTPUT_DIR,
        STATISTICS_OUTPUT_DIR
    ]

    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)


def get_spark_session():
    """Create and configure a Spark session."""
    from pyspark.sql import SparkSession

    SPARK_CONFIG.setup_environment()

    return SparkSession.builder \
        .appName(SPARK_CONFIG.app_name) \
        .master(SPARK_CONFIG.master) \
        .config("spark.executor.memory", SPARK_CONFIG.executor_memory) \
        .config("spark.driver.memory", SPARK_CONFIG.driver_memory) \
        .config("spark.ui.enabled", str(SPARK_CONFIG.ui_enabled).lower()) \
        .config("spark.ui.port", str(SPARK_CONFIG.ui_port)) \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "localhost") \
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
        .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
        .config("spark.sql.shuffle.partitions", str(SPARK_CONFIG.shuffle_partitions)) \
        .getOrCreate()
