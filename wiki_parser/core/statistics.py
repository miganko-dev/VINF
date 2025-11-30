"""
Statistics Module

Generates comprehensive statistics about Pokemon cards, wiki pages,
and their relationships using Apache Spark.

Usage:
    from wiki_parser.core.statistics import StatisticsGenerator
    generator = StatisticsGenerator()
    stats = generator.generate_all()
"""

import json
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from collections import Counter

from loguru import logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, max as spark_max, min as spark_min,
    countDistinct, collect_list, collect_set, explode, size,
    when, lit, desc, asc, round as spark_round, concat_ws
)

from wiki_parser.config import (
    CARDS_DIR, SETS_DIR, WIKI_SPARK_OUTPUT_DIR, WIKI_PARSED_DIR,
    JOINED_OUTPUT_DIR, STATISTICS_OUTPUT_DIR, OutputFiles,
    get_spark_session, ensure_directories
)


@dataclass
class CardStatistics:
    """Statistics about Pokemon cards."""
    total_cards: int
    unique_pokemon: int
    unique_sets: int
    cards_per_pokemon_avg: float
    cards_per_pokemon_max: int
    cards_per_set_avg: float
    price_avg: float
    price_max: float
    price_min: float
    price_median: float
    rarities_distribution: Dict[str, int]
    top_pokemon_by_cards: List[Dict]
    top_sets_by_cards: List[Dict]
    price_ranges: Dict[str, int]


@dataclass
class WikiStatistics:
    """Statistics about Wikipedia pages."""
    total_pages: int
    pages_with_pokemon_info: int
    avg_text_length: int
    pages_with_types: int
    pages_with_abilities: int
    pages_with_evolution: int
    type_distribution: Dict[str, int]
    generation_distribution: Dict[str, int]


@dataclass
class JoinStatistics:
    """Statistics about card-wiki joins."""
    pokemon_with_wiki: int
    pokemon_without_wiki: int
    match_rate: float
    avg_wiki_pages_per_pokemon: float
    cards_with_wiki: int
    cards_without_wiki: int


@dataclass
class CombinedStatistics:
    """All statistics combined."""
    card_stats: CardStatistics
    wiki_stats: WikiStatistics
    join_stats: JoinStatistics
    generated_at: str


class StatisticsGenerator:
    """
    Generates comprehensive statistics using Spark.

    This generator:
    1. Loads card, wiki, and join data
    2. Calculates detailed statistics using Spark aggregations
    3. Saves results to JSON files
    """

    def __init__(self, spark: Optional[SparkSession] = None):
        """
        Initialize the generator.

        Args:
            spark: Optional existing Spark session
        """
        self.spark = spark
        self._owns_spark = spark is None

    def _get_spark(self) -> SparkSession:
        """Get or create Spark session."""
        if self.spark is None:
            self.spark = get_spark_session()
        return self.spark

    def load_cards(self) -> List[Dict]:
        """Load all card JSON files."""
        cards = []
        if not CARDS_DIR.exists():
            return cards

        for json_file in CARDS_DIR.glob("*.json"):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    card = json.load(f)
                    # Parse price
                    price_str = str(card.get('Price', '0') or '0')
                    try:
                        price = float(price_str.replace('$', '').replace(',', ''))
                    except ValueError:
                        price = 0.0

                    cards.append({
                        'card_name': card.get('Name', ''),
                        'pokemon': card.get('Pokemon') or card.get('Name', ''),
                        'card_set': card.get('Set', ''),
                        'card_id': card.get('Id', ''),
                        'rarity': card.get('Rarity') or 'Unknown',
                        'price': price
                    })
            except Exception:
                pass

        return cards

    def load_sets(self) -> List[Dict]:
        """Load all set JSON files."""
        sets = []
        if not SETS_DIR.exists():
            return sets

        for json_file in SETS_DIR.glob("*.json"):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    set_data = json.load(f)
                    sets.append({
                        'name': set_data.get('Name', ''),
                        'release': set_data.get('Release', ''),
                        'series': set_data.get('Series', ''),
                        'total_cards': set_data.get('Total cards', 0)
                    })
            except Exception:
                pass

        return sets

    def load_wiki_info(self) -> List[Dict]:
        """Load extracted wiki info."""
        info_file = WIKI_PARSED_DIR / OutputFiles.POKEMON_WIKI_INFO_JSON
        if not info_file.exists():
            return []

        with open(info_file, 'r', encoding='utf-8') as f:
            return json.load(f)

    def load_joined_data(self) -> List[Dict]:
        """Load joined Pokemon-wiki data."""
        joined_file = JOINED_OUTPUT_DIR / OutputFiles.POKEMON_WITH_WIKI_JSON
        if not joined_file.exists():
            return []

        with open(joined_file, 'r', encoding='utf-8') as f:
            return json.load(f)

    def calculate_card_statistics(self) -> CardStatistics:
        """Calculate card statistics using Spark."""
        logger.info("Calculating card statistics...")
        spark = self._get_spark()

        cards = self.load_cards()
        if not cards:
            logger.warning("No cards found")
            return self._empty_card_stats()

        df = spark.createDataFrame(cards)
        df.cache()

        # Basic counts
        total_cards = df.count()
        unique_pokemon = df.select("pokemon").distinct().count()
        unique_sets = df.select("card_set").distinct().count()

        # Cards per Pokemon
        pokemon_counts = df.groupBy("pokemon").count()
        cards_per_pokemon = pokemon_counts.agg(
            avg("count").alias("avg"),
            spark_max("count").alias("max")
        ).collect()[0]

        # Cards per set
        set_counts = df.groupBy("card_set").count()
        cards_per_set_avg = set_counts.agg(avg("count")).collect()[0][0]

        # Price statistics
        price_stats = df.filter(col("price") > 0).agg(
            avg("price").alias("avg"),
            spark_max("price").alias("max"),
            spark_min("price").alias("min")
        ).collect()[0]

        # Price median (approximate)
        prices_sorted = [row.price for row in df.filter(col("price") > 0).orderBy("price").collect()]
        price_median = prices_sorted[len(prices_sorted) // 2] if prices_sorted else 0

        # Rarity distribution
        rarity_counts = df.groupBy("rarity").count().collect()
        rarities = {row.rarity: row['count'] for row in rarity_counts}

        # Top Pokemon by card count
        top_pokemon = pokemon_counts.orderBy(desc("count")).limit(20).collect()
        top_pokemon_list = [{"pokemon": row.pokemon, "cards": row['count']} for row in top_pokemon]

        # Top sets by card count
        top_sets = set_counts.orderBy(desc("count")).limit(20).collect()
        top_sets_list = [{"set": row.card_set, "cards": row['count']} for row in top_sets]

        # Price ranges
        price_ranges = {
            "free": df.filter(col("price") == 0).count(),
            "under_1": df.filter((col("price") > 0) & (col("price") < 1)).count(),
            "1_to_5": df.filter((col("price") >= 1) & (col("price") < 5)).count(),
            "5_to_10": df.filter((col("price") >= 5) & (col("price") < 10)).count(),
            "10_to_50": df.filter((col("price") >= 10) & (col("price") < 50)).count(),
            "50_to_100": df.filter((col("price") >= 50) & (col("price") < 100)).count(),
            "over_100": df.filter(col("price") >= 100).count()
        }

        return CardStatistics(
            total_cards=total_cards,
            unique_pokemon=unique_pokemon,
            unique_sets=unique_sets,
            cards_per_pokemon_avg=round(cards_per_pokemon.avg or 0, 2),
            cards_per_pokemon_max=cards_per_pokemon['max'] or 0,
            cards_per_set_avg=round(cards_per_set_avg or 0, 2),
            price_avg=round(price_stats.avg or 0, 2),
            price_max=round(price_stats['max'] or 0, 2),
            price_min=round(price_stats['min'] or 0, 2),
            price_median=round(price_median, 2),
            rarities_distribution=rarities,
            top_pokemon_by_cards=top_pokemon_list,
            top_sets_by_cards=top_sets_list,
            price_ranges=price_ranges
        )

    def calculate_wiki_statistics(self) -> WikiStatistics:
        """Calculate wiki statistics."""
        logger.info("Calculating wiki statistics...")
        spark = self._get_spark()

        wiki_info = self.load_wiki_info()
        if not wiki_info:
            logger.warning("No wiki info found")
            return self._empty_wiki_stats()

        total_pages = len(wiki_info)
        pages_with_info = sum(1 for p in wiki_info if p.get('has_pokemon_info'))

        # Type distribution
        type_counts = Counter()
        for page in wiki_info:
            types = page.get('types') or []
            for t in types:
                type_counts[t] += 1

        # Generation distribution
        gen_counts = Counter()
        for page in wiki_info:
            gen = page.get('generation')
            if gen:
                gen_counts[str(gen)] += 1

        # Feature counts
        with_types = sum(1 for p in wiki_info if p.get('types'))
        with_abilities = sum(1 for p in wiki_info if p.get('abilities'))
        with_evolution = sum(1 for p in wiki_info if p.get('evolves_from') or p.get('evolves_to'))

        # Average text length (load from spark output)
        spark_stats_file = WIKI_SPARK_OUTPUT_DIR / OutputFiles.SPARK_STATS_JSON
        avg_text_len = 0
        if spark_stats_file.exists():
            with open(spark_stats_file, 'r', encoding='utf-8') as f:
                spark_stats = json.load(f)
                avg_text_len = spark_stats.get('avg_text_length', 0)

        return WikiStatistics(
            total_pages=total_pages,
            pages_with_pokemon_info=pages_with_info,
            avg_text_length=avg_text_len,
            pages_with_types=with_types,
            pages_with_abilities=with_abilities,
            pages_with_evolution=with_evolution,
            type_distribution=dict(type_counts.most_common()),
            generation_distribution=dict(gen_counts.most_common())
        )

    def calculate_join_statistics(self) -> JoinStatistics:
        """Calculate join statistics."""
        logger.info("Calculating join statistics...")

        joined_data = self.load_joined_data()
        if not joined_data:
            logger.warning("No joined data found")
            return self._empty_join_stats()

        total_pokemon = len(joined_data)
        with_wiki = sum(1 for p in joined_data if p.get('wiki_pages'))
        without_wiki = total_pokemon - with_wiki

        # Calculate wiki pages per Pokemon
        wiki_page_counts = [len(p.get('wiki_pages') or []) for p in joined_data if p.get('wiki_pages')]
        avg_wiki_pages = sum(wiki_page_counts) / len(wiki_page_counts) if wiki_page_counts else 0

        # Card counts
        total_cards = sum(p.get('card_count', 0) for p in joined_data)
        cards_with_wiki = sum(
            p.get('card_count', 0) for p in joined_data if p.get('wiki_pages')
        )

        return JoinStatistics(
            pokemon_with_wiki=with_wiki,
            pokemon_without_wiki=without_wiki,
            match_rate=round(with_wiki / total_pokemon * 100, 2) if total_pokemon else 0,
            avg_wiki_pages_per_pokemon=round(avg_wiki_pages, 2),
            cards_with_wiki=cards_with_wiki,
            cards_without_wiki=total_cards - cards_with_wiki
        )

    def _empty_card_stats(self) -> CardStatistics:
        """Return empty card statistics."""
        return CardStatistics(
            total_cards=0, unique_pokemon=0, unique_sets=0,
            cards_per_pokemon_avg=0, cards_per_pokemon_max=0,
            cards_per_set_avg=0, price_avg=0, price_max=0,
            price_min=0, price_median=0, rarities_distribution={},
            top_pokemon_by_cards=[], top_sets_by_cards=[], price_ranges={}
        )

    def _empty_wiki_stats(self) -> WikiStatistics:
        """Return empty wiki statistics."""
        return WikiStatistics(
            total_pages=0, pages_with_pokemon_info=0, avg_text_length=0,
            pages_with_types=0, pages_with_abilities=0, pages_with_evolution=0,
            type_distribution={}, generation_distribution={}
        )

    def _empty_join_stats(self) -> JoinStatistics:
        """Return empty join statistics."""
        return JoinStatistics(
            pokemon_with_wiki=0, pokemon_without_wiki=0, match_rate=0,
            avg_wiki_pages_per_pokemon=0, cards_with_wiki=0, cards_without_wiki=0
        )

    def save_statistics(
        self,
        card_stats: CardStatistics,
        wiki_stats: WikiStatistics,
        join_stats: JoinStatistics
    ) -> List[str]:
        """Save all statistics to files."""
        ensure_directories()
        output_files = []

        # Save card statistics
        card_file = STATISTICS_OUTPUT_DIR / OutputFiles.CARD_STATISTICS_JSON
        with open(card_file, 'w', encoding='utf-8') as f:
            json.dump(asdict(card_stats), f, ensure_ascii=False, indent=2)
        output_files.append(str(card_file))
        logger.info(f"Saved card statistics to {card_file}")

        # Save wiki statistics
        wiki_file = STATISTICS_OUTPUT_DIR / OutputFiles.WIKI_STATISTICS_JSON
        with open(wiki_file, 'w', encoding='utf-8') as f:
            json.dump(asdict(wiki_stats), f, ensure_ascii=False, indent=2)
        output_files.append(str(wiki_file))
        logger.info(f"Saved wiki statistics to {wiki_file}")

        # Save combined statistics
        from datetime import datetime
        combined = {
            'card_statistics': asdict(card_stats),
            'wiki_statistics': asdict(wiki_stats),
            'join_statistics': asdict(join_stats),
            'generated_at': datetime.now().isoformat()
        }
        combined_file = STATISTICS_OUTPUT_DIR / OutputFiles.COMBINED_STATISTICS_JSON
        with open(combined_file, 'w', encoding='utf-8') as f:
            json.dump(combined, f, ensure_ascii=False, indent=2)
        output_files.append(str(combined_file))
        logger.info(f"Saved combined statistics to {combined_file}")

        return output_files

    def print_summary(
        self,
        card_stats: CardStatistics,
        wiki_stats: WikiStatistics,
        join_stats: JoinStatistics
    ) -> None:
        """Print statistics summary to console."""
        logger.info("=" * 60)
        logger.info("STATISTICS SUMMARY")
        logger.info("=" * 60)

        logger.info("\n--- CARD STATISTICS ---")
        logger.info(f"  Total cards: {card_stats.total_cards:,}")
        logger.info(f"  Unique Pokemon: {card_stats.unique_pokemon:,}")
        logger.info(f"  Unique sets: {card_stats.unique_sets:,}")
        logger.info(f"  Avg cards per Pokemon: {card_stats.cards_per_pokemon_avg}")
        logger.info(f"  Avg price: ${card_stats.price_avg}")
        logger.info(f"  Max price: ${card_stats.price_max}")

        logger.info("\n--- WIKI STATISTICS ---")
        logger.info(f"  Total wiki pages: {wiki_stats.total_pages:,}")
        logger.info(f"  Pages with Pokemon info: {wiki_stats.pages_with_pokemon_info:,}")
        logger.info(f"  Pages with types: {wiki_stats.pages_with_types:,}")
        logger.info(f"  Pages with abilities: {wiki_stats.pages_with_abilities:,}")

        logger.info("\n--- JOIN STATISTICS ---")
        logger.info(f"  Pokemon with wiki: {join_stats.pokemon_with_wiki:,}")
        logger.info(f"  Pokemon without wiki: {join_stats.pokemon_without_wiki:,}")
        logger.info(f"  Match rate: {join_stats.match_rate}%")
        logger.info(f"  Cards with wiki: {join_stats.cards_with_wiki:,}")

        logger.info("\n--- TOP 5 POKEMON BY CARDS ---")
        for i, p in enumerate(card_stats.top_pokemon_by_cards[:5], 1):
            logger.info(f"  {i}. {p['pokemon']}: {p['cards']} cards")

        logger.info("\n--- TOP 5 SETS BY CARDS ---")
        for i, s in enumerate(card_stats.top_sets_by_cards[:5], 1):
            logger.info(f"  {i}. {s['set']}: {s['cards']} cards")

        if wiki_stats.type_distribution:
            logger.info("\n--- TOP 5 POKEMON TYPES ---")
            top_types = list(wiki_stats.type_distribution.items())[:5]
            for i, (t, c) in enumerate(top_types, 1):
                logger.info(f"  {i}. {t}: {c} pages")

        logger.info("=" * 60)

    def generate_all(self) -> Dict:
        """
        Generate all statistics.

        Returns:
            Dictionary with all statistics
        """
        logger.info("=" * 80)
        logger.info("GENERATING STATISTICS")
        logger.info("=" * 80)

        try:
            card_stats = self.calculate_card_statistics()
            wiki_stats = self.calculate_wiki_statistics()
            join_stats = self.calculate_join_statistics()

            self.save_statistics(card_stats, wiki_stats, join_stats)
            self.print_summary(card_stats, wiki_stats, join_stats)

            return {
                'card': asdict(card_stats),
                'wiki': asdict(wiki_stats),
                'join': asdict(join_stats)
            }

        finally:
            if self._owns_spark and self.spark:
                self.spark.stop()


def run_statistics() -> Dict:
    """Generate all statistics."""
    generator = StatisticsGenerator()
    return generator.generate_all()


if __name__ == "__main__":
    run_statistics()
