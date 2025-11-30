import re
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

from loguru import logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lower, regexp_extract, length, substring,
    udf, lit, when, count, avg, max as spark_max, min as spark_min
)
from pyspark.sql.types import StringType, StructType, StructField, BooleanType

from wiki_parser.config import (
    WIKI_DUMP_FILE, WIKI_SPARK_OUTPUT_DIR, WIKI_CONFIG, SETS_DIR, CARDS_DIR,
    OutputFiles, get_spark_session, ensure_directories
)


@dataclass
class ProcessingResult:
    total_pages: int
    pokemon_pages: int
    avg_text_length: int
    max_text_length: int
    min_text_length: int
    output_files: List[str]


def load_set_names() -> List[str]:
    set_names = []
    if SETS_DIR.exists():
        for json_file in SETS_DIR.glob("*.json"):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                name = data.get('Name', '')
                if name and name != 'undefined':
                    set_names.append(name.lower())
                    series = data.get('Series', '')
                    if series and series.lower() not in set_names:
                        set_names.append(series.lower())
            except:
                pass
    return list(set(set_names))


def load_pokemon_names() -> List[str]:
    pokemon_names = set()
    if CARDS_DIR.exists():
        for json_file in CARDS_DIR.glob("*.json"):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                name = data.get('Pokemon', '') or data.get('Name', '')
                if name:
                    pokemon_names.add(name.lower())
            except:
                pass
    return list(pokemon_names)


class WikiSparkProcessor:
    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark
        self._owns_spark = spark is None
        self.set_names = load_set_names()
        self.pokemon_names = load_pokemon_names()
        logger.info(f"Loaded {len(self.set_names)} set/series names for filtering")
        logger.info(f"Loaded {len(self.pokemon_names)} Pokemon names for filtering")

    def _get_spark(self) -> SparkSession:
        if self.spark is None:
            logger.info("Creating Spark session...")
            self.spark = get_spark_session()
            logger.info(f"Spark session created: {self.spark.sparkContext.appName}")
        return self.spark

    def _extract_title(self, page_content: str) -> str:
        match = re.search(r'<title>([^<]+)</title>', page_content)
        return match.group(1) if match else ""

    def _extract_text(self, page_content: str) -> str:
        match = re.search(r'<text[^>]*>(.*?)</text>', page_content, re.DOTALL)
        if match:
            text = match.group(1)
            max_len = WIKI_CONFIG.max_text_length
            if max_len and len(text) > max_len:
                text = text[:max_len]
            return text
        return ""

    def _is_system_page(self, title: str) -> bool:
        return any(title.startswith(prefix) for prefix in WIKI_CONFIG.skip_prefixes)

    def _contains_pokemon_keyword(self, text: str) -> bool:
        text_lower = text.lower()
        return any(kw in text_lower for kw in WIKI_CONFIG.pokemon_keywords[:2])

    def load_wiki_dump(self) -> DataFrame:
        spark = self._get_spark()

        if not WIKI_DUMP_FILE.exists():
            raise FileNotFoundError(f"Wikipedia dump not found: {WIKI_DUMP_FILE}")

        logger.info(f"Loading Wikipedia dump: {WIKI_DUMP_FILE}")
        logger.info(f"File size: {WIKI_DUMP_FILE.stat().st_size / (1024**3):.2f} GB")

        df = spark.read.option("lineSep", "</page>").text(str(WIKI_DUMP_FILE))
        logger.info("Wikipedia dump loaded into Spark")

        return df

    def filter_pokemon_pages(self, df: DataFrame) -> DataFrame:
        logger.info("Filtering Pokemon-related pages...")

        title_pattern = r"<title>([^<]+)</title>"

        filtered_df = df \
            .withColumn("title", regexp_extract(col("value"), title_pattern, 1)) \
            .filter(length(col("title")) > 0) \
            .filter(~col("value").contains("<redirect")) \
            .filter(~col("title").startswith("Wikipedia:")) \
            .filter(~col("title").startswith("Talk:")) \
            .filter(~col("title").startswith("User:")) \
            .filter(~col("title").startswith("Template:")) \
            .filter(~col("title").startswith("Category:")) \
            .filter(~col("title").startswith("File:")) \
            .filter(~col("title").startswith("Draft:")) \
            .filter(~col("title").startswith("Portal:")) \
            .filter(~col("title").startswith("Help:")) \
            .filter(~col("title").startswith("MediaWiki:")) \
            .filter(~col("title").startswith("Module:")) \
            .filter(
                lower(col("value")).contains("pokemon") |
                lower(col("value")).contains("pokémon") |
                lower(col("title")).contains("pokemon") |
                lower(col("title")).contains("pokémon") |
                lower(col("value")).contains("pikachu") |
                lower(col("value")).contains("charizard") |
                lower(col("value")).contains("mewtwo") |
                lower(col("value")).contains("trading card game") |
                lower(col("value")).contains("scarlet & violet") |
                lower(col("value")).contains("sun & moon") |
                lower(col("value")).contains("sword & shield")
            ) \
            .select("title", "value")

        return filtered_df

    def extract_page_data(self, df: DataFrame) -> List[Dict]:
        logger.info("Extracting page content...")

        df.cache()
        count = df.count()
        logger.info(f"Found {count} Pokemon-related pages")

        if count == 0:
            return []

        rows = df.collect()

        results = []
        for row in rows:
            title = row.title
            text = self._extract_text(row.value)
            results.append({
                "title": title,
                "text": text
            })

        logger.info(f"Extracted content from {len(results)} pages")
        return results

    def calculate_statistics(self, pages: List[Dict]) -> Dict:
        if not pages:
            return {
                "total_pages": 0,
                "avg_text_length": 0,
                "max_text_length": 0,
                "min_text_length": 0
            }

        text_lengths = [len(p.get("text", "")) for p in pages]

        return {
            "total_pages_with_pokemon_mention": len(pages),
            "avg_text_length": sum(text_lengths) // len(text_lengths),
            "max_text_length": max(text_lengths),
            "min_text_length": min(text_lengths),
            "pages_with_text": sum(1 for l in text_lengths if l > 0),
            "pages_without_text": sum(1 for l in text_lengths if l == 0)
        }

    def save_results(self, pages: List[Dict], stats: Dict) -> List[str]:
        ensure_directories()
        output_files = []

        pages_file = WIKI_SPARK_OUTPUT_DIR / OutputFiles.WIKI_PAGES_WITH_TEXT_JSON
        with open(pages_file, 'w', encoding='utf-8') as f:
            json.dump(pages, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(pages)} pages to {pages_file}")
        output_files.append(str(pages_file))

        titles = [p["title"] for p in pages]
        titles_file = WIKI_SPARK_OUTPUT_DIR / OutputFiles.WIKI_PAGES_JSON
        with open(titles_file, 'w', encoding='utf-8') as f:
            json.dump(titles, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(titles)} titles to {titles_file}")
        output_files.append(str(titles_file))

        stats_file = WIKI_SPARK_OUTPUT_DIR / OutputFiles.SPARK_STATS_JSON
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved statistics to {stats_file}")
        output_files.append(str(stats_file))

        return output_files

    def run(self, wait_for_user: bool = True) -> ProcessingResult:
        logger.info("=" * 80)
        logger.info("WIKIPEDIA POKEMON FILTER - SPARK JOB")
        logger.info("=" * 80)

        try:
            df = self.load_wiki_dump()
            filtered_df = self.filter_pokemon_pages(df)
            pages = self.extract_page_data(filtered_df)

            stats = self.calculate_statistics(pages)
            output_files = self.save_results(pages, stats)

            logger.info("-" * 40)
            logger.info("PROCESSING COMPLETE")
            logger.info(f"  Total Pokemon pages: {stats['total_pages_with_pokemon_mention']}")
            logger.info(f"  Avg text length: {stats['avg_text_length']:,} chars")
            logger.info(f"  Max text length: {stats['max_text_length']:,} chars")
            logger.info("-" * 40)

            result = ProcessingResult(
                total_pages=stats['total_pages_with_pokemon_mention'],
                pokemon_pages=stats['total_pages_with_pokemon_mention'],
                avg_text_length=stats['avg_text_length'],
                max_text_length=stats['max_text_length'],
                min_text_length=stats['min_text_length'],
                output_files=output_files
            )

            if wait_for_user:
                logger.info(f"Spark UI available at http://localhost:4040")
                input("Press Enter to stop Spark session...")

            return result

        finally:
            if self._owns_spark and self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")


def run_spark_job() -> ProcessingResult:
    processor = WikiSparkProcessor()
    return processor.run()


if __name__ == "__main__":
    run_spark_job()
