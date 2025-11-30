import os
import json
import re
from pathlib import Path
from typing import List, Dict, Optional
from loguru import logger

from whoosh import index
from whoosh.fields import Schema, TEXT, ID, KEYWORD, NUMERIC, STORED
from whoosh.qparser import QueryParser, MultifieldParser, OrGroup
from whoosh.query import And, Or, Term, FuzzyTerm, Phrase, NumericRange
from whoosh.analysis import StandardAnalyzer, StemmingAnalyzer
from whoosh import scoring

from indexer.config import (
    CARDS_DIR, JOINED_DIR, LUCENE_INDEX_DIR, ensure_directories
)

INDEX_DIR = LUCENE_INDEX_DIR
JOINED_DATA_FILE = JOINED_DIR / "pokemon_with_wiki_and_cards.json"


class LuceneStyleIndexer:
    def __init__(self):
        self.index_dir = INDEX_DIR
        self.ix = None
        self.schema = self._create_schema()

    def _create_schema(self) -> Schema:
        return Schema(
            card_name=TEXT(stored=True, analyzer=StemmingAnalyzer()),
            pokemon=TEXT(stored=True, analyzer=StandardAnalyzer()),
            card_set=TEXT(stored=True, analyzer=StandardAnalyzer()),
            card_id=ID(stored=True, unique=True),
            rarity=KEYWORD(stored=True, lowercase=True),
            price=NUMERIC(stored=True, numtype=float),
            image_url=STORED,
            source_url=STORED,
            wiki_page=TEXT(stored=True, analyzer=StandardAnalyzer()),
            content=TEXT(analyzer=StemmingAnalyzer())
        )

    def build_index(self, use_joined_data: bool = True) -> None:
        logger.info("Building Lucene-style index...")

        self.index_dir.mkdir(parents=True, exist_ok=True)
        self.ix = index.create_in(str(self.index_dir), self.schema)

        if use_joined_data and JOINED_DATA_FILE.exists():
            cards = self._load_joined_data()
        else:
            cards = self._load_card_files()

        if not cards:
            logger.error("No cards to index!")
            return

        writer = self.ix.writer()
        indexed_count = 0

        for card in cards:
            try:
                self._index_document(writer, card)
                indexed_count += 1
            except Exception as e:
                logger.debug(f"Error indexing card: {e}")

        writer.commit()
        logger.info(f"Indexed {indexed_count} cards successfully")
        self._save_stats(indexed_count)

    def _load_joined_data(self) -> List[Dict]:
        logger.info(f"Loading joined data from {JOINED_DATA_FILE}")
        with open(JOINED_DATA_FILE, 'r', encoding='utf-8') as f:
            pokemon_data = json.load(f)

        cards = []
        for pokemon_entry in pokemon_data:
            pokemon_name = pokemon_entry.get('pokemon', '')
            wiki_pages = pokemon_entry.get('wiki_pages', [])
            wiki_page = wiki_pages[0] if wiki_pages else ''

            for card in pokemon_entry.get('cards', []):
                cards.append({
                    'card_name': card.get('name', ''),
                    'pokemon': pokemon_name,
                    'card_set': card.get('set', ''),
                    'card_id': card.get('id', ''),
                    'rarity': card.get('rarity', ''),
                    'price': card.get('price', '0'),
                    'image_url': card.get('image', ''),
                    'card_source': card.get('source', ''),
                    'wiki_page': wiki_page
                })

        logger.info(f"Loaded {len(cards)} cards from joined data")
        return cards

    def _load_card_files(self) -> List[Dict]:
        cards = []
        if not CARDS_DIR.exists():
            logger.warning(f"Cards directory not found: {CARDS_DIR}")
            return cards

        for json_file in CARDS_DIR.glob("*.json"):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    card = json.load(f)
                    cards.append({
                        'card_name': card.get('Name', ''),
                        'pokemon': card.get('Pokemon', ''),
                        'set': card.get('Set', ''),
                        'card_id': card.get('Id', ''),
                        'rarity': card.get('Rarity', ''),
                        'price': card.get('Price', '0'),
                        'image_url': card.get('Image', ''),
                        'card_source': card.get('Source', ''),
                        'wiki_page': None
                    })
            except Exception as e:
                logger.debug(f"Error reading {json_file}: {e}")

        logger.info(f"Loaded {len(cards)} cards from files")
        return cards

    def _index_document(self, writer, card: Dict) -> None:
        price_str = str(card.get('price', '0') or '0')
        try:
            price = float(price_str.replace('$', '').replace(',', ''))
        except ValueError:
            price = 0.0

        content_parts = [
            card.get('card_name', '') or '',
            card.get('pokemon', '') or '',
            card.get('set', '') or card.get('card_set', '') or '',
            card.get('rarity', '') or '',
            card.get('wiki_page', '') or ''
        ]
        content = ' '.join(filter(None, content_parts))

        writer.add_document(
            card_name=card.get('card_name', '') or '',
            pokemon=card.get('pokemon', '') or '',
            card_set=card.get('set', '') or card.get('card_set', '') or '',
            card_id=card.get('card_id', '') or '',
            rarity=card.get('rarity', '') or 'unknown',
            price=price,
            image_url=card.get('image_url', '') or '',
            source_url=card.get('card_source', '') or '',
            wiki_page=card.get('wiki_page', '') or '',
            content=content
        )

    def _save_stats(self, count: int) -> None:
        stats = {
            "total_documents": count,
            "index_path": str(self.index_dir),
            "schema_fields": list(self.schema.names())
        }
        stats_file = self.index_dir / "index_stats.json"
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2)
        logger.info(f"Stats saved to {stats_file}")

    def open_index(self) -> bool:
        if not self.index_dir.exists():
            logger.error(f"Index directory not found: {self.index_dir}")
            return False
        try:
            self.ix = index.open_dir(str(self.index_dir))
            return True
        except Exception as e:
            logger.error(f"Error opening index: {e}")
            return False

    def search_boolean(self, query_str: str, top_k: int = 10) -> List[Dict]:
        if not self.ix:
            if not self.open_index():
                return []

        logger.info(f"Boolean query: {query_str}")

        parser = MultifieldParser(
            ["content", "card_name", "pokemon", "card_set"],
            schema=self.schema
        )

        try:
            query = parser.parse(query_str)
            with self.ix.searcher(weighting=scoring.BM25F()) as searcher:
                results = searcher.search(query, limit=top_k)
                return self._format_results(results, "Boolean AND/OR")
        except Exception as e:
            logger.error(f"Boolean query error: {e}")
            return []

    def search_range(self, min_price: float, max_price: float, top_k: int = 10) -> List[Dict]:
        if not self.ix:
            if not self.open_index():
                return []

        logger.info(f"Range query: price ${min_price} - ${max_price}")

        query = NumericRange("price", min_price, max_price)

        with self.ix.searcher() as searcher:
            results = searcher.search(query, limit=top_k, sortedby="price")
            return self._format_results(results, "Range")

    def search_phrase(self, phrase: str, field: str = "card_name", top_k: int = 10) -> List[Dict]:
        if not self.ix:
            if not self.open_index():
                return []

        logger.info(f"Phrase query: \"{phrase}\" in {field}")

        words = phrase.lower().split()
        query = Phrase(field, words)

        with self.ix.searcher(weighting=scoring.BM25F()) as searcher:
            results = searcher.search(query, limit=top_k)
            return self._format_results(results, "Phrase")

    def search_fuzzy(self, term: str, field: str = "pokemon", max_dist: int = 2, top_k: int = 10) -> List[Dict]:
        if not self.ix:
            if not self.open_index():
                return []

        logger.info(f"Fuzzy query: {term}~{max_dist} in {field}")

        query = FuzzyTerm(field, term, maxdist=max_dist)

        with self.ix.searcher(weighting=scoring.BM25F()) as searcher:
            results = searcher.search(query, limit=top_k)
            return self._format_results(results, "Fuzzy")

    def search_combined(self, query_str: str, top_k: int = 10) -> List[Dict]:
        if not self.ix:
            if not self.open_index():
                return []

        logger.info(f"Combined query: {query_str}")

        parser = MultifieldParser(
            ["content", "card_name", "pokemon", "card_set", "wiki_page"],
            schema=self.schema
        )

        try:
            query = parser.parse(query_str)
            with self.ix.searcher(weighting=scoring.BM25F()) as searcher:
                results = searcher.search(query, limit=top_k)
                return self._format_results(results, "Combined")
        except Exception as e:
            logger.error(f"Combined query error: {e}")
            return []

    def _format_results(self, results, query_type: str) -> List[Dict]:
        formatted = []
        for hit in results:
            formatted.append({
                "score": hit.score,
                "query_type": query_type,
                "card_name": hit.get("card_name", ""),
                "pokemon": hit.get("pokemon", ""),
                "card_set": hit.get("card_set", ""),
                "card_id": hit.get("card_id", ""),
                "rarity": hit.get("rarity", ""),
                "price": hit.get("price", 0.0),
                "image_url": hit.get("image_url", ""),
                "wiki_page": hit.get("wiki_page", "")
            })
        return formatted

    def get_statistics(self) -> Dict:
        if not self.ix:
            if not self.open_index():
                return {}

        with self.ix.searcher() as searcher:
            return {
                "total_documents": self.ix.doc_count(),
                "schema_fields": list(self.schema.names()),
                "index_path": str(self.index_dir)
            }


def demo_queries():
    indexer = LuceneStyleIndexer()

    if not indexer.open_index():
        logger.error("Index not found. Building index first...")
        indexer.build_index()

    print("\n" + "=" * 80)
    print("QUERY DEMONSTRATIONS")
    print("=" * 80)

    print("\n--- Boolean AND Query: 'pikachu AND 151' ---")
    results = indexer.search_boolean("pikachu AND 151", top_k=5)
    for r in results[:3]:
        print(f"  {r['card_name']} ({r['card_set']}) - ${r['price']}")

    print("\n--- Boolean OR Query: 'pikachu OR charizard' ---")
    results = indexer.search_boolean("pikachu OR charizard", top_k=5)
    for r in results[:3]:
        print(f"  {r['card_name']} ({r['card_set']}) - ${r['price']}")

    print("\n--- Range Query: price $5.00 - $50.00 ---")
    results = indexer.search_range(5.0, 50.0, top_k=5)
    for r in results[:3]:
        print(f"  {r['card_name']} ({r['card_set']}) - ${r['price']}")

    print("\n--- Phrase Query: 'Reverse Holo' ---")
    results = indexer.search_phrase("reverse holo", field="card_name", top_k=5)
    for r in results[:3]:
        print(f"  {r['card_name']} ({r['card_set']}) - ${r['price']}")

    print("\n--- Fuzzy Query: 'pikacu' (with typo) ---")
    results = indexer.search_fuzzy("pikacu", field="pokemon", max_dist=2, top_k=5)
    for r in results[:3]:
        print(f"  {r['card_name']} ({r['card_set']}) - ${r['price']}")

    print("\n--- Combined Query: 'pokemon:pikachu AND card_set:151' ---")
    results = indexer.search_combined("pokemon:pikachu AND card_set:151", top_k=5)
    for r in results[:3]:
        print(f"  {r['card_name']} ({r['card_set']}) - ${r['price']}")

    print("\n" + "=" * 80)
    print("Statistics:", indexer.get_statistics())
    print("=" * 80)


if __name__ == "__main__":
    indexer = LuceneStyleIndexer()
    indexer.build_index()
    demo_queries()
