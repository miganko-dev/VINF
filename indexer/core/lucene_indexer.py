import json
from typing import List, Dict
from loguru import logger

from whoosh import index
from whoosh.fields import Schema, TEXT, ID, KEYWORD, NUMERIC, STORED
from whoosh.qparser import MultifieldParser
from whoosh.query import FuzzyTerm, Phrase, NumericRange
from whoosh.analysis import StandardAnalyzer, StemmingAnalyzer
from whoosh import scoring

from indexer.config import CARDS_DIR, JOINED_DIR, LUCENE_INDEX_DIR

INDEX_DIR = LUCENE_INDEX_DIR
JOINED_DATA_FILE = JOINED_DIR / "pokemon_with_wiki_and_cards.json"


class LuceneStyleIndexer:
    def __init__(self):
        self.index_dir = INDEX_DIR
        self.ix = None
        self.schema = Schema(
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

        cards = self._load_joined_data() if use_joined_data and JOINED_DATA_FILE.exists() else self._load_card_files()
        if not cards:
            logger.error("No cards to index!")
            return

        writer = self.ix.writer()
        indexed_count = 0

        for card in cards:
            try:
                price_str = str(card.get('price', '0') or '0')
                try:
                    price = float(price_str.replace('$', '').replace(',', ''))
                except ValueError:
                    price = 0.0

                content = ' '.join(filter(None, [
                    card.get('card_name', ''), card.get('pokemon', ''),
                    card.get('set', '') or card.get('card_set', ''),
                    card.get('rarity', ''), card.get('wiki_page', '')
                ]))

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
                indexed_count += 1
            except Exception as e:
                logger.debug(f"Error indexing card: {e}")

        writer.commit()
        logger.info(f"Indexed {indexed_count} cards successfully")

        with open(self.index_dir / "index_stats.json", 'w', encoding='utf-8') as f:
            json.dump({"total_documents": indexed_count, "index_path": str(self.index_dir),
                      "schema_fields": list(self.schema.names())}, f, indent=2)

    def _load_joined_data(self) -> List[Dict]:
        logger.info(f"Loading joined data from {JOINED_DATA_FILE}")
        with open(JOINED_DATA_FILE, 'r', encoding='utf-8') as f:
            pokemon_data = json.load(f)

        cards = []
        for pokemon_entry in pokemon_data:
            wiki_pages = pokemon_entry.get('wiki_pages', [])
            wiki_page = wiki_pages[0] if wiki_pages else ''
            for card in pokemon_entry.get('cards', []):
                cards.append({
                    'card_name': card.get('name', ''), 'pokemon': pokemon_entry.get('pokemon', ''),
                    'card_set': card.get('set', ''), 'card_id': card.get('id', ''),
                    'rarity': card.get('rarity', ''), 'price': card.get('price', '0'),
                    'image_url': card.get('image', ''), 'card_source': card.get('source', ''),
                    'wiki_page': wiki_page
                })
        logger.info(f"Loaded {len(cards)} cards from joined data")
        return cards

    def _load_card_files(self) -> List[Dict]:
        cards = []
        if not CARDS_DIR.exists():
            return cards

        for json_file in CARDS_DIR.glob("*.json"):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    card = json.load(f)
                cards.append({
                    'card_name': card.get('Name', ''), 'pokemon': card.get('Pokemon', ''),
                    'set': card.get('Set', ''), 'card_id': card.get('Id', ''),
                    'rarity': card.get('Rarity', ''), 'price': card.get('Price', '0'),
                    'image_url': card.get('Image', ''), 'card_source': card.get('Source', ''),
                    'wiki_page': None
                })
            except Exception as e:
                logger.debug(f"Error reading {json_file}: {e}")
        logger.info(f"Loaded {len(cards)} cards from files")
        return cards

    def open_index(self) -> bool:
        if not self.index_dir.exists():
            return False
        try:
            self.ix = index.open_dir(str(self.index_dir))
            return True
        except Exception as e:
            logger.error(f"Error opening index: {e}")
            return False

    def search_boolean(self, query_str: str, top_k: int = 10) -> List[Dict]:
        if not self.ix and not self.open_index():
            return []
        logger.info(f"Boolean query: {query_str}")
        parser = MultifieldParser(["content", "card_name", "pokemon", "card_set"], schema=self.schema)
        try:
            with self.ix.searcher(weighting=scoring.BM25F()) as searcher:
                return self._format_results(searcher.search(parser.parse(query_str), limit=top_k), "Boolean AND/OR")
        except Exception as e:
            logger.error(f"Boolean query error: {e}")
            return []

    def search_range(self, min_price: float, max_price: float, top_k: int = 10) -> List[Dict]:
        if not self.ix and not self.open_index():
            return []
        logger.info(f"Range query: price ${min_price} - ${max_price}")
        with self.ix.searcher() as searcher:
            return self._format_results(searcher.search(NumericRange("price", min_price, max_price), limit=top_k, sortedby="price"), "Range")

    def search_phrase(self, phrase: str, field: str = "card_name", top_k: int = 10) -> List[Dict]:
        if not self.ix and not self.open_index():
            return []
        logger.info(f"Phrase query: \"{phrase}\" in {field}")
        with self.ix.searcher(weighting=scoring.BM25F()) as searcher:
            return self._format_results(searcher.search(Phrase(field, phrase.lower().split()), limit=top_k), "Phrase")

    def search_fuzzy(self, term: str, field: str = "pokemon", max_dist: int = 2, top_k: int = 10) -> List[Dict]:
        if not self.ix and not self.open_index():
            return []
        logger.info(f"Fuzzy query: {term}~{max_dist} in {field}")
        with self.ix.searcher(weighting=scoring.BM25F()) as searcher:
            return self._format_results(searcher.search(FuzzyTerm(field, term, maxdist=max_dist), limit=top_k), "Fuzzy")

    def search_combined(self, query_str: str, top_k: int = 10) -> List[Dict]:
        if not self.ix and not self.open_index():
            return []
        logger.info(f"Combined query: {query_str}")
        parser = MultifieldParser(["content", "card_name", "pokemon", "card_set", "wiki_page"], schema=self.schema)
        try:
            with self.ix.searcher(weighting=scoring.BM25F()) as searcher:
                return self._format_results(searcher.search(parser.parse(query_str), limit=top_k), "Combined")
        except Exception as e:
            logger.error(f"Combined query error: {e}")
            return []

    def _format_results(self, results, query_type: str) -> List[Dict]:
        return [{
            "score": hit.score, "query_type": query_type,
            "card_name": hit.get("card_name", ""), "pokemon": hit.get("pokemon", ""),
            "card_set": hit.get("card_set", ""), "card_id": hit.get("card_id", ""),
            "rarity": hit.get("rarity", ""), "price": hit.get("price", 0.0),
            "image_url": hit.get("image_url", ""), "wiki_page": hit.get("wiki_page", "")
        } for hit in results]

    def get_statistics(self) -> Dict:
        if not self.ix and not self.open_index():
            return {}
        with self.ix.searcher() as searcher:
            return {"total_documents": self.ix.doc_count(), "schema_fields": list(self.schema.names()), "index_path": str(self.index_dir)}


if __name__ == "__main__":
    indexer = LuceneStyleIndexer()
    indexer.build_index()
