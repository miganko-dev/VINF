import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger
from indexer.config import ensure_directories, LUCENE_INDEX_DIR, INDEX_DATA_DIR


def setup_logging(verbose: bool = False) -> None:
    logger.remove()
    level = "DEBUG" if verbose else "INFO"
    logger.add(
        sys.stderr,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
        level=level
    )


def cmd_build(args: argparse.Namespace) -> int:
    from indexer.core.lucene_indexer import LuceneStyleIndexer

    ensure_directories()

    logger.info("=" * 60)
    logger.info("BUILDING SEARCH INDEX")
    logger.info("=" * 60)

    lucene_indexer = LuceneStyleIndexer()
    lucene_indexer.build_index(use_joined_data=not args.no_wiki)
    lucene_stats = lucene_indexer.get_statistics()
    logger.info(f"Index: {lucene_stats.get('total_documents', 0)} documents")

    logger.info("=" * 60)
    logger.info("INDEX BUILD COMPLETE")
    logger.info("=" * 60)

    return 0


def cmd_search(args: argparse.Namespace) -> int:
    from indexer.core.lucene_indexer import LuceneStyleIndexer

    indexer = LuceneStyleIndexer()
    if not indexer.open_index():
        logger.error("Index not found. Run 'build' command first.")
        return 1

    query = args.query
    top_k = args.limit

    logger.info(f"Searching for: {query}")
    logger.info("-" * 40)

    if args.fuzzy:
        results = indexer.search_fuzzy(query, field=args.field, max_dist=args.fuzzy, top_k=top_k)
    elif args.phrase:
        results = indexer.search_phrase(query, field=args.field, top_k=top_k)
    elif args.price_min is not None or args.price_max is not None:
        results = indexer.search_range(
            args.price_min or 0,
            args.price_max or 999999,
            top_k=top_k
        )
    else:
        results = indexer.search_combined(query, top_k=top_k)

    if not results:
        logger.info("No results found.")
        return 0

    logger.info(f"Found {len(results)} results:\n")

    for i, r in enumerate(results, 1):
        price = f"${r.get('price', 0):.2f}" if r.get('price') else "N/A"
        print(f"{i}. {r.get('card_name', 'Unknown')}")
        print(f"   Pokemon: {r.get('pokemon', 'N/A')}")
        print(f"   Set: {r.get('card_set', 'N/A')}")
        print(f"   Rarity: {r.get('rarity', 'N/A')}")
        print(f"   Price: {price}")
        print(f"   Score: {r.get('score', 0):.4f}")
        if r.get('wiki_page'):
            print(f"   Wiki: {r.get('wiki_page')}")
        print()

    return 0


def cmd_compare(args: argparse.Namespace) -> int:
    logger.info("Compare command not available")
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    from indexer.core.lucene_indexer import LuceneStyleIndexer

    indexer = LuceneStyleIndexer()
    if not indexer.open_index():
        logger.error("Index not found. Run 'build' command first.")
        return 1

    print("\n" + "=" * 80)
    print("QUERY DEMONSTRATIONS")
    print("=" * 80)

    print("\n--- Boolean AND: 'pikachu AND 151' ---")
    results = indexer.search_boolean("pikachu AND 151", top_k=5)
    for r in results[:3]:
        print(f"  {r['card_name']} ({r['card_set']}) - ${r['price']}")

    print("\n--- Boolean OR: 'pikachu OR charizard' ---")
    results = indexer.search_boolean("pikachu OR charizard", top_k=5)
    for r in results[:3]:
        print(f"  {r['card_name']} ({r['card_set']}) - ${r['price']}")

    print("\n--- Range: price $5-$50 ---")
    results = indexer.search_range(5.0, 50.0, top_k=5)
    for r in results[:3]:
        print(f"  {r['card_name']} ({r['card_set']}) - ${r['price']}")

    print("\n--- Phrase: 'reverse holo' ---")
    results = indexer.search_phrase("reverse holo", field="card_name", top_k=5)
    for r in results[:3]:
        print(f"  {r['card_name']} ({r['card_set']}) - ${r['price']}")

    print("\n--- Fuzzy: 'pikacu' (with typo) ---")
    results = indexer.search_fuzzy("pikacu", field="pokemon", max_dist=2, top_k=5)
    for r in results[:3]:
        print(f"  {r['card_name']} ({r['card_set']}) - ${r['price']}")

    print("\n--- Combined: 'pokemon:pikachu AND card_set:151' ---")
    results = indexer.search_combined("pokemon:pikachu AND card_set:151", top_k=5)
    for r in results[:3]:
        print(f"  {r['card_name']} ({r['card_set']}) - ${r['price']}")

    print("\n" + "=" * 80)
    print("Statistics:", indexer.get_statistics())
    print("=" * 80)

    return 0


def cmd_gui(args: argparse.Namespace) -> int:
    logger.info("Launching search GUI...")
    from indexer.lucene_gui import main as lucene_gui
    lucene_gui()
    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    from indexer.core.lucene_indexer import LuceneStyleIndexer

    indexer = LuceneStyleIndexer()
    if not indexer.open_index():
        logger.error("Index not found. Run 'build' command first.")
        return 1

    stats = indexer.get_statistics()

    print("\n" + "=" * 60)
    print("INDEX STATISTICS")
    print("=" * 60)
    print(f"Total documents: {stats.get('total_documents', 0):,}")
    print(f"Index path: {stats.get('index_path', 'N/A')}")
    print(f"Schema fields: {', '.join(stats.get('schema_fields', []))}")
    print("=" * 60)

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="indexer",
        description="Pokemon Card Search Indexer",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")

    subparsers = parser.add_subparsers(title="commands", dest="command", help="Available commands")

    build_parser = subparsers.add_parser("build", help="Build search index")
    build_parser.add_argument("--no-wiki", action="store_true", help="Don't include wiki data")
    build_parser.set_defaults(func=cmd_build)

    search_parser = subparsers.add_parser("search", help="Search the index")
    search_parser.add_argument("query", nargs="?", default="", help="Search query")
    search_parser.add_argument("-l", "--limit", type=int, default=10, help="Maximum results")
    search_parser.add_argument("-f", "--field", default="pokemon", help="Field to search")
    search_parser.add_argument("--fuzzy", type=int, metavar="DIST", help="Fuzzy search distance")
    search_parser.add_argument("--phrase", action="store_true", help="Exact phrase search")
    search_parser.add_argument("--price-min", type=float, help="Minimum price filter")
    search_parser.add_argument("--price-max", type=float, help="Maximum price filter")
    search_parser.set_defaults(func=cmd_search)

    compare_parser = subparsers.add_parser("compare", help="Compare search results")
    compare_parser.set_defaults(func=cmd_compare)

    demo_parser = subparsers.add_parser("demo", help="Run query demonstrations")
    demo_parser.set_defaults(func=cmd_demo)

    gui_parser = subparsers.add_parser("gui", help="Launch search GUI")
    gui_parser.set_defaults(func=cmd_gui)

    stats_parser = subparsers.add_parser("stats", help="Show index statistics")
    stats_parser.set_defaults(func=cmd_stats)

    args = parser.parse_args()
    setup_logging(args.verbose)

    if not args.command:
        parser.print_help()
        return 0

    try:
        return args.func(args)
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user")
        return 130
    except Exception as e:
        logger.error(f"Error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
