import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger


def setup_logging(verbose: bool = False) -> None:
    logger.remove()
    logger.add(sys.stderr, format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
               level="DEBUG" if verbose else "INFO")


def cmd_spark(args: argparse.Namespace) -> int:
    from wiki_parser.core.spark_job import WikiSparkProcessor
    logger.info("=" * 80)
    logger.info("WIKIPEDIA POKEMON FILTER - SPARK JOB")
    logger.info("=" * 80)
    result = WikiSparkProcessor().run(wait_for_user=not args.no_wait)
    logger.info(f"Processed {result.total_pages} pages")
    return 0


def cmd_extract(args: argparse.Namespace) -> int:
    from wiki_parser.core.wiki_info_extractor import run_extraction
    logger.info("=" * 80)
    logger.info("WIKI INFO EXTRACTOR")
    logger.info("=" * 80)
    run_extraction()
    return 0


def cmd_join(args: argparse.Namespace) -> int:
    from wiki_parser.core.join_cards_wiki import CardWikiJoiner, WikiToPokemonJoiner
    logger.info("=" * 80)
    logger.info("CARD-WIKI JOIN")
    logger.info("=" * 80)
    stats = CardWikiJoiner().run()
    logger.info(f"Matched {stats.pokemon_with_wiki} Pokemon with wiki pages")
    if not args.skip_reverse:
        print("\n" + "=" * 80 + "\n")
        wiki_stats = WikiToPokemonJoiner().run()
        logger.info(f"Matched {wiki_stats['wiki_with_pokemon']} wiki pages with Pokemon")
    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    from wiki_parser.core.statistics import StatisticsGenerator
    logger.info("=" * 80)
    logger.info("STATISTICS GENERATOR")
    logger.info("=" * 80)
    StatisticsGenerator().generate_all()
    logger.info("Statistics generated successfully")
    return 0


def cmd_all(args: argparse.Namespace) -> int:
    logger.info("=" * 80)
    logger.info("RUNNING ALL STEPS")
    logger.info("=" * 80)
    for i, (name, func) in enumerate([("Spark Job", lambda: cmd_spark(args)), ("Extract Info", lambda: cmd_extract(args)),
                                       ("Join", lambda: cmd_join(args)), ("Statistics", lambda: cmd_stats(args))], 1):
        logger.info(f"\n{'=' * 80}\nSTEP {i}/4: {name}\n{'=' * 80}")
        try:
            func()
        except Exception as e:
            logger.error(f"Step '{name}' failed: {e}")
            if not args.continue_on_error:
                return 1
    logger.info("\n" + "=" * 80 + "\nALL STEPS COMPLETED\n" + "=" * 80)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="wiki_parser", description="Pokemon Wiki Parser")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    subparsers = parser.add_subparsers(title="commands", dest="command", help="Available commands")

    spark_parser = subparsers.add_parser("spark", help="Run Spark job to filter Wikipedia pages")
    spark_parser.add_argument("--no-wait", action="store_true", help="Don't wait for user input")
    spark_parser.set_defaults(func=cmd_spark)

    extract_parser = subparsers.add_parser("extract", help="Extract Pokemon info from wiki pages")
    extract_parser.set_defaults(func=cmd_extract)

    join_parser = subparsers.add_parser("join", help="Join cards with wiki pages")
    join_parser.add_argument("--skip-reverse", action="store_true", help="Skip wiki-to-pokemon reverse join")
    join_parser.set_defaults(func=cmd_join)

    stats_parser = subparsers.add_parser("stats", help="Generate comprehensive statistics")
    stats_parser.set_defaults(func=cmd_stats)

    all_parser = subparsers.add_parser("all", help="Run all steps in sequence")
    all_parser.add_argument("--no-wait", action="store_true", help="Don't wait for user input in Spark step")
    all_parser.add_argument("--skip-reverse", action="store_true", help="Skip wiki-to-pokemon reverse join")
    all_parser.add_argument("--continue-on-error", action="store_true", help="Continue on error")
    all_parser.set_defaults(func=cmd_all)

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
