"""
Wiki Parser - Main Entry Point

This is the main CLI for the Wiki Parser module. It provides commands for:
- spark: Filter Wikipedia dump for Pokemon-related pages
- extract: Extract Pokemon info from wiki pages
- join: Join card data with wiki pages
- stats: Generate comprehensive statistics
- all: Run all steps in sequence

Usage:
    python -m wiki_parser.main spark
    python -m wiki_parser.main join
    python -m wiki_parser.main stats
    python -m wiki_parser.main all
"""

import sys
import argparse
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    logger.remove()
    level = "DEBUG" if verbose else "INFO"
    logger.add(
        sys.stderr,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
        level=level
    )


def cmd_spark(args: argparse.Namespace) -> int:
    """Run Spark job to filter Wikipedia pages."""
    from wiki_parser.core.spark_job import WikiSparkProcessor

    logger.info("=" * 80)
    logger.info("WIKIPEDIA POKEMON FILTER - SPARK JOB")
    logger.info("=" * 80)

    processor = WikiSparkProcessor()
    result = processor.run(wait_for_user=not args.no_wait)

    logger.info(f"Processed {result.total_pages} pages")
    return 0


def cmd_extract(args: argparse.Namespace) -> int:
    """Extract Pokemon info from wiki pages."""
    from wiki_parser.core.wiki_info_extractor import run_extraction

    logger.info("=" * 80)
    logger.info("WIKI INFO EXTRACTOR")
    logger.info("=" * 80)

    run_extraction()
    return 0


def cmd_join(args: argparse.Namespace) -> int:
    """Run join between cards and wiki pages."""
    from wiki_parser.core.join_cards_wiki import CardWikiJoiner, WikiToPokemonJoiner

    logger.info("=" * 80)
    logger.info("CARD-WIKI JOIN")
    logger.info("=" * 80)

    # Card to Wiki join
    joiner = CardWikiJoiner()
    stats = joiner.run()
    logger.info(f"Matched {stats.pokemon_with_wiki} Pokemon with wiki pages")

    # Wiki to Pokemon join (reverse)
    if not args.skip_reverse:
        print("\n" + "=" * 80 + "\n")
        wiki_joiner = WikiToPokemonJoiner()
        wiki_stats = wiki_joiner.run()
        logger.info(f"Matched {wiki_stats['wiki_with_pokemon']} wiki pages with Pokemon")

    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    """Generate comprehensive statistics."""
    from wiki_parser.core.statistics import StatisticsGenerator

    logger.info("=" * 80)
    logger.info("STATISTICS GENERATOR")
    logger.info("=" * 80)

    generator = StatisticsGenerator()
    stats = generator.generate_all()

    logger.info("Statistics generated successfully")
    return 0


def cmd_all(args: argparse.Namespace) -> int:
    """Run all steps in sequence."""
    logger.info("=" * 80)
    logger.info("RUNNING ALL STEPS")
    logger.info("=" * 80)

    steps = [
        ("Spark Job", lambda: cmd_spark(args)),
        ("Extract Info", lambda: cmd_extract(args)),
        ("Join", lambda: cmd_join(args)),
        ("Statistics", lambda: cmd_stats(args))
    ]

    for i, (name, func) in enumerate(steps, 1):
        logger.info(f"\n{'=' * 80}")
        logger.info(f"STEP {i}/{len(steps)}: {name}")
        logger.info("=" * 80)

        try:
            func()
        except Exception as e:
            logger.error(f"Step '{name}' failed: {e}")
            if not args.continue_on_error:
                return 1

    logger.info("\n" + "=" * 80)
    logger.info("ALL STEPS COMPLETED")
    logger.info("=" * 80)
    return 0


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        prog="wiki_parser",
        description="Pokemon Wiki Parser - Process Wikipedia data and join with card data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m wiki_parser.main spark          Filter Wikipedia for Pokemon pages
  python -m wiki_parser.main extract        Extract Pokemon info from wiki pages
  python -m wiki_parser.main join           Join cards with wiki pages
  python -m wiki_parser.main stats          Generate statistics
  python -m wiki_parser.main all            Run all steps

Output directories:
  data/wiki/spark_output/   Filtered wiki pages
  data/wiki/parsed/         Extracted wiki info
  data/joined/              Card-wiki join results
  data/statistics/          Generated statistics
        """
    )

    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )

    subparsers = parser.add_subparsers(
        title="commands",
        dest="command",
        help="Available commands"
    )

    # Spark command
    spark_parser = subparsers.add_parser(
        "spark",
        help="Run Spark job to filter Wikipedia pages"
    )
    spark_parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Don't wait for user input before stopping Spark"
    )
    spark_parser.set_defaults(func=cmd_spark)

    # Extract command
    extract_parser = subparsers.add_parser(
        "extract",
        help="Extract Pokemon info from wiki pages"
    )
    extract_parser.set_defaults(func=cmd_extract)

    # Join command
    join_parser = subparsers.add_parser(
        "join",
        help="Join cards with wiki pages"
    )
    join_parser.add_argument(
        "--skip-reverse",
        action="store_true",
        help="Skip wiki-to-pokemon reverse join"
    )
    join_parser.set_defaults(func=cmd_join)

    # Stats command
    stats_parser = subparsers.add_parser(
        "stats",
        help="Generate comprehensive statistics"
    )
    stats_parser.set_defaults(func=cmd_stats)

    # All command
    all_parser = subparsers.add_parser(
        "all",
        help="Run all steps in sequence"
    )
    all_parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Don't wait for user input in Spark step"
    )
    all_parser.add_argument(
        "--skip-reverse",
        action="store_true",
        help="Skip wiki-to-pokemon reverse join"
    )
    all_parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue to next step even if current step fails"
    )
    all_parser.set_defaults(func=cmd_all)

    # Parse arguments
    args = parser.parse_args()

    # Setup logging
    setup_logging(args.verbose)

    # Show help if no command specified
    if not args.command:
        parser.print_help()
        return 0

    # Run command
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
