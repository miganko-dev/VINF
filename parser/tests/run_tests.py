"""
Test runner script for parser regex tests.

Usage:
    python run_tests.py              # Run all tests
    python run_tests.py -v           # Verbose output
    python run_tests.py card         # Run only card tests
    python run_tests.py set          # Run only set tests
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def run_all_tests(verbosity=2):
    loader = unittest.TestLoader()
    suite = loader.discover(str(Path(__file__).parent), pattern="test_*.py")
    runner = unittest.TextTestRunner(verbosity=verbosity)
    result = runner.run(suite)
    return result.wasSuccessful()


def run_card_tests(verbosity=2):
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromName("test_card_regex")
    runner = unittest.TextTestRunner(verbosity=verbosity)
    result = runner.run(suite)
    return result.wasSuccessful()


def run_set_tests(verbosity=2):
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromName("test_set_regex")
    runner = unittest.TextTestRunner(verbosity=verbosity)
    result = runner.run(suite)
    return result.wasSuccessful()


if __name__ == "__main__":
    args = sys.argv[1:]
    
    verbosity = 2
    if "-v" in args or "--verbose" in args:
        verbosity = 2
        args = [a for a in args if a not in ["-v", "--verbose"]]
    elif "-q" in args or "--quiet" in args:
        verbosity = 0
        args = [a for a in args if a not in ["-q", "--quiet"]]
    
    if not args or "all" in args:
        print("Running all parser tests...")
        success = run_all_tests(verbosity)
    elif "card" in args or "cards" in args:
        print("Running card regex tests...")
        success = run_card_tests(verbosity)
    elif "set" in args or "sets" in args:
        print("Running set regex tests...")
        success = run_set_tests(verbosity)
    else:
        print(__doc__)
        sys.exit(1)
    
    sys.exit(0 if success else 1)
