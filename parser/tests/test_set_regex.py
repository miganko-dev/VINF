import unittest
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import REGEX


class ExpectedSetOutput:
    BASIC_SET = {
        "name": "",
        "release": "",
        "series": "",
        "symbol": "",
        "total_cards": ""
    }
    
    SET_WITH_HTML_ENTITIES = {
        "name": "",
        "release": "",
        "series": "",
        "symbol": "",
        "total_cards": ""
    }
    
    SET_WITH_SPECIAL_CHARS = {
        "name": "",
        "release": "",
        "series": "",
        "symbol": "",
        "total_cards": ""
    }
    
    SET_WITH_LARGE_COUNT = {
        "name": "",
        "release": "",
        "series": "",
        "symbol": "",
        "total_cards": ""
    }
    
    CLASSIC_SET = {
        "name": "",
        "release": "",
        "series": "",
        "symbol": "",
        "total_cards": ""
    }


class TestSetRegex(unittest.TestCase):
    def setUp(self):
        self.regexes = REGEX
        self.test_data_dir = Path(__file__).parent / "fixtures" / "sets"
        self.test_data_dir.mkdir(parents=True, exist_ok=True)
    
    def _load_html_fixture(self, filename: str) -> str:
        fixture_path = self.test_data_dir / filename
        if not fixture_path.exists():
            self.skipTest(f"Fixture file not found: {fixture_path}")
        
        return fixture_path.read_text(encoding="utf-8")
    
    def _extract_set_data(self, html: str) -> dict:
        name_match = re.search(self.regexes["SET_NAME"], html)
        release_match = re.search(self.regexes["SET_RELEASE"], html)
        series_symbol_matches = re.findall(self.regexes["SET_SERIES_SYMBOL"], html)
        total_cards_match = re.search(self.regexes["SET_TOTAL_CARDS"], html)
        
        series = series_symbol_matches[0] if len(series_symbol_matches) > 0 else None
        symbol = series_symbol_matches[1] if len(series_symbol_matches) > 1 else None
        
        return {
            "name": name_match.group(1) if name_match else None,
            "release": release_match.group(1) if release_match else None,
            "series": series,
            "symbol": symbol,
            "total_cards": total_cards_match.group(1) if total_cards_match else None
        }
    
    def test_basic_set_extraction(self):
        html = self._load_html_fixture("basic_set.html")
        result = self._extract_set_data(html)
        expected = ExpectedSetOutput.BASIC_SET
        
        self.assertEqual(result["name"], expected["name"], "Set name should match")
        self.assertEqual(result["release"], expected["release"], "Release date should match")
        self.assertEqual(result["series"], expected["series"], "Series should match")
        self.assertEqual(result["symbol"], expected["symbol"], "Symbol should match")
        self.assertEqual(result["total_cards"], expected["total_cards"], "Total cards should match")
    
    def test_set_with_html_entities(self):
        html = self._load_html_fixture("set_with_html_entities.html")
        result = self._extract_set_data(html)
        expected = ExpectedSetOutput.SET_WITH_HTML_ENTITIES
        
        self.assertEqual(result["name"], expected["name"], "Set name with HTML entities should match")
        self.assertIsNotNone(result["name"], "Set name should not be None")
    
    def test_set_with_special_characters(self):
        html = self._load_html_fixture("set_with_special_chars.html")
        result = self._extract_set_data(html)
        expected = ExpectedSetOutput.SET_WITH_SPECIAL_CHARS
        
        self.assertEqual(result["name"], expected["name"], "Set name with special chars should match")
    
    def test_set_with_large_card_count(self):
        html = self._load_html_fixture("set_with_large_count.html")
        result = self._extract_set_data(html)
        expected = ExpectedSetOutput.SET_WITH_LARGE_COUNT
        
        self.assertEqual(result["total_cards"], expected["total_cards"], "Large card count should match")
        self.assertIsNotNone(result["total_cards"], "Total cards should not be None")
        
        if result["total_cards"]:
            card_count = int(result["total_cards"])
            self.assertGreater(card_count, 0, "Card count should be positive")
    
    def test_classic_set(self):
        html = self._load_html_fixture("classic_set.html")
        result = self._extract_set_data(html)
        expected = ExpectedSetOutput.CLASSIC_SET
        
        self.assertEqual(result["name"], expected["name"], "Classic set name should match")
        self.assertIsNotNone(result["name"], "Set name should not be None")
    
    def test_set_name_regex_pattern(self):
        pattern = self.regexes["SET_NAME"]
        
        test_cases = [
            ('<span class="MuiTypography-root MuiTypography-avenir_28_700">Base Set</span>', "Base Set"),
            ('<span class="MuiTypography-avenir_28_700 mui-xyz">Jungle</span>', "Jungle"),
            ('<span class="MuiTypography-avenir_28_700">Scarlet &amp; Violet</span>', "Scarlet &amp; Violet"),
        ]
        
        for html, expected in test_cases:
            with self.subTest(html=html):
                match = re.search(pattern, html)
                self.assertIsNotNone(match, f"Pattern should match: {html}")
                if match:
                    self.assertEqual(match.group(1), expected, f"Should extract: {expected}")
    
    def test_set_release_regex_pattern(self):
        pattern = self.regexes["SET_RELEASE"]
        
        test_cases = [
            ('<span class="MuiTypography-avenir_16_400 mui-style-fczuhl">January 9, 1999</span>', "January 9, 1999"),
            ('<span class="MuiTypography-avenir_16_400 mui-style-fczuhl">March 24, 2023</span>', "March 24, 2023"),
        ]
        
        for html, expected in test_cases:
            with self.subTest(html=html):
                match = re.search(pattern, html)
                self.assertIsNotNone(match, f"Pattern should match: {html}")
                if match:
                    self.assertEqual(match.group(1), expected, f"Should extract: {expected}")
    
    def test_set_series_symbol_regex_pattern(self):
        pattern = self.regexes["SET_SERIES_SYMBOL"]
        
        test_html = '''
        <span class="MuiTypography-avenir_16_400 mui-style-ku8hna">Base</span>
        <span class="MuiTypography-avenir_16_400 mui-style-ku8hna">⚡</span>
        '''
        
        matches = re.findall(pattern, test_html)
        self.assertEqual(len(matches), 2, "Should find exactly 2 matches (series and symbol)")
        self.assertEqual(matches[0], "Base", "First match should be series")
        self.assertEqual(matches[1], "⚡", "Second match should be symbol")
    
    def test_set_total_cards_regex_pattern(self):
        pattern = self.regexes["SET_TOTAL_CARDS"]
        
        test_cases = [
            ('<span class="MuiTypography-avenir_16_400 mui-style-1lkn006">/<!-- --> 102</span>', "102"),
            ('<span class="MuiTypography-avenir_16_400 mui-style-1lkn006">/<!-- --> 300</span>', "300"),
            ('<span class="MuiTypography-avenir_16_400 mui-style-1lkn006">/<!-- --> 25</span>', "25"),
        ]
        
        for html, expected in test_cases:
            with self.subTest(html=html):
                match = re.search(pattern, html)
                self.assertIsNotNone(match, f"Pattern should match: {html}")
                if match:
                    self.assertEqual(match.group(1), expected, f"Should extract: {expected}")
    
    def test_missing_fields_handling(self):
        html = "<div>No set data here</div>"
        result = self._extract_set_data(html)
        
        self.assertIsNone(result["name"], "Missing name should be None")
        self.assertIsNone(result["release"], "Missing release should be None")
        self.assertIsNone(result["series"], "Missing series should be None")
        self.assertIsNone(result["symbol"], "Missing symbol should be None")
        self.assertIsNone(result["total_cards"], "Missing total_cards should be None")
    
    def test_partial_set_data(self):
        html = '<span class="MuiTypography-avenir_28_700">Test Set</span>'
        result = self._extract_set_data(html)
        
        self.assertEqual(result["name"], "Test Set", "Name should be extracted")
        self.assertIsNone(result["release"], "Missing release should be None")
        self.assertIsNone(result["total_cards"], "Missing total_cards should be None")
    
    def test_multiple_series_symbol_spans(self):
        pattern = self.regexes["SET_SERIES_SYMBOL"]
        
        test_html = '''
        <span class="MuiTypography-avenir_16_400 mui-style-ku8hna">Scarlet</span>
        <span class="MuiTypography-avenir_16_400 mui-style-ku8hna">★</span>
        <span class="MuiTypography-avenir_16_400 mui-style-ku8hna">Extra</span>
        '''
        
        matches = re.findall(pattern, test_html)
        self.assertGreaterEqual(len(matches), 2, "Should find at least 2 matches")
        self.assertEqual(matches[0], "Scarlet", "First match should be series")
        self.assertEqual(matches[1], "★", "Second match should be symbol")


if __name__ == "__main__":
    unittest.main()
