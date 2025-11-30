import unittest
import re
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import REGEX


class ExpectedCardOutput:
    BASIC_CARD = {
        "name": "Pikachu",
        "image": "https://pokemoncardimages.s3.us-east-2.amazonaws.com/images/Paldean+Fates/131.webp",
        "id": "131",
        "price": "$49.13",
        "set": "Paldean Fates"
    }
    
    CARD_WITH_RARITY = {
        "name": "Paldean Wooper Reverse Holo",
        "image": "https://pokemoncardimages.s3.us-east-2.amazonaws.com/images/Paldean+Fates/058r.webp",
        "id": "058",
        "price": "$0.13",
        "set": "Paldean Fates"
    }
    
    CARD_WITH_SPECIAL_CHARS = {
        "name": "Farfetch'd",
        "image": "https://pokemoncardimages.s3.us-east-2.amazonaws.com/images/Boundaries+Crossed/107.webp",
        "id": "107",
        "price": "$0.17",
        "set": "Boundaries Crossed"
    }
    
    CARD_WITH_TYPE = {
        "name": "Wigglytuff ex",
        "image": "https://pokemoncardimages.s3.us-east-2.amazonaws.com/images/Pokemon+Card+151_english/187.webp",
        "id": "187",
        "price": "$9.11",
        "set": "Pokemon Card 151"
    }


class TestCardRegex(unittest.TestCase):
    def setUp(self):
        self.regexes = REGEX
        self.test_data_dir = Path(__file__).parent / "fixtures" / "cards"
        self.test_data_dir.mkdir(parents=True, exist_ok=True)
    
    def _load_html_fixture(self, filename: str) -> str:
        fixture_path = self.test_data_dir / filename
        if not fixture_path.exists():
            self.skipTest(f"Fixture file not found: {fixture_path}")
        
        return fixture_path.read_text(encoding="utf-8")
    
    def _extract_card_data(self, html: str) -> dict:
        name_match = re.search(self.regexes["CARD_NAME"], html)
        image_match = re.search(self.regexes["CARD_IMAGE"], html)
        id_match = re.search(self.regexes["CARD_ID"], html)
        price_match = re.findall(self.regexes["CARD_PRICE"], html)
        set_match = re.search(self.regexes["CARD_SET"], html)
        
        return {
            "name": name_match.group(1) if name_match else None,
            "image": image_match.group(1) if image_match else None,
            "id": id_match.group(1) if id_match else None,
            "price": price_match[-1] if price_match else None,
            "set": set_match.group(2) if set_match and set_match.lastindex >= 2 else None
        }
    
    def test_basic_card_extraction(self):
        html = self._load_html_fixture("basic_card.html")
        result = self._extract_card_data(html)
        expected = ExpectedCardOutput.BASIC_CARD
        
        self.assertEqual(result["name"], expected["name"], "Card name should match")
        self.assertEqual(result["image"], expected["image"], "Card image URL should match")
        self.assertEqual(result["id"], expected["id"], "Card ID should match")
        self.assertEqual(result["price"], expected["price"], "Card price should match")
        self.assertEqual(result["set"], expected["set"], "Card set should match")
    
    def test_card_with_rarity(self):
        html = self._load_html_fixture("card_with_rarity.html")
        result = self._extract_card_data(html)
        expected = ExpectedCardOutput.CARD_WITH_RARITY
        
        self.assertEqual(result["name"], expected["name"], "Card name with rarity should match")
        self.assertIsNotNone(result["name"], "Card name should not be None")
    
    def test_card_with_special_characters(self):
        html = self._load_html_fixture("card_with_special_chars.html")
        result = self._extract_card_data(html)
        expected = ExpectedCardOutput.CARD_WITH_SPECIAL_CHARS
        
        self.assertEqual(result["name"], expected["name"], "Card name with special chars should match")
        self.assertIn("'", result["name"] or "", "Card name should preserve apostrophes")
    
    def test_card_with_type_suffix(self):
        html = self._load_html_fixture("card_with_type.html")
        result = self._extract_card_data(html)
        expected = ExpectedCardOutput.CARD_WITH_TYPE
        
        self.assertEqual(result["name"], expected["name"], "Card name with type should match")
        self.assertIsNotNone(result["name"], "Card name should not be None")
    
    def test_card_name_regex_pattern(self):
        pattern = self.regexes["CARD_NAME"]
        
        test_cases = [
            ('<span class="MuiTypography-root MuiTypography-avenir_32_700 mui-style-u6codg">Charizard</span>', "Charizard"),
            ('<span class="avenir_32_700">Pikachu V</span>', "Pikachu V"),
            ('<span class="MuiTypography-avenir_32_700">N\'s Plan</span>', "N's Plan"),
        ]
        
        for html, expected in test_cases:
            with self.subTest(html=html):
                match = re.search(pattern, html)
                self.assertIsNotNone(match, f"Pattern should match: {html}")
                if match:
                    self.assertEqual(match.group(1).strip(), expected, f"Should extract: {expected}")
    
    def test_card_image_regex_pattern(self):
        pattern = self.regexes["CARD_IMAGE"]
        
        test_html = '<img class="MuiBox-root css-abc123" alt="Card" src="https://example.com/image.webp" />'
        match = re.search(pattern, test_html)
        
        self.assertIsNotNone(match, "Pattern should match image tag")
        if match:
            self.assertEqual(match.group(1), "https://example.com/image.webp", "Should extract image URL")
    
    def test_card_id_regex_pattern(self):
        pattern = self.regexes["CARD_ID"]
        
        test_cases = [
            ('<span>4</span>', "4"),
            ('<span class="test">25</span>', "25"),
            ('<span>  123  </span>', "123"),
        ]
        
        for html, expected in test_cases:
            with self.subTest(html=html):
                match = re.search(pattern, html)
                self.assertIsNotNone(match, f"Pattern should match: {html}")
                if match:
                    self.assertEqual(match.group(1), expected, f"Should extract ID: {expected}")
    
    def test_card_price_regex_pattern(self):
        pattern = self.regexes["CARD_PRICE"]
        
        test_cases = [
            ('<span>$20.53</span>', "20.53"),
            ('<span>$450.00</span>', "450.00"),
            ('<span>$1.99</span>', "1.99"),
        ]
        
        for html, expected in test_cases:
            with self.subTest(html=html):
                match = re.search(pattern, html)
                self.assertIsNotNone(match, f"Pattern should match: {html}")
                if match:
                    self.assertEqual(match.group(1), expected, f"Should extract price: {expected}")
    
    def test_card_set_regex_pattern(self):
        pattern = self.regexes["CARD_SET"]
        
        test_html = '<a href="/set/Base+Set"><span>Base Set</span></a>'
        match = re.search(pattern, test_html)
        
        self.assertIsNotNone(match, "Pattern should match set link")
        if match:
            self.assertEqual(match.group(2), "Base Set", "Should extract set name from group 2")
    
    def test_missing_fields_handling(self):
        html = "<div>No card data here</div>"
        result = self._extract_card_data(html)
        
        self.assertIsNone(result["name"], "Missing name should be None")
        self.assertIsNone(result["image"], "Missing image should be None")
        self.assertIsNone(result["id"], "Missing ID should be None")
        self.assertIsNone(result["price"], "Missing price should be None")
        self.assertIsNone(result["set"], "Missing set should be None")


if __name__ == "__main__":
    unittest.main()
