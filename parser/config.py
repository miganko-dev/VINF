from pathlib import Path
DATA_DIR = Path("data")
PARSER_LOG_DIR = Path("parser", "data")
PARSER_LOG_FILE = PARSER_LOG_DIR / "parser.log"
LOG_ROTATION = "10 MB"
REGEX = {
    "DOMAIN": r'^(?:https?:\/\/)?(?:www\.)?([^.\/:]+)\.[a-zA-Z]{2,}(?:\.[a-zA-Z]{2,})?',
    "CARD_NAME": r'<span[^>]*MuiTypography-avenir_32_700[^>]*?>\s*([^<]+?)\s*</span>',
    "CARD_IMAGE": r'<img[^>]*class="MuiBox-root[^"]*"[^>]*alt="Card"[^>]*src="([^"]+)"',
    "CARD_ID": r'<span[^>]*MuiTypography-avenir_400_16[^>]*>\s*([^<]+?)\s*</span>',
    "CARD_PRICE": r'<span[^>]*MuiTypography-avenir_24_700[^>]*?>\s*([^<]+?)\s*</span>',
    "CARD_SET": r'<span[^>]*MuiTypography-avenir_16_700[^>]*?>\s*([^<]+?)\s*</span>',
    "SET_NAME": r'<span[^>]*MuiTypography-avenir_28_700[^>]*>([^<]+)</span>',
    "SET_RELEASE": r'<span[^>]*MuiTypography-avenir_16_400[^>]*mui-style-fczuhl[^>]*>([^<]+)</span>',
    "SET_SERIES_SYMBOL": r'<span[^>]*MuiTypography-avenir_16_400[^>]*mui-style-ku8hna[^>]*>([^<]+)</span>',
    "SET_TOTAL_CARDS": r'<span[^>]*MuiTypography-avenir_16_400[^>]*mui-style-1lkn006[^>]*>/<!-- -->\s*([0-9]+)</span>'
}