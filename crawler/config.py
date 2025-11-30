from pathlib import Path
BROWSER_HEADLESS = True
BROWSER_TIMEOUT = 180000
REQUEST_DELAY_MIN = 20
REQUEST_DELAY_MAX = 35
DATA_DIR = Path("data")
USER_AGENTS_FILE = DATA_DIR / "user-agents" / "agents.json"
CRAWLER_LOG_DIR = Path("crawler", "data")
CRAWLER_LOG_FILE = CRAWLER_LOG_DIR / "crawler.log"
LOG_ROTATION = "10 MB"
MAX_PAGES = 10
REGEX = {
    "DOMAIN": r'^(?:https?:\/\/)?(?:www\.)?([^.\/:]+)\.[a-zA-Z]{2,}(?:\.[a-zA-Z]{2,})?',
    "HREF": r'href\s*=\s*["\']([^"\']+)["\']',
    "SRC": r'src\s*=\s*["\']([^"\']+)["\']',
    "CARD_NAME": r'<span class="MuiTypography-root[^"]*">([^<]*)<\/span>',
    "CARD_ID": r'<span class="MuiTypography-root[^"]*">([^<]*)<\/span>'
}