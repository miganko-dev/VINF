import re
from config import REGEX
from typing import Set, Pattern
from urllib.parse import urlparse, urljoin

class LinkExtractor:
    def __init__(self, base_url: str, robots: Set[str]):
        self.base_url = base_url
        self.robots = robots
        self.href_regex = re.compile(REGEX["HREF"], re.IGNORECASE)
        self.src_regex = re.compile(REGEX["SRC"], re.IGNORECASE)
        self.blocked_exts = {
            ".js", ".css", ".png", ".jpg", ".jpeg", ".gif", ".svg",
            ".ico", ".pdf", ".zip", ".rar", ".mp4", ".mp3", ".woff",
            ".ttf", ".webp", ".json"
        }

    def extract_links(self, content: str) -> Set[str]:
        links = set()
        href_links = self.extract(self.href_regex, content)
        src_link = self.extract(self.src_regex, content)
        for link in href_links:
            links.add(link)
        for link in src_link:
            links.add(link)
        return links
    
    def extract(self, regex: Pattern, content: str) -> Set[str]:
        links = set()
        matches = regex.findall(content)
        for link in matches:
            full_url = urljoin(self.base_url, link)
            parsed = urlparse(full_url)
            if not parsed.scheme.startswith("http"):
                continue
            structure_link = f"{parsed.scheme}://{parsed.netloc}{parsed.path or ''}"
            if any(structure_link.lower().endswith(ext) for ext in self.blocked_exts):
                continue
            if not structure_link.startswith(self.base_url):
                continue
            if structure_link in self.robots:
                continue
            if "&quot" in structure_link or "&amp;":
                continue
            links.add(structure_link)
        return links