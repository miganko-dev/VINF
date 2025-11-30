from typing import Dict
from loguru import logger
import gc
import asyncio
from core.browser import Browser
from utils.url_manager import extract_domain
from utils.file_manager import FileManager
from core.robots import RobotsParser
from utils.link_manager import LinkExtractor
from config import (
    CRAWLER_LOG_DIR,
    CRAWLER_LOG_FILE,
    LOG_ROTATION,
    DATA_DIR,
    MAX_PAGES
)
class Crawler:
    def __init__(self, root_url: str):
        self.root_url = root_url
        self.domain = extract_domain(self.root_url)
        self.browser = None
        self.setup_logger()
        self.initialize_storage()
        self.link_extractor = LinkExtractor(self.root_url, set())
    def setup_logger(self) -> None:
        FileManager.directory(CRAWLER_LOG_DIR)
        logger.add(CRAWLER_LOG_FILE, rotation=LOG_ROTATION)
    def initialize_storage(self) -> None:
        self.save_dir = DATA_DIR / self.domain
        self.metadata_dir = self.save_dir / "metadata"
        self.html_dir = self.save_dir / "html"
        FileManager.directory(self.save_dir)
        FileManager.directory(self.metadata_dir)
        FileManager.directory(self.html_dir)
        self.links_file = self.metadata_dir / "links.json"
        FileManager.directory(self.links_file)
        self.links: Dict[str, Dict[str, bool]] = FileManager.load_json(self.links_file) or {}
        if self.root_url not in self.links:
            self.links[self.root_url] = {"visited": False}
            self.save_data()
        logger.info(f"Storage initialized at {self.save_dir}")
    async def get_robots(self) -> None:
        if not self.browser or not self.browser.is_running:
            logger.warning("Browser is not initialized or not started")
            await self.initialize()
        robots_url = self.root_url + "/robots.txt"
        content = None
        while not content:
            content = await self.browser.scrape_page(robots_url)
            if content:
                self.disallow = RobotsParser.parse(content=content["content"])
                self.link_extractor = LinkExtractor(self.root_url, self.disallow)
                logger.info(f"Found {len(self.disallow)} disallow rules: {self.disallow}")
    async def initialize(self) -> None:
        if self.browser:
            await self.browser.stop()
        self.browser = Browser()
        await self.browser.start()
    def get_url(self, pages) -> str:
        links = []
        for url, meta in self.links.items():
            if not meta.get("visited", False):
                links.append(url)
                if len(links) == pages:
                    break
        return links
    def add_to_visit(self, url: str) -> bool:
        if url in self.links:
            return
        if "&amp" in url:
            return 
        self.links[url] = {"visited": False}
    async def run(self) -> None:
        logger.info(f"Starting to crawl website {self.root_url}")
        links_to_crawl = self.get_url(MAX_PAGES)
        while len(links_to_crawl) > 0:
            await self.initialize()
            results = await asyncio.gather(*(self.browser.scrape_page(url, self.html_dir) for url in links_to_crawl))
            links = set()
            successfully_crawled = 0
            for result in results:
                if result: 
                    successfully_crawled += 1
                    url = result["url"]
                    path = result["path"]
                    timestamp = result["timestamp"]
                    extracted_links = list(self.link_extractor.extract_links(result["content"]))
                    links.update(extracted_links)
                    self.links[url].update({
                        "path": path,
                        "timestamp": timestamp,
                        "visited": True,
                    })
            logger.info(f"Successfully crawled {successfully_crawled}/{len(links_to_crawl)}")
            for link in links:
                self.add_to_visit(link)
            links_to_crawl = self.get_url(MAX_PAGES)
            self.save_data()
            gc.collect()
    def save_data(self):
        FileManager.save_json(self.links, self.links_file)