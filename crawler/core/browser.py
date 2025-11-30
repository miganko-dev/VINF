import asyncio
import random
from typing import Set
from loguru import logger
from pathlib import Path
from datetime import datetime
from utils.file_manager import FileManager
from playwright.async_api import async_playwright, Browser as PlaywrightBrowser
from config import (
    BROWSER_HEADLESS,
    BROWSER_TIMEOUT,
    REQUEST_DELAY_MIN,
    REQUEST_DELAY_MAX
)
class Browser:
    def __init__(self) -> None:
        self.browser = None
        self.playwright = None
        self.timeout: int = BROWSER_TIMEOUT
        self.user_agents: Set[str] = set()
        self.is_running: bool = False
    async def start(self) -> None:
        if self.is_running:
            logger.warning("Browser is already running")
            return
        if len(self.user_agents) == 0:
            self.user_agents = self.load_user_agents()
        logger.info("Starting browser")
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=BROWSER_HEADLESS)
        self.is_running = True
        await asyncio.sleep(random.uniform(2, 5))
        logger.info("Browser started")
    async def stop(self) -> None:
        if not self.is_running:
            return
        logger.info("Stopping browser")
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
            self.playwright = None
        self.is_running = False
        logger.info("Browser stopped")
    def load_user_agents(self) -> None:
        from config import USER_AGENTS_FILE
        agents = FileManager.load_json(USER_AGENTS_FILE)
        if agents:
            return set(agents) if isinstance(agents, list) else agents
        logger.warning("No user agents loaded, using default")
        return set()
    async def scrape_page(self, link: str, save_path = None) -> str:
        if not self.is_running or self.browser is None:
            raise RuntimeError("Browser is not running. Call start() first.")
        out_file = Path("data/state.json")
        logger.info(f"Scraping: {link}")
        context = await self.browser.new_context(
            user_agent=random.choice(list(self.user_agents)),
            viewport={"width": 1280, "height": 800}
        )
        page = await context.new_page()
        try:
            await page.goto(link, timeout=self.timeout, wait_until='load')
            await self.simulate_human_behavior(page)
            content = await page.content()
            return {
                "url": link,
                "path": FileManager.save_html(content, save_path, link) if save_path else None,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "content": content
            }
        except Exception as e:
            logger.error(f"Error scraping {link}: {e}")
            return None
        finally:
            await context.storage_state(path=str(out_file))
            await context.close()
    async def simulate_human_behavior(self, page):
        await asyncio.sleep(random.uniform(1, 3))
        total_height = await page.evaluate("() => document.body.scrollHeight")
        scroll_steps = random.randint(5, 10)
        for i in range(scroll_steps):
            scroll_y = (total_height / scroll_steps) * (i + 1)
            await page.evaluate(f"window.scrollTo(0, {scroll_y});")
            await asyncio.sleep(random.uniform(0.5, 2.0))
        if random.random() > 0.5:
            await page.evaluate("window.scrollTo(0, 0);")
            await asyncio.sleep(random.uniform(0.5, 1.5))
        if random.random() > 0.3:
            await self.random_mouse_movements(page)
        await asyncio.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
    async def random_mouse_movements(self, page, steps=10):
        width, height = 1280, 800
        for _ in range(steps):
            x = random.randint(0, width)
            y = random.randint(0, height)
            await page.mouse.move(x, y, steps=random.randint(2, 10))
            await asyncio.sleep(random.uniform(0.1, 0.4))