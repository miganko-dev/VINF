import asyncio
from loguru import logger
from core.crawler import Crawler
from utils.link_manager import LinkExtractor
async def main():
    crawler = Crawler("https://houseofvara.co.uk")

    await crawler.run()
if __name__ == "__main__":
    asyncio.run(main())