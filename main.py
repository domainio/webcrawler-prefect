#!/usr/bin/env python3

import asyncio
from src.flows.crawler_flow import crawler_flow

async def run_crawler():
    """Entry point for the crawler."""
    # Start crawling from Python.org
    start_url = "https://python.org"
    await crawler_flow([start_url], max_depth=2)

if __name__ == "__main__":
    asyncio.run(run_crawler())
