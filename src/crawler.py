from prefect import task
import aiohttp
import asyncio
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from typing import Set, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@task
async def extract_links(session: aiohttp.ClientSession, url: str) -> Set[str]:
    """Extract all valid links from a given URL."""
    try:
        async with session.get(url) as response:
            if response.status != 200:
                logger.warning(f"Failed to fetch {url}, status: {response.status}")
                return set()
            
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            links = set()
            
            base_domain = urlparse(url).netloc
            for link in soup.find_all('a'):
                href = link.get('href')
                if href:
                    absolute_url = urljoin(url, href)
                    # Only include links from the same domain
                    if urlparse(absolute_url).netloc == base_domain:
                        links.add(absolute_url)
            
            logger.info(f"Found {len(links)} valid links on {url}")
            return links
    except Exception as e:
        logger.error(f"Error processing {url}: {str(e)}")
        return set()

@task
async def process_depth(
    session: aiohttp.ClientSession,
    urls: Set[str],
    visited: Dict[str, int],
    current_depth: int,
    max_depth: int
) -> Set[str]:
    """Process all URLs at the current depth in parallel."""
    if current_depth >= max_depth:
        return set()

    tasks = []
    for url in urls:
        if url not in visited or visited[url] > current_depth:
            visited[url] = current_depth
            tasks.append(extract_links(session, url))

    if not tasks:
        return set()

    results = await asyncio.gather(*tasks)
    return set().union(*results)

@task
async def crawl_website(start_url: str, max_depth: int = 3) -> Dict[str, int]:
    """
    Task to crawl a website starting from a given URL up to a maximum depth.
    
    Args:
        start_url: The initial URL to start crawling from
        max_depth: Maximum depth to crawl (default: 3)
    
    Returns:
        Dict[str, int]: Dictionary mapping URLs to their crawl depth
    """
    visited: Dict[str, int] = {}  # URL -> depth mapping
    current_urls = {start_url}
    
    async with aiohttp.ClientSession() as session:
        for depth in range(max_depth):
            logger.info(f"Processing depth {depth}")
            # Process current depth
            new_urls = await process_depth(
                session=session,
                urls=current_urls,
                visited=visited,
                current_depth=depth,
                max_depth=max_depth
            )
            
            if not new_urls:
                logger.info(f"No more URLs to process at depth {depth}")
                break
                
            current_urls = new_urls
            
    logger.info(f"Crawling completed. Total URLs processed: {len(visited)}")
    return visited
