import time
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from typing import Set, Dict, List, Tuple
from prefect import flow, task, get_run_logger
from dotenv import load_dotenv
import logging

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

@task(retries=3)
async def extract_links(url: str) -> Tuple[Set[str], float]:
    """
    Extract all valid links from a given URL and calculate page rank.
    
    Returns:
        Tuple of (set of valid same-domain links, rank)
        Rank is the ratio of same-domain links to all valid links
    """
    task_logger = get_run_logger()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    task_logger.warning(f"Failed to fetch {url}, status: {response.status}")
                    return set(), 0.0
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                same_domain_links = set()
                all_valid_links = set()
                
                base_domain = urlparse(url).netloc
                for link in soup.find_all('a'):
                    href = link.get('href')
                    if href:
                        try:
                            absolute_url = urljoin(url, href)
                            # Only count URLs we can parse
                            parsed_url = urlparse(absolute_url)
                            if parsed_url.scheme in ('http', 'https'):
                                all_valid_links.add(absolute_url)
                                if parsed_url.netloc == base_domain:
                                    same_domain_links.add(absolute_url)
                        except Exception:
                            continue
                
                # Calculate rank
                rank = len(same_domain_links) / len(all_valid_links) if all_valid_links else 0.0
                
                # Log using Prefect's logger
                task_logger.info("Page Analysis:")
                task_logger.info(f"URL: {url}")
                task_logger.info(f"Same domain links: {len(same_domain_links)}")
                task_logger.info(f"All valid links: {len(all_valid_links)}")
                task_logger.info(f"Rank: {rank:.2f}")
                
                return same_domain_links, rank
    except Exception as e:
        task_logger.error(f"Error processing {url}: {str(e)}")
        return set(), 0.0

@task
async def process_depth(
    urls: Set[str],
    visited: Dict[str, int],
    current_depth: int,
    max_depth: int
) -> Set[str]:
    """Process all URLs at the current depth in parallel."""
    if current_depth >= max_depth:
        return set()

    futures = []
    for url in urls:
        if url not in visited or visited[url] > current_depth:
            visited[url] = current_depth
            future = extract_links.submit(url)
            futures.append(future)

    if not futures:
        return set()

    # Wait for all tasks to complete and get their results
    all_links = set()
    for future in futures:
        try:
            links, rank = future.result()
            if isinstance(links, set):
                all_links.update(links)
        except Exception as e:
            logger.error(f"Error getting result from task: {str(e)}")
            continue

    return all_links

@flow(name="crawler_flow")
async def run_crawler(urls: List[str], max_depth: int = 3) -> Dict[str, Dict[str, int]]:
    """
    Flow to crawl multiple websites in parallel.
    
    Args:
        urls: List of URLs to crawl
        max_depth: Maximum depth to crawl for each URL
    
    Returns:
        Dict mapping start URLs to their crawl results
    """
    flow_logger = get_run_logger()
    flow_logger.info(f"Starting crawler flow for URLs: {urls}")
    visited: Dict[str, int] = {}  # URL -> depth mapping
    
    for url in urls:
        current_urls = {url}
        for depth in range(max_depth):
            flow_logger.info(f"Processing {url} at depth {depth}")
            # Process current depth
            new_urls = await process_depth(
                urls=current_urls,
                visited=visited,
                current_depth=depth,
                max_depth=max_depth
            )
            
            if not new_urls:
                flow_logger.info(f"No more URLs to process for {url} at depth {depth}")
                break
                
            current_urls = new_urls
            
    flow_logger.info(f"Crawling completed. Total URLs processed: {len(visited)}")
    return visited

if __name__ == "__main__":
    # Example usage of the crawler flow
    urls_to_crawl = [
        "https://python.org"
    ]
    asyncio.run(run_crawler(urls_to_crawl, max_depth=2))
