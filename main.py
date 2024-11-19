import time
import asyncio
import aiohttp
import os
import hashlib
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse
from typing import Set, Dict, List, Tuple, Optional
from prefect import flow, task, get_run_logger
from dotenv import load_dotenv
import logging
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Create directory for storing HTML files
HTML_STORE_DIR = "crawled_pages"
os.makedirs(HTML_STORE_DIR, exist_ok=True)

def normalize_url(url: str) -> str:
    """Normalize URL by adding scheme if missing and standardizing format."""
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    parsed = urlparse(url)
    # Remove fragments and normalize path
    normalized = urlunparse((
        parsed.scheme,
        parsed.netloc.lower(),
        parsed.path.rstrip('/') or '/',
        parsed.params,
        parsed.query,
        ''  # Remove fragment
    ))
    return normalized

def url_to_filename(url: str) -> str:
    """Convert URL to a valid filename using hash."""
    url_hash = hashlib.md5(url.encode()).hexdigest()
    return f"{url_hash}.html"

@task(retries=3)
async def extract_links(url: str, visited: Set[str]) -> Tuple[Set[str], dict]:
    """
    Extract all valid links from a given URL and calculate page metrics.
    
    Returns:
        Tuple of (set of valid same-domain links, metrics dictionary)
    """
    task_logger = get_run_logger()
    metrics = {
        'url': url,
        'same_domain_links_count': 0,
        'total_links_count': 0,
        'external_links_count': 0,
        'ratio': 0.0,
        'timestamp': datetime.now().isoformat(),
        'success': False,
        'error': None
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    metrics['error'] = f"HTTP {response.status}"
                    return set(), metrics
                
                html = await response.text()
                
                # Save HTML content
                filename = url_to_filename(url)
                filepath = os.path.join(HTML_STORE_DIR, filename)
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(html)
                
                soup = BeautifulSoup(html, 'html.parser')
                same_domain_links = set()
                all_valid_links = set()
                
                base_domain = urlparse(url).netloc
                for link in soup.find_all('a'):
                    href = link.get('href')
                    if href:
                        try:
                            absolute_url = urljoin(url, href)
                            absolute_url = normalize_url(absolute_url)
                            parsed_url = urlparse(absolute_url)
                            
                            if parsed_url.scheme in ('http', 'https'):
                                all_valid_links.add(absolute_url)
                                if parsed_url.netloc == base_domain:
                                    # Only add links we haven't visited yet
                                    if absolute_url not in visited:
                                        same_domain_links.add(absolute_url)
                        except Exception:
                            continue
                
                metrics.update({
                    'same_domain_links_count': len(same_domain_links),
                    'total_links_count': len(all_valid_links),
                    'external_links_count': len(all_valid_links) - len(same_domain_links),
                    'ratio': len(same_domain_links) / len(all_valid_links) if all_valid_links else 0.0,
                    'success': True
                })
                
                task_logger.info(f"Processed {url}: {metrics['same_domain_links_count']} internal, {metrics['external_links_count']} external links")
                return same_domain_links, metrics
                
    except Exception as e:
        metrics['error'] = str(e)
        task_logger.error(f"Error processing {url}: {str(e)}")
        return set(), metrics

@task
async def process_depth(
    urls: Set[str],
    visited: Set[str],
    metrics: List[dict],
    current_depth: int,
    max_depth: int
) -> Set[str]:
    """Process all URLs at the current depth in parallel."""
    if current_depth >= max_depth:
        return set()

    # Process URLs in parallel batches
    batch_size = 10  # Adjust based on your needs
    all_new_links = set()
    
    for i in range(0, len(urls), batch_size):
        batch_urls = list(urls)[i:i + batch_size]
        tasks = []
        for url in batch_urls:
            if url not in visited:
                visited.add(url)
                tasks.append(extract_links.submit(url, visited))

        if not tasks:
            continue

        # Wait for batch to complete and get results
        for task in tasks:
            try:
                # Get the result without awaiting the task itself
                links, metric = task.result()
                if isinstance(links, set):
                    all_new_links.update(links)
                metric['depth'] = current_depth
                metrics.append(metric)
            except Exception as e:
                logger.error(f"Error processing task: {str(e)}")

    return all_new_links

@flow(name="crawler_flow")
async def run_crawler(urls: List[str], max_depth: int = 3) -> None:
    """
    Flow to crawl multiple websites in parallel and generate a report.
    
    Args:
        urls: List of URLs to crawl
        max_depth: Maximum depth to crawl for each URL
    """
    flow_logger = get_run_logger()
    
    # Normalize input URLs
    normalized_urls = {normalize_url(url) for url in urls}
    flow_logger.info(f"Starting crawler flow for URLs: {normalized_urls}")
    
    visited: Set[str] = set()
    metrics: List[dict] = []
    
    for url in normalized_urls:
        current_urls = {url}
        for depth in range(max_depth):
            flow_logger.info(f"Processing depth {depth}")
            new_urls = await process_depth(
                urls=current_urls,
                visited=visited,
                metrics=metrics,
                current_depth=depth,
                max_depth=max_depth
            )
            
            if not new_urls:
                flow_logger.info(f"No more URLs to process at depth {depth}")
                break
                
            current_urls = new_urls
    
    # Generate TSV report
    df = pd.DataFrame(metrics)
    report_file = "crawl_report.tsv"
    df.to_csv(report_file, sep='\t', index=False)
    flow_logger.info(f"Crawling completed. Report saved to {report_file}")
    flow_logger.info(f"Total URLs processed: {len(visited)}")
    flow_logger.info(f"HTML files saved in: {HTML_STORE_DIR}")

if __name__ == "__main__":
    # Example usage of the crawler flow
    urls_to_crawl = [
        "python.org",  # Will be normalized to https://python.org
        "https://docs.python.org"
    ]
    asyncio.run(run_crawler(urls_to_crawl, max_depth=2))
