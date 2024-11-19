"""Task for extracting links from a webpage."""
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Set, Tuple, Dict
from urllib.parse import urlparse, urljoin
from prefect import task, get_run_logger

from src.utils.url import normalize_url
from src.tasks.save_page_content import save_page_content

@task(retries=3)
async def extract_links(url: str, visited: Set[str]) -> Tuple[Set[str], dict]:
    """
    Extract all valid links from a given URL and calculate page metrics.
    
    Args:
        url: URL to extract links from
        visited: Set of already visited URLs
        
    Returns:
        Set of new URLs to visit and page metrics
    """
    logger = get_run_logger()
    metrics = {
        'url': url,
        'depth': 1,  # Will be updated by caller
        'internal_links': 0,
        'total_links': 0,
        'external_links': 0,
        'ratio': 0.0,
        'timestamp': datetime.utcnow().isoformat(),
        'success': False,
        'error': None
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    metrics['error'] = f'HTTP {response.status}'
                    return set(), metrics
                
                html = await response.text()
                await save_page_content.with_options(retries=2)(url, html)
                
                soup = BeautifulSoup(html, 'html.parser')
                same_domain_links = set()
                all_valid_links = set()
                
                base_domain = urlparse(url).netloc
                
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    absolute_url = urljoin(url, href)
                    
                    # Skip invalid URLs
                    if not absolute_url.startswith(('http://', 'https://')):
                        continue
                        
                    normalized_url = normalize_url(absolute_url)
                    all_valid_links.add(normalized_url)
                    
                    # Check if link is to same domain
                    if urlparse(normalized_url).netloc == base_domain:
                        same_domain_links.add(normalized_url)
                
                metrics['internal_links'] = len(same_domain_links)
                metrics['total_links'] = len(all_valid_links)
                metrics['external_links'] = len(all_valid_links) - len(same_domain_links)
                if metrics['total_links'] > 0:
                    metrics['ratio'] = metrics['internal_links'] / metrics['total_links']
                metrics['success'] = True
                
                logger.info(f"Processed {url}: {metrics['internal_links']} internal, {metrics['external_links']} external links")
                
                # Only return links we haven't visited yet
                return same_domain_links - visited, metrics
                
    except Exception as e:
        metrics['error'] = str(e)
        return set(), metrics
