import asyncio
from typing import Set, List, Tuple, Dict
from prefect import task

from src.tasks.extract_links import extract_links

# Limit concurrent tasks
MAX_CONCURRENT = 10
semaphore = asyncio.Semaphore(MAX_CONCURRENT)

@task(retries=2)
async def process_depth(urls: Set[str], visited: Set[str], depth: int, max_depth: int) -> Tuple[Set[str], List[dict]]:
    """
    Process all URLs at current depth level.
    
    Args:
        urls: URLs to process
        visited: Already visited URLs
        depth: Current depth level
        max_depth: Maximum depth to crawl
        
    Returns:
        New URLs to visit and metrics for processed URLs
    """
    next_urls = set()
    all_metrics = []
    
    # Skip if we've exceeded max_depth
    if depth > max_depth:
        return next_urls, all_metrics
    
    # Process URLs in parallel with concurrency limit
    tasks = []
    for url in urls:
        if url not in visited:
            visited.add(url)
            tasks.append(process_url(url, visited))
    
    # Wait for all URLs at current depth to complete
    if tasks:
        results = await asyncio.gather(*tasks)
        for new_urls, metrics in results:
            metrics['depth'] = depth
            # Only collect next_urls if we haven't reached max_depth
            if depth < max_depth:
                next_urls.update(new_urls)
            all_metrics.append(metrics)
    
    return next_urls, all_metrics

async def process_url(url: str, visited: Set[str]) -> Tuple[Set[str], dict]:
    """Process a single URL with concurrency control."""
    async with semaphore:
        return await extract_links.with_options(retries=2)(url, visited)
