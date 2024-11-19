"""Task for processing URLs at a specific depth level."""
import asyncio
from typing import Set, List, Tuple, Dict
from prefect import task

from src.tasks.extract_links import extract_links

@task
async def process_depth(urls: Set[str], visited: Set[str], depth: int) -> Tuple[Set[str], List[dict]]:
    """
    Process all URLs at current depth level.
    
    Args:
        urls: URLs to process
        visited: Already visited URLs
        depth: Current depth level
        
    Returns:
        New URLs to visit and metrics for processed URLs
    """
    next_urls = set()
    all_metrics = []
    
    # Process URLs in parallel
    tasks = []
    for url in urls:
        if url not in visited:
            visited.add(url)
            tasks.append(extract_links(url, visited))
    
    if tasks:
        results = await asyncio.gather(*tasks)
        for new_urls, metrics in results:
            metrics['depth'] = depth
            next_urls.update(new_urls)
            all_metrics.append(metrics)
    
    return next_urls, all_metrics
