import pandas as pd
from typing import List
from prefect import flow, get_run_logger
from tabulate import tabulate

from src.tasks.process_depth import process_depth

@flow
async def crawler_flow(start_urls: List[str], max_depth: int = 2):
    """
    Main crawler flow.
    
    Args:
        start_urls: List of URLs to start crawling from
        max_depth: Maximum depth to crawl
    """
    logger = get_run_logger()
    logger.info(f"Starting crawl of {len(start_urls)} URLs with max depth {max_depth}")
    
    visited = set()
    all_metrics = []
    current_urls = set(start_urls)
    
    # Process each depth level sequentially
    for depth in range(max_depth + 1):
        if not current_urls:
            break
            
        logger.info(f"Processing depth {depth}, {len(current_urls)} URLs")
        # Pass max_depth to ensure proper depth limiting
        next_urls, metrics = await process_depth(current_urls, visited, depth, max_depth)
        all_metrics.extend(metrics)
        current_urls = next_urls
        
        logger.info(f"Completed depth {depth}, found {len(next_urls)} new URLs")
    
    # Create report
    df = pd.DataFrame(all_metrics)
    logger.info(f"Crawl completed. Processed {len(df)} unique URLs.")
    
    # Save report using tabulate
    report_file = 'crawl_report.tsv'
    table = tabulate(df, headers='keys', tablefmt='tsv', floatfmt='.6f', showindex=False)
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(table)
    logger.info(f"Report saved to {report_file}")
    
    # Print summary table
    print(tabulate(df, headers='keys', tablefmt='grid', floatfmt='.6f'))
