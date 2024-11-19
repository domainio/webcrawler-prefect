import time
import asyncio
from prefect import flow, task
from dotenv import load_dotenv
import logging
from typing import Dict, List
from src.crawler import crawl_website

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@task
def fetch_data():
    """Fetch sample data with a delay."""
    print("Fetching data...")
    time.sleep(10)
    return {"data": "sample"}

@task
def process_data(input_data):
    """Process the input data with a delay."""
    print("Processing data:", input_data)
    time.sleep(5)
    return "processed " + str(input_data)

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
    logger.info(f"Starting crawler flow for URLs: {urls}")
    
    # Create tasks for each URL
    crawl_tasks = [crawl_website(url, max_depth) for url in urls]
    
    # Run all crawls in parallel
    results = await asyncio.gather(*crawl_tasks)
    
    # Map results to their start URLs
    return dict(zip(urls, results))

@flow(name="etl_flow")
def etl_flow():
    """Sample ETL flow."""
    data = fetch_data()
    result = process_data(data)
    print("Flow completed with result:", result)
    return result

if __name__ == "__main__":
    # Example usage of the crawler flow
    urls_to_crawl = [
        "https://example.com",
        "https://example.org"
    ]
    asyncio.run(run_crawler(urls_to_crawl, max_depth=2))
