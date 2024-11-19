import os
import asyncio
from prefect import task

from src.utils.url import sanitize_filename

@task(retries=2)
async def save_page_content(url: str, content: str):
    """
    Save page content to a file.
    
    Args:
        url: URL of the page
        content: HTML content to save
    """
    # Create directory if it doesn't exist
    os.makedirs('crawled_pages', exist_ok=True)
    
    # Create safe filename from URL
    filename = sanitize_filename(url)
    filepath = os.path.join('crawled_pages', f'{filename}.html')
    
    # Write content to file
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
    except OSError as e:
        if e.errno == 24:  # Too many open files
            # Wait a bit and retry
            await asyncio.sleep(1)
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
        else:
            raise
