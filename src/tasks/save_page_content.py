import os
from prefect import task

from src.utils.url import sanitize_filename

@task
async def save_page_content(url: str, content: str):
    """
    Save page content to a file.
    
    Args:
        url: URL of the page
        content: HTML content to save
    """
    os.makedirs('crawled_pages', exist_ok=True)
    
    # Use sanitized URL as filename
    filename = sanitize_filename(url)
    filepath = os.path.join('crawled_pages', f"{filename}.html")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
