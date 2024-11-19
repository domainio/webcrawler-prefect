"""URL utility functions."""
import re
from urllib.parse import urlparse, urlunparse

def normalize_url(url: str) -> str:
    """
    Normalize a URL by removing fragments and standardizing format.
    
    Args:
        url: URL to normalize
        
    Returns:
        Normalized URL
    """
    # Parse URL
    parsed = urlparse(url)
    
    # Remove fragment and normalize
    normalized = urlunparse((
        parsed.scheme,
        parsed.netloc.lower(),
        parsed.path,
        parsed.params,
        parsed.query,
        ''  # Remove fragment
    ))
    
    # Remove trailing slash if present
    if normalized.endswith('/'):
        normalized = normalized[:-1]
    
    return normalized

def sanitize_filename(url: str) -> str:
    """
    Convert URL to a valid filename.
    
    Args:
        url: URL to convert
        
    Returns:
        Valid filename based on URL
    """
    # Remove scheme and special characters
    filename = re.sub(r'^https?://', '', url)
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    
    # Ensure reasonable length
    if len(filename) > 200:
        filename = filename[:200]
    
    return filename
