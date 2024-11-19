import re
from urllib.parse import urlparse, urlunparse

def normalize_url(url: str) -> str:
    """
    Normalize URL by removing fragments and trailing slashes.
    
    Args:
        url: URL to normalize
        
    Returns:
        Normalized URL
    """
    parsed = urlparse(url)
    # Remove fragments and normalize path
    normalized = urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path.rstrip('/') or '/',
        parsed.params,
        parsed.query,
        ''  # Remove fragment
    ))
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
    filename = re.sub(r'[^\w\-_.]', '_', filename)
    return filename
