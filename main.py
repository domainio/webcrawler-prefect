#!/usr/bin/env python3

import asyncio
import click
from src.flows.crawler_flow import crawler_flow
from src.utils.url import normalize_url

def validate_url(ctx, param, value: str) -> str:
    if not value.startswith(('http://', 'https://')):
        raise click.BadParameter('URL must start with http:// or https://')
    return normalize_url(value)

@click.command()
@click.argument('url', callback=validate_url)
@click.argument('max_depth', type=click.IntRange(min=0))
def main(url: str, max_depth: int):
    """Web crawler that starts from URL and crawls up to MAX_DEPTH levels deep."""
    asyncio.run(crawler_flow([url], max_depth))

if __name__ == "__main__":
    main()
