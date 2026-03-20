import logging

import requests

from .config import TIMEOUT

log = logging.getLogger(__name__)


def crawl_article(url: str) -> str:
    """Fetch textual content of an URL via BeautifulSoup."""
    try:
        from bs4 import BeautifulSoup

        resp = requests.get(url, timeout=TIMEOUT, headers={'User-Agent': 'GSF-Veille/2.0'})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')

        for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
            tag.decompose()

        return soup.get_text(separator=' ', strip=True)[:2500]
    except Exception as e:
        log.debug(f"Crawl error {url}: {e}")
        return ''


def crawl_article_links(listing_url: str, base_url: str, max_links: int = 5) -> list:
    """
    Crawl a listing page and extract links to individual articles.
    Returns a list of dicts: {titre, url}.
    """
    try:
        from bs4 import BeautifulSoup

        resp = requests.get(listing_url, timeout=TIMEOUT, headers={'User-Agent': 'GSF-Veille/2.0'})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')

        links = []
        seen = set()

        for a in soup.find_all('a', href=True):
            href = a['href'].strip()
            titre = a.get_text(strip=True)

            if not titre or len(titre) < 15:
                continue
            if any(
                skip in href
                for skip in [
                    '#', 'mailto:', 'javascript:', '/tag/', '/category/',
                    '/page/', '/feed', '/rss', '/newsletter', '/contact',
                    '/about', '/team', '/equipe', '/linstitut',
                ]
            ):
                continue

            if href.startswith('http'):
                full_url = href
            elif href.startswith('/'):
                full_url = base_url.rstrip('/') + href
            else:
                continue

            if full_url in seen:
                continue
            seen.add(full_url)

            # Keep only same domain
            if base_url.split('/')[2] not in full_url:
                continue

            links.append({'titre': titre[:200], 'url': full_url})
            if len(links) >= max_links:
                break

        return links
    except Exception as e:
        log.debug(f"crawl_article_links error {listing_url}: {e}")
        return []

