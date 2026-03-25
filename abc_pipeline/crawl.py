import json
import logging
import re

import requests

from .config import TIMEOUT

log = logging.getLogger(__name__)

_image_cache: dict = {}
_date_cache:  dict = {}


def get_crawled_image(url: str) -> str:
    """Return the og:image URL cached during the last crawl_article() call for this URL."""
    return _image_cache.get(url, '')


def get_crawled_date(url: str) -> str:
    """Return the publication date (YYYY-MM-DD) cached during the last crawl_article() call, or ''."""
    return _date_cache.get(url, '')


def _extract_date_from_soup(soup) -> str:
    """Try to extract a publication date from common HTML patterns. Returns 'YYYY-MM-DD' or ''."""
    # 1. Open Graph / meta article:published_time
    for attr in ({'property': 'article:published_time'}, {'name': 'date'}, {'name': 'DC.date'}, {'itemprop': 'datePublished'}):
        tag = soup.find('meta', attrs=attr)
        if tag and tag.get('content'):
            m = re.search(r'(\d{4}-\d{2}-\d{2})', tag['content'])
            if m:
                return m.group(1)

    # 2. <time datetime="YYYY-MM-DD..."> (first one found)
    for t in soup.find_all('time', datetime=True):
        m = re.search(r'(\d{4}-\d{2}-\d{2})', t['datetime'])
        if m:
            return m.group(1)

    # 3. JSON-LD datePublished
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string or '')
            # Handle both single object and list
            items = data if isinstance(data, list) else [data]
            for item in items:
                dp = item.get('datePublished') or item.get('dateCreated') or ''
                m = re.search(r'(\d{4}-\d{2}-\d{2})', dp)
                if m:
                    return m.group(1)
        except Exception:
            pass

    return ''


def crawl_article(url: str) -> str:
    """Fetch textual content of an URL via BeautifulSoup.
    Also caches og:image and publication date, retrievable via get_crawled_image() / get_crawled_date().
    """
    try:
        from bs4 import BeautifulSoup

        resp = requests.get(url, timeout=TIMEOUT, headers={'User-Agent': 'ABC-Veille/2.0'})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')

        og = (
            soup.find('meta', property='og:image')
            or soup.find('meta', attrs={'name': 'twitter:image'})
        )
        if og and og.get('content', '').strip().startswith('http'):
            _image_cache[url] = og['content'].strip()

        pub_date = _extract_date_from_soup(soup)
        if pub_date:
            _date_cache[url] = pub_date

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

        resp = requests.get(listing_url, timeout=TIMEOUT, headers={'User-Agent': 'ABC-Veille/2.0'})
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


def crawl_article_links_filtered(listing_url: str, base_url: str, url_contains: str, max_links: int = 8) -> list:
    """Like crawl_article_links but only keeps links whose URL contains `url_contains`."""
    try:
        from bs4 import BeautifulSoup
        resp = requests.get(listing_url, timeout=TIMEOUT, headers={'User-Agent': 'ABC-Veille/2.0'})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        links = []
        seen = set()
        for a in soup.find_all('a', href=True):
            href = a['href'].strip()
            titre = a.get_text(strip=True)
            if not titre or len(titre) < 15:
                continue
            full_url = href if href.startswith('http') else base_url.rstrip('/') + href if href.startswith('/') else None
            if not full_url or full_url in seen:
                continue
            if base_url.split('/')[2] not in full_url:
                continue
            if url_contains not in full_url:
                continue
            seen.add(full_url)
            links.append({'titre': titre[:200], 'url': full_url})
            if len(links) >= max_links:
                break
        return links
    except Exception as e:
        log.debug(f"crawl_article_links_filtered error {listing_url}: {e}")
        return []

