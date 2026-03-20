import html
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import feedparser
import requests

from ..config import RSS_SOURCES, TIMEOUT
from ..crawl import crawl_article, crawl_article_links
from ..filters import categorise, keyword_match, make_id
from ..llm import groq_analyse_rss

log = logging.getLogger(__name__)

_MONTHS = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12,
    'janvier': 1, 'février': 2, 'mars': 3, 'avril': 4,
    'mai': 5, 'juin': 6, 'juillet': 7, 'août': 8,
    'septembre': 9, 'octobre': 10, 'novembre': 11, 'décembre': 12,
}

def _date_from_text(text: str):
    """Extract a date like '2 April 2020' or '5 December 2025' embedded in text."""
    m = re.search(r'(\d{1,2})\s+([A-Za-zéûîôàè]+)\s+(\d{4})', text)
    if m:
        month = _MONTHS.get(m.group(2).lower())
        if month:
            try:
                return datetime(int(m.group(3)), month, int(m.group(1)))
            except ValueError:
                pass
    return None


def fetch_rss_source(source: dict, today_str: str):
    items = []
    name = source['name']

    try:
        try:
            rss_resp = requests.get(
                source['url'],
                timeout=TIMEOUT,
                headers={
                    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                },
            )
            feed = feedparser.parse(rss_resp.content)
        except Exception:
            feed = feedparser.parse(source['url'])

        if feed.bozo and not feed.entries:
            raise ValueError(f"Feed invalide : {feed.bozo_exception}")

        entries = feed.entries[:15]
        log.info(f"RSS {name} : {len(entries)} entrées")

        for entry in entries:
            titre = (entry.get('title') or '').strip()
            if not titre:
                continue

            contenu = html.unescape(re.sub(
                r'<[^>]+>',
                ' ',
                (
                    entry.get('summary')
                    or entry.get('description')
                    or (entry.get('content') or [{}])[0].get('value')
                    or ''
                ),
            )).strip()

            url = entry.get('link', '')

            pub = entry.get('published_parsed') or entry.get('updated_parsed')
            article_dt = None
            article_date = today_str
            if pub:
                try:
                    article_dt = datetime.fromtimestamp(time.mktime(pub))
                    article_date = article_dt.strftime('%Y-%m-%d')
                except Exception:
                    pass
            if article_dt is None:
                # feedparser couldn't auto-parse — try the raw string
                raw = entry.get('published') or entry.get('updated') or ''
                if raw:
                    try:
                        article_dt = parsedate_to_datetime(raw).astimezone(timezone.utc).replace(tzinfo=None)
                        article_date = article_dt.strftime('%Y-%m-%d')
                    except Exception:
                        try:
                            article_dt = datetime.fromisoformat(raw.replace('Z', '+00:00')).replace(tzinfo=None)
                            article_date = article_dt.strftime('%Y-%m-%d')
                        except Exception:
                            pass

            now = datetime.now()
            if now.weekday() == 0:
                cutoff_dt = (now - timedelta(days=3)).replace(
                    hour=18, minute=0, second=0, microsecond=0
                )
            else:
                cutoff_dt = now - timedelta(hours=24)

            if article_dt is None or article_dt < cutoff_dt:
                log.debug(f"Article sans date ou trop ancien ({article_date}), exclu : {titre[:60]}")
                continue

            # Certains flux (ex: Carbone 4) publient pubDate=aujourd'hui sur de vieux articles.
            # Si une date est lisible dans le titre et qu'elle est ancienne → exclure.
            title_dt = _date_from_text(titre)
            if title_dt and title_dt < cutoff_dt:
                log.debug(f"Date dans le titre trop ancienne ({title_dt.date()}), exclu : {titre[:60]}")
                continue

            if len(contenu) < 100 and url:
                contenu = crawl_article(url) or contenu

            if source.get('require_keywords'):
                txt = (titre + ' ' + contenu).lower()
                if not any(k.lower() in txt for k in source['require_keywords']):
                    continue

            if not keyword_match(titre + ' ' + contenu):
                continue

            analysis = groq_analyse_rss(titre, contenu)
            if analysis.get('pertinent') is False:
                log.debug(f"Non pertinent GSF, exclu : {titre[:60]}")
                continue

            score = int(analysis.get('score', 1))
            pourquoi = analysis.get('pourquoi', '')
            if score < 2:
                pourquoi = ''

            items.append({
                'id': make_id(name, titre),
                'source': name,
                'categorie': categorise(titre + ' ' + contenu),
                'titre': titre,
                'resume': analysis.get('resume') or titre,
                'pourquoi': pourquoi,
                'criticite': score,
                'impact_gsf': score >= 2,
                'url': url,
                'date': article_date,
            })

    except Exception as e:
        log.warning(f"RSS {name} error : {e}")

        if source.get('fallback_crawl'):
            fallback_url = source['fallback_crawl']
            base_url = '/'.join(fallback_url.split('/')[:3])
            try:
                log.info(f"Fallback → {fallback_url}")
                article_links = crawl_article_links(fallback_url, base_url, max_links=5)

                if not article_links:
                    contenu = crawl_article(fallback_url)
                    if contenu and keyword_match(contenu):
                        analysis = groq_analyse_rss(f"Actualités {name}", contenu)
                        if analysis.get('pertinent') is not False:
                            items.append({
                                'id': make_id(name, 'fallback'),
                                'source': name,
                                'categorie': 'Presse',
                                'titre': f'Actualités {name}',
                                'resume': analysis.get('resume') or 'Source consultée via fallback.',
                                'pourquoi': '',
                                'criticite': 1,
                                'impact_gsf': False,
                                'url': fallback_url,
                                'date': today_str,
                            })
                else:
                    now = datetime.now()
                    fallback_cutoff = (now - timedelta(days=3)).replace(hour=18, minute=0, second=0, microsecond=0) if now.weekday() == 0 else now - timedelta(hours=24)
                    for link in article_links:
                        try:
                            titre_art = link['titre']
                            # Filtre date dans le titre (ex: Carbone 4 "Guidelines2 April 2020")
                            title_dt = _date_from_text(titre_art)
                            if title_dt and title_dt < fallback_cutoff:
                                log.debug(f"Fallback date titre trop ancienne ({title_dt.date()}), exclu : {titre_art[:60]}")
                                continue

                            contenu = crawl_article(link['url'])
                            if not contenu:
                                continue

                            txt = titre_art + ' ' + contenu

                            if source.get('require_keywords'):
                                if not any(k.lower() in txt.lower() for k in source['require_keywords']):
                                    continue

                            if not keyword_match(txt):
                                continue

                            analysis = groq_analyse_rss(titre_art, contenu)
                            if analysis.get('pertinent') is False:
                                continue

                            score = int(analysis.get('score') or 1)
                            pourquoi = analysis.get('pourquoi', '') if score >= 2 else ''

                            items.append({
                                'id': make_id(name, titre_art),
                                'source': name,
                                'categorie': categorise(txt),
                                'titre': titre_art,
                                'resume': analysis.get('resume') or titre_art,
                                'pourquoi': pourquoi,
                                'criticite': score,
                                'impact_gsf': score >= 2,
                                'url': link['url'],
                                'date': today_str,
                            })

                        except Exception as e_art:
                            log.debug(f"Fallback article {link['url']} : {e_art}")

            except Exception as e2:
                log.warning(f"Fallback {name} error : {e2}")

    return items


def fetch_rss(today_str: str):
    log.info("=== RSS ===")
    all_items = []
    for source in RSS_SOURCES:
        items = fetch_rss_source(source, today_str)
        all_items.extend(items)
        log.info(f"RSS {source['name']} : {len(items)} retenus")
    return all_items

