#!/usr/bin/env python3
"""
Veille Environnementale — Pipeline quotidien
Exécuté chaque matin à 7h30 via cron macOS

Dépendances :
    pip install requests feedparser crawl4ai beautifulsoup4

Ajout crontab (crontab -e) :
    30 7 * * * /usr/bin/python3 /path/to/pipeline.py >> /path/to/pipeline.log 2>&1
"""

import os
import sys
import json
import tarfile
import io
import re
import html
import hashlib
import shutil
import subprocess
import logging
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin

import requests
import feedparser

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

TODAY        = datetime.now().strftime('%Y-%m-%d')
SCRIPT_DIR   = Path(__file__).parent.resolve()
GHPAGES_WORKTREE = Path('/tmp/-veille-ghpages')
LOG_PATH     = SCRIPT_DIR / 'pipeline.log'

JORF_BASE_URL = 'https://echanges.dila.gouv.fr/OPENDATA/JORF/'
HUBEAU_URL    = 'https://hubeau.eaufrance.fr/api/v1/propluvia/restrictions'

# Mots-clés de pertinence 
KEYWORDS = [
    'ICPE', 'installation classée', 'eau', 'rejet aqueux', 'émissions',
    'biodiversité', 'espèce protégée', 'énergie', 'déchet', 'déchets',
    'pollution', 'environnement', 'écologique', 'REACH', 'biocide',
    'bruit', 'sol', 'air', 'risque industriel', 'Natura 2000',
    'nettoyage', 'propreté', 'entretien', 'désinfection',
    'produit chimique', 'COV', 'composé organique volatil',
    'SEVESO', 'PPR', 'DPE', 'amiante', 'plomb', 'REP',
    'responsabilité élargie', 'VHU', 'bâtiment tertiaire',
]

OLLAMA_URL   = 'http://localhost:11434/api/generate'
OLLAMA_MODEL = 'mistral'

RSS_SOURCES = [
    {
        'name': 'ADEME',
        'url': 'https://www.ademe.fr/feed/',
        'categorie': 'Presse',
        'fallback_crawl': 'https://www.ademe.fr/actualites/',
    },
    {
        'name': 'Actu-Environnement',
        'url': 'https://www.actu-environnement.com/ae/news/flux_rss.php4',
        'categorie': 'Presse',
        'fallback_crawl': 'https://www.actu-environnement.com/ae/news/',
    },
    {
        'name': 'Min. Transition Écologique',
        'url': 'https://www.ecologie.gouv.fr/actualites/rss',
        'categorie': 'Réglementation',
        'fallback_crawl': None,
    },
    {
        'name': 'Légifrance RSS',
        'url': 'https://www.legifrance.gouv.fr/rss/jorf.xml',
        'categorie': 'JO/DILA',
        'fallback_crawl': None,
    },
]

TIMEOUT = 30  # secondes

# Ordre des niveaux de restriction eau
NIVEAUX_ORDRE = {
    'vigilance': 1,
    'alerte': 2,
    'alerte renforcée': 3,
    'crise': 4,
}

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH, encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def make_id(source: str, titre: str) -> str:
    """ID unique reproductible à partir de la source et du titre."""
    return hashlib.md5(f"{source}:{titre}".encode()).hexdigest()[:12]


def keyword_match(text: str) -> bool:
    """Vérifie si un texte contient un mot-clé environnemental."""
    t = text.lower()
    return any(kw.lower() in t for kw in KEYWORDS)


def categorise(text: str) -> str:
    """Détermine la catégorie principale d'un article."""
    t = text.lower()
    if any(k in t for k in ['icpe', 'installation classée', 'seveso', 'autorisation', 'enregistrement']):
        return 'ICPE'
    if any(k in t for k in ['eau', 'rejet', 'assainissement', 'captage', 'hubeau', 'nappe']):
        return 'Eau'
    if any(k in t for k in ['énergie', 'dpe', 'thermique', 'renouvelable', 'carbone', 'ges']):
        return 'Énergie'
    if any(k in t for k in ['biodiversité', 'espèce', 'natura', 'faune', 'flore', 'erc']):
        return 'Biodiversité'
    if any(k in t for k in ['déchet', 'rep', 'vhu', 'tri', 'recyclage', 'traitement']):
        return 'Déchets'
    if any(k in t for k in ['émission', 'cov', 'pollution', 'bruit', 'air', 'formaldéhyde']):
        return 'Émissions'
    return 'Environnement'


# ─────────────────────────────────────────────
# OLLAMA
# ─────────────────────────────────────────────

def call_ollama(prompt: str) -> str:
    """Appelle Ollama (Mistral local) et retourne la réponse texte."""
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={'model': OLLAMA_MODEL, 'prompt': prompt, 'stream': False},
            timeout=90,
        )
        resp.raise_for_status()
        return resp.json().get('response', '').strip()
    except Exception as e:
        log.warning(f"Ollama indisponible : {e}")
        return ''


def extract_json(text: str) -> dict:
    """Extrait le premier bloc JSON valide d'une chaîne."""
    match = re.search(r'\{.*?\}', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except Exception:
            pass
    return {}


def ollama_impact_(titre: str, contenu: str) -> dict:
    """Évalue la pertinence et l'impact d'un texte JO pour ."""
    prompt = (
        "Tu es expert en réglementation environnementale pour , entreprise de propreté "
        "et services (nettoyage industriel, entretien de bâtiments, gestion de déchets, produits chimiques).\n\n"
        f"TITRE : {titre}\n"
        f"EXTRAIT : {contenu[:600]}\n\n"
        "Réponds UNIQUEMENT en JSON valide, sans aucun texte avant ou après :\n"
        '{"pertinent": true/false, "score": 1, "resume": "2-3 lignes max"}\n'
        "score : 1=information, 2=vigilance, 3=impact direct activités \n"
        "pertinent : true si concerne nettoyage/propreté/déchets/ICPE/eau/produits chimiques/bâtiments"
    )
    raw = call_ollama(prompt)
    result = extract_json(raw)
    return result if result else {'pertinent': False, 'score': 1, 'resume': ''}


def ollama_summarise(titre: str, contenu: str) -> dict:
    """Résume un article de presse et évalue son score pour ."""
    prompt = (
        "Tu travailles pour , entreprise de propreté et services.\n\n"
        f"TITRE : {titre}\n"
        f"CONTENU : {contenu[:800]}\n\n"
        "Réponds UNIQUEMENT en JSON valide :\n"
        '{"resume": "2 phrases max", "score": 1}\n'
        "score : 1=intéressant, 2=important, 3=impact direct "
    )
    raw = call_ollama(prompt)
    result = extract_json(raw)
    return result if result else {'resume': titre[:200], 'score': 1}


# ─────────────────────────────────────────────
# CRAWL4AI / FALLBACK
# ─────────────────────────────────────────────

def crawl_article(url: str) -> str:
    """Récupère le contenu textuel d'une URL (Crawl4AI ou BeautifulSoup)."""
    # Tentative Crawl4AI
    try:
        from crawl4ai import WebCrawler
        crawler = WebCrawler()
        crawler.warmup()
        result = crawler.run(url=url)
        if result.success and result.markdown:
            return result.markdown[:2500]
    except ImportError:
        pass
    except Exception as e:
        log.debug(f"Crawl4AI error {url}: {e}")

    # Fallback BeautifulSoup
    try:
        from bs4 import BeautifulSoup
        resp = requests.get(url, timeout=TIMEOUT, headers={'User-Agent': '-Veille/1.0'})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
            tag.decompose()
        return soup.get_text(separator=' ', strip=True)[:2500]
    except Exception as e:
        log.debug(f"Crawl fallback error {url}: {e}")

    return ''


# ─────────────────────────────────────────────
# JORF — Journal Officiel
# ─────────────────────────────────────────────

def list_jorf_files() -> list[str]:
    """Liste les fichiers tar.gz disponibles sur le serveur DILA."""
    try:
        resp = requests.get(JORF_BASE_URL, timeout=TIMEOUT)
        resp.raise_for_status()
        return sorted(re.findall(r'href="(JORF[^"]*\.tar\.gz)"', resp.text))
    except Exception as e:
        log.warning(f"JORF listing error : {e}")
        return []


def get_today_jorf_url() -> str | None:
    """Retourne l'URL du fichier JORF le plus récent pour aujourd'hui."""
    files = list_jorf_files()
    today_compact = datetime.now().strftime('%Y%m%d')
    today_files = [f for f in files if today_compact in f]
    chosen = today_files[-1] if today_files else (files[-1] if files else None)
    if chosen:
        log.info(f"Fichier JORF sélectionné : {chosen}")
        return urljoin(JORF_BASE_URL, chosen)
    return None


def parse_jorf_xml(content: bytes) -> list[dict]:
    """Parse un fichier XML JORF et extrait les articles filtrés par mots-clés."""
    import xml.etree.ElementTree as ET
    articles = []
    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        log.debug(f"XML parse error : {e}")
        return articles

    # Parcourir toute l'arborescence — DILA peut changer le format
    for elem in root.iter():
        tag = elem.tag.split('}')[-1].upper()
        if tag not in ('ARTICLE', 'TEXTE', 'ITEM', 'ENTRY'):
            continue

        titre, contenu, url = '', '', ''

        for child in elem:
            ctag = child.tag.split('}')[-1].upper()
            text = (child.text or '').strip()
            if ctag in ('TITRE', 'TITLE', 'INTITULE', 'NOR'):
                titre = titre or text
            elif ctag in ('CONTENU', 'CONTENT', 'DESCRIPTION', 'SUMMARY'):
                contenu = html.unescape(re.sub(r'<[^>]+>', ' ', text))
            elif ctag in ('URL', 'LINK', 'LIEN'):
                url = text or child.get('href', '')

        if not titre:
            continue
        if keyword_match(titre + ' ' + contenu):
            articles.append({'titre': titre, 'contenu': contenu[:500], 'url': url})

    return articles


def fetch_jorf() -> tuple[list, int]:
    """Télécharge le JORF, filtre et analyse via Ollama. Retourne (items, nb_analysés)."""
    log.info("=== JORF ===")
    items, total_analysed = [], 0

    try:
        url = get_today_jorf_url()
        if not url:
            log.warning("Aucun fichier JORF disponible")
            return items, 0

        log.info(f"Téléchargement : {url}")
        resp = requests.get(url, timeout=180)
        resp.raise_for_status()

        with tarfile.open(fileobj=io.BytesIO(resp.content), mode='r:gz') as tar:
            xml_members = [m for m in tar.getmembers() if m.name.endswith('.xml')]
            log.info(f"JORF : {len(xml_members)} fichiers XML")

            all_articles = []
            for member in xml_members:
                try:
                    f = tar.extractfile(member)
                    if f:
                        all_articles.extend(parse_jorf_xml(f.read()))
                except Exception as e:
                    log.debug(f"Erreur XML {member.name} : {e}")

            total_analysed = len(all_articles)
            log.info(f"JORF : {total_analysed} articles filtrés par mots-clés")

            for art in all_articles:
                try:
                    contenu = art['contenu']
                    # Enrichissement via Légifrance si URL dispo
                    if art.get('url') and 'legifrance' in art.get('url', ''):
                        full = crawl_article(art['url'])
                        if full:
                            contenu = full

                    analysis = ollama_impact_(art['titre'], contenu)

                    if analysis.get('pertinent'):
                        items.append({
                            'id': make_id('JORF', art['titre']),
                            'source': 'JORF',
                            'categorie': categorise(art['titre'] + ' ' + contenu),
                            'titre': art['titre'],
                            'resume': analysis.get('resume') or art['titre'],
                            'criticite': int(analysis.get('score', 1)),
                            'impact_': True,
                            'url': art.get('url', ''),
                            'date': TODAY,
                        })
                except Exception as e:
                    log.debug(f"Erreur analyse JORF : {e}")

    except Exception as e:
        log.error(f"JORF fatal : {e}", exc_info=True)

    log.info(f"JORF : {len(items)} retenus / {total_analysed} analysés")
    return items, total_analysed


# ─────────────────────────────────────────────
# RSS / PRESSE
# ─────────────────────────────────────────────

def fetch_rss_source(source: dict) -> list[dict]:
    """Récupère et analyse les articles d'une source RSS."""
    items = []
    name = source['name']

    try:
        feed = feedparser.parse(source['url'])
        if feed.bozo and not feed.entries:
            raise ValueError(f"Feed invalide : {feed.bozo_exception}")

        entries = feed.entries[:15]
        log.info(f"RSS {name} : {len(entries)} entrées")

        for entry in entries:
            titre = (entry.get('title') or '').strip()
            if not titre:
                continue

            contenu = html.unescape(re.sub(r'<[^>]+>', ' ', (
                entry.get('summary') or
                entry.get('description') or
                (entry.get('content') or [{}])[0].get('value') or ''
            ))).strip()

            url = entry.get('link', '')

            # Enrichissement si contenu insuffisant
            if len(contenu) < 100 and url:
                contenu = crawl_article(url) or contenu

            if not keyword_match(titre + ' ' + contenu):
                continue

            analysis = ollama_summarise(titre, contenu)
            items.append({
                'id': make_id(name, titre),
                'source': name,
                'categorie': categorise(titre + ' ' + contenu),
                'titre': titre,
                'resume': analysis.get('resume') or titre,
                'criticite': int(analysis.get('score', 1)),
                'impact_': int(analysis.get('score', 1)) >= 2,
                'url': url,
                'date': TODAY,
            })

    except Exception as e:
        log.warning(f"RSS {name} error : {e}")

        # Fallback page principale via Crawl4AI
        if source.get('fallback_crawl'):
            try:
                log.info(f"Fallback Crawl4AI → {source['fallback_crawl']}")
                contenu = crawl_article(source['fallback_crawl'])
                if contenu:
                    analysis = ollama_summarise(f"Actualités {name}", contenu)
                    items.append({
                        'id': make_id(name, 'fallback'),
                        'source': name,
                        'categorie': 'Presse',
                        'titre': f'Actualités {name}',
                        'resume': analysis.get('resume') or 'Source consultée via fallback.',
                        'criticite': 1,
                        'impact_': False,
                        'url': source['fallback_crawl'],
                        'date': TODAY,
                    })
            except Exception as e2:
                log.warning(f"Fallback {name} error : {e2}")

    return items


def fetch_rss() -> list[dict]:
    """Collecte et agrège tous les flux RSS."""
    log.info("=== RSS ===")
    all_items = []
    for source in RSS_SOURCES:
        items = fetch_rss_source(source)
        all_items.extend(items)
        log.info(f"RSS {source['name']} : {len(items)} retenus")
    return all_items


# ─────────────────────────────────────────────
# HUB'EAU — RESTRICTIONS EAU
# ─────────────────────────────────────────────

def fetch_hubeau() -> list[dict]:
    """Récupère les restrictions eau en vigueur via l'API Hub'Eau Propluvia."""
    log.info("=== Hub'Eau ===")
    restrictions = []

    try:
        resp = requests.get(HUBEAU_URL, params={'format': 'json', 'size': 5000}, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()

        # Un département peut avoir plusieurs arrêtés — garder le niveau le plus élevé
        dept_map: dict[str, dict] = {}

        for item in data.get('data', []):
            code = str(item.get('num_departement') or '').zfill(2)
            nom  = item.get('nom_departement', '')
            niv  = (item.get('nom_niveau') or '').lower().strip()
            url  = item.get('url_arrete', '') or ''

            if not code or not niv:
                continue

            existing = dept_map.get(code, {})
            if NIVEAUX_ORDRE.get(niv, 0) > NIVEAUX_ORDRE.get(existing.get('niveau', ''), 0):
                dept_map[code] = {
                    'dept_code': code,
                    'dept_nom': nom,
                    'niveau': niv,
                    'arrete_url': url,
                }

        restrictions = sorted(
            dept_map.values(),
            key=lambda x: NIVEAUX_ORDRE.get(x['niveau'], 0),
            reverse=True,
        )
        log.info(f"Hub'Eau : {len(restrictions)} départements concernés")

    except Exception as e:
        log.error(f"Hub'Eau fatal : {e}", exc_info=True)

    return list(restrictions)


# ─────────────────────────────────────────────
# GIT / GH-PAGES
# ─────────────────────────────────────────────

def push_to_ghpages(json_data: dict, date_str: str) -> bool:
    """
    Pousse le JSON quotidien vers gh-pages via git worktree.
    Ne touche JAMAIS la branche main.
    """
    log.info("=== Push gh-pages ===")
    worktree = GHPAGES_WORKTREE

    try:
        # Supprimer l'ancien worktree si présent
        if worktree.exists():
            subprocess.run(
                ['git', '-C', str(SCRIPT_DIR), 'worktree', 'remove', str(worktree), '--force'],
                capture_output=True,
            )

        # Créer le worktree sur gh-pages
        r = subprocess.run(
            ['git', '-C', str(SCRIPT_DIR), 'worktree', 'add', str(worktree), 'gh-pages'],
            capture_output=True, text=True,
        )
        if r.returncode != 0:
            log.error(f"worktree add échoué : {r.stderr}")
            return False

        # Écrire le JSON du jour
        data_dir = worktree / 'data'
        data_dir.mkdir(exist_ok=True)
        (data_dir / f'{date_str}.json').write_text(
            json.dumps(json_data, ensure_ascii=False, indent=2), encoding='utf-8'
        )

        # Mettre à jour archive.json
        archive_file = worktree / 'archive.json'
        archive: dict = json.loads(archive_file.read_text()) if archive_file.exists() else {'dates': []}
        archive.setdefault('dates', [])
        if date_str not in archive['dates']:
            archive['dates'].append(date_str)
        archive['dates'] = sorted(set(archive['dates']), reverse=True)
        archive['updated_at'] = datetime.now().isoformat()

        # Nettoyer les JSON > 90 jours
        cutoff = datetime.now() - timedelta(days=90)
        removed = []
        for f in data_dir.glob('*.json'):
            try:
                if datetime.strptime(f.stem, '%Y-%m-%d') < cutoff:
                    f.unlink()
                    removed.append(f.stem)
                    log.info(f"Supprimé : {f.name}")
            except ValueError:
                pass
        if removed:
            archive['dates'] = [d for d in archive['dates'] if d not in removed]

        archive_file.write_text(
            json.dumps(archive, ensure_ascii=False, indent=2), encoding='utf-8'
        )

        # git add + commit + push
        subprocess.run(['git', '-C', str(worktree), 'add', '-A'], check=True, capture_output=True)

        r_commit = subprocess.run(
            ['git', '-C', str(worktree), 'commit', '-m', f'data: veille {date_str}'],
            capture_output=True, text=True,
        )
        if 'nothing to commit' in r_commit.stdout + r_commit.stderr:
            log.info("Données déjà à jour — pas de commit")
            return True

        r_push = subprocess.run(
            ['git', '-C', str(worktree), 'push', 'origin', 'gh-pages'],
            capture_output=True, text=True,
        )
        if r_push.returncode != 0:
            log.error(f"Push échoué : {r_push.stderr}")
            return False

        log.info(f"gh-pages mis à jour pour {date_str}")
        return True

    except Exception as e:
        log.error(f"Push gh-pages fatal : {e}", exc_info=True)
        return False

    finally:
        subprocess.run(
            ['git', '-C', str(SCRIPT_DIR), 'worktree', 'remove', str(worktree), '--force'],
            capture_output=True,
        )


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main() -> int:
    log.info('=' * 60)
    log.info(f'Pipeline  Veille Environnementale — {TODAY}')
    log.info('=' * 60)

    start   = datetime.now()
    errors  = []
    items   = []
    stats   = {}
    restrictions = []

    # 1. JORF
    try:
        jorf_items, jorf_total = fetch_jorf()
        items.extend(jorf_items)
        stats['jo_analyses'] = jorf_total
        stats['jo_retenus']  = len(jorf_items)
    except Exception as e:
        log.error(f"JORF fatal : {e}")
        errors.append('JORF')
        stats.update({'jo_analyses': 0, 'jo_retenus': 0})

    # 2. RSS / Presse
    try:
        rss_items = fetch_rss()
        items.extend(rss_items)
        stats['articles_presse'] = len(rss_items)
    except Exception as e:
        log.error(f"RSS fatal : {e}")
        errors.append('RSS')
        stats['articles_presse'] = 0

    # 3. Hub'Eau
    try:
        restrictions = fetch_hubeau()
        stats['depts_restriction'] = len(restrictions)
    except Exception as e:
        log.error(f"Hub'Eau fatal : {e}")
        errors.append('HubEau')
        stats['depts_restriction'] = 0

    # Déduplication + tri par criticité décroissante
    seen: set[str] = set()
    unique: list[dict] = []
    for item in sorted(items, key=lambda x: -x.get('criticite', 1)):
        if item['id'] not in seen:
            seen.add(item['id'])
            unique.append(item)

    # 4. Générer le JSON
    elapsed = round((datetime.now() - start).total_seconds())
    json_data = {
        'date': TODAY,
        'generated_at': datetime.now().strftime('%H:%M'),
        'elapsed_seconds': elapsed,
        'errors': errors,
        'stats': stats,
        'items': unique,
        'restrictions_eau': restrictions,
    }

    log.info(f"JSON : {len(unique)} items, {len(restrictions)} départements eau")

    # 5. Push gh-pages
    ok = push_to_ghpages(json_data, TODAY)

    duration = (datetime.now() - start).total_seconds()
    status   = 'OK' if ok and not errors else ('PARTIEL' if ok else 'ERREUR')
    log.info(f"Pipeline terminé en {duration:.1f}s — {status}")

    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
