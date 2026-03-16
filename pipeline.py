#!/usr/bin/env python3
"""
Veille Environnementale GSF — Pipeline quotidien
Exécuté chaque matin à 07:20 UTC via GitHub Actions

Dépendances :
    pip install requests feedparser groq beautifulsoup4
"""

import os
import sys
import json
import tarfile
import io
import re
import html
import hashlib
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin

import requests
import feedparser
from groq import Groq

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

TODAY      = datetime.now().strftime('%Y-%m-%d')
SCRIPT_DIR = Path(__file__).parent.resolve()
LOG_PATH   = SCRIPT_DIR / 'pipeline.log'

JORF_BASE_URL     = 'https://echanges.dila.gouv.fr/OPENDATA/JORF/'
VIGIEAU_DEPTS_URL = 'https://api.vigieau.gouv.fr/api/departements'

# Groq
GROQ_MODEL      = 'llama-3.1-8b-instant'   # Free tier, rapide
GROQ_MAX_RETRY  = 4                          # Tentatives sur 429
GROQ_RETRY_WAIT = 20                         # Secondes d'attente entre retries

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

JORF_KEYWORDS_STRICT = [
    'nettoyage industriel', 'nettoyage tertiaire', 'nettoyage des locaux',
    'propreté industrielle', 'agent de propreté', 'entreprise de propreté',
    'branche propreté', 'convention collective', 'propreté et services',
    'désinfection', 'décontamination', 'hygiène des locaux',
    'déchet dangereux', 'déchets dangereux', 'déchet industriel', 'déchets industriels',
    'DASRI', 'déchet infectieux', 'déchet de soins',
    'collecte de déchets', 'traitement de déchets', 'élimination de déchets',
    'responsabilité élargie du producteur', 'filière REP',
    'biocide', 'détergent', 'substance dangereuse', 'produit chimique',
    'CMR', 'composé organique volatil', 'COV', 'solvant',
    'REACH', 'CLP', 'fiche de données de sécurité', 'FDS',
    'installation classée', 'ICPE', 'SEVESO', 'rubrique ICPE',
    'radioprotection', 'zone contrôlée', 'zone surveillée',
    'amiante', 'désamiantage', 'plomb', 'saturnisme',
]

RSS_SOURCES = [
    {
        'name': 'ADEME',
        'url': 'https://www.ademe.fr/feed/',
        'categorie': 'Presse',
        'fallback_crawl': 'https://www.ademe.fr/actualites/',
    },
    {
        'name': 'Actu-Environnement',
        'url': 'https://www.actu-environnement.com/flux/rss/environnement/',
        'categorie': 'Presse',
        'fallback_crawl': 'https://www.actu-environnement.com/ae/news/',
    },
    {
        'name': 'Min. Transition Écologique',
        'url': 'https://www.ecologie.gouv.fr/rss-actualites.xml',
        'categorie': 'Réglementation',
        'fallback_crawl': 'https://www.ecologie.gouv.fr/actualites',
    },
    {
        'name': 'Contexte Environnement',
        'url': 'https://www.contexte.com/articles/rss/edition/environnement',
        'categorie': 'Réglementation',
        'fallback_crawl': 'https://www.contexte.com/environnement/',
    },
    {
        'name': 'Novethic',
        'url': 'https://www.novethic.fr/feed',
        'categorie': 'Presse',
        'fallback_crawl': 'https://www.novethic.fr/',
    },
    {
        'name': 'Reporterre',
        'url': 'https://reporterre.net/spip.php?page=backend',
        'categorie': 'Presse',
        'fallback_crawl': 'https://reporterre.net/',
    },
    {
        'name': 'Contexte Env.',
        'url': '',  # Pas de RSS public — fallback scraping uniquement
        'categorie': 'Réglementation',
        'fallback_crawl': 'https://www.contexte.com/fr/verticale/environnement/',
    },
    {
        'name': 'Politico Energy EU',
        'url': 'https://www.politico.eu/section/energy-fr/feed/',
        'categorie': 'Réglementation',
        'fallback_crawl': 'https://www.politico.eu/section/energy-fr/',
    },
]

TIMEOUT = 30

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
    return hashlib.md5(f"{source}:{titre}".encode()).hexdigest()[:12]


def keyword_match(text: str) -> bool:
    t = text.lower()
    return any(kw.lower() in t for kw in KEYWORDS)


def categorise(text: str) -> str:
    t = text.lower()
    if any(k in t for k in ['icpe', 'installation classée', 'seveso', 'autorisation', 'enregistrement']):
        return 'ICPE'
    if any(k in t for k in ['eau', 'rejet', 'assainissement', 'captage', 'nappe']):
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
# GROQ — remplace Ollama
# ─────────────────────────────────────────────

_groq_client = None


def get_groq_client() -> Groq:
    global _groq_client
    if _groq_client is None:
        api_key = os.environ.get('GROQ_API_KEY', '')
        if not api_key:
            raise RuntimeError("GROQ_API_KEY manquant dans les variables d'environnement")
        _groq_client = Groq(api_key=api_key)
    return _groq_client


def call_groq(prompt: str, system: str = '') -> str:
    """Appelle l'API Groq avec retry sur erreur 429 (rate limit)."""
    client = get_groq_client()
    messages = []
    if system:
        messages.append({'role': 'system', 'content': system})
    messages.append({'role': 'user', 'content': prompt})

    for attempt in range(1, GROQ_MAX_RETRY + 1):
        try:
            resp = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=messages,
                max_tokens=300,
                temperature=0.2,
                response_format={"type": "json_object"},
            )
            return resp.choices[0].message.content.strip()

        except Exception as e:
            err_str = str(e)
            if '429' in err_str or 'rate_limit' in err_str.lower():
                wait = GROQ_RETRY_WAIT * attempt
                log.warning(f"Groq rate limit (tentative {attempt}/{GROQ_MAX_RETRY}) — attente {wait}s")
                time.sleep(wait)
            else:
                log.warning(f"Groq erreur : {e}")
                return ''

    log.error("Groq : toutes les tentatives épuisées")
    return ''


def extract_json(text: str) -> dict:
    """Extrait le premier bloc JSON valide d'une chaîne."""
    try:
        return json.loads(text)
    except Exception:
        pass
    match = re.search(r'\{.*?\}', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except Exception:
            pass
    return {}



def _gsf_est_pertinent(titre: str, contenu: str) -> bool:
    """Filtre binaire strict : pertinent pour le responsable Env/Decarbo de GSF ?"""
    system = 'Tu es un filtre binaire. JSON uniquement, rien dautre.'
    prompt = (
        'GSF = groupe de nettoyage industriel. '
        'Responsable Environnement veut lire : ICPE, REACH, biocides, dechets industriels, '
        'eau industrielle, decarbonation entreprises, RSE reglementaire, sante-securite '
        'agents nettoyage, convention collective proprete, circularite/REP.\n'
        'Exclure : politique, elections, sondages, guerres, geopolitique, immobilier, '
        'logement, agriculture, finance, faits divers, polemiques societales.\n\n'
        f'TITRE: {titre}\n'
        f'DEBUT: {contenu[:300]}\n\n'
        'Reponds uniquement: {"ok": true} si dans le perimetre, {"ok": false} sinon.'
    )
    raw = call_groq(prompt, system)
    result = extract_json(raw)
    return bool(result.get('ok', False))


def _gsf_resumer(titre: str, contenu: str) -> dict:
    """Resume et donne un score de priorite a un article pertinent pour GSF."""
    system = 'Analyste reglementaire GSF. JSON valide uniquement.'
    prompt = (
        'GSF nettoie usines, hopitaux, sites nucleaires. '
        'Produits chimiques, dechets industriels, ICPE.\n\n'
        'Score:\n'
        '  1 = tendance sectorielle a connaitre\n'
        '  2 = reglementation en evolution pouvant affecter GSF\n'
        '  3 = obligation legale en vigueur OU risque direct GSF\n\n'
        f'TITRE: {titre}\n'
        f'CONTENU: {contenu[:600]}\n\n'
        'JSON: {"resume": "1-2 phrases utiles", "score": 1}'
    )
    raw = call_groq(prompt, system)
    result = extract_json(raw)
    return result if result else {'resume': titre[:200], 'score': 1}


def groq_summarise(titre: str, contenu: str) -> dict:
    """2 appels Groq : filtre d'abord, resume si pertinent."""
    if not _gsf_est_pertinent(titre, contenu):
        log.debug(f'Exclu (non pertinent GSF) : {titre[:60]}')
        return {'pertinent': False, 'resume': '', 'score': 1}
    analysis = _gsf_resumer(titre, contenu)
    analysis['pertinent'] = True
    return analysis



def groq_impact_gsf(titre: str, contenu: str) -> dict:
    """Évalue la pertinence et l'impact d'un texte JO pour GSF."""
    system = (
        "Tu es un analyste réglementaire pour GSF (nettoyage industriel, 42 000 salariés). "
        "Réponds UNIQUEMENT en JSON valide, sans texte avant ou après."
    )
    prompt = (
        "GSF intervient dans : usines agroalimentaires, industrie, nucléaire, pharmaceutique, "
        "hôpitaux, bureaux, transports. GSF gère des déchets industriels, utilise des produits "
        "chimiques (détergents, désinfectants, biocides) et exploite des installations ICPE.\n\n"
        "EXCLUSIONS ABSOLUES (pertinent=false) :\n"
        "- Professions réglementées (médecins, avocats, notaires...)\n"
        "- Nominations, mutations, concours fonction publique\n"
        "- Finances publiques, fiscalité, défense, justice\n"
        "- Urbanisme, agriculture, sylviculture (sauf lien direct nettoyage/déchets)\n\n"
        "PERTINENT uniquement si le texte modifie directement :\n"
        "- Règles biocides, détergents, CMR, solvants, REACH\n"
        "- Réglementation ICPE (rubriques, seuils, obligations)\n"
        "- Gestion/collecte/traitement déchets industriels ou dangereux\n"
        "- Normes hygiène agroalimentaire, pharma, santé, nucléaire\n"
        "- Santé-sécurité agents de nettoyage (TMS, chimiques, EPI)\n"
        "- Rejets aqueux, eaux usées industrielles\n"
        "- Conventions collectives ou accords branche propreté\n\n"
        "EN CAS DE DOUTE : pertinent=false.\n\n"
        f"TITRE : {titre}\n"
        f"EXTRAIT : {contenu[:600]}\n\n"
        "Réponds avec ce JSON exactement :\n"
        '{"pertinent": true, "score": 2, "resume": "2 phrases max si pertinent, sinon vide"}\n'
        "score : 1=veille, 2=à surveiller, 3=obligation directe sur opérations GSF"
    )
    raw = call_groq(prompt, system)
    result = extract_json(raw)
    return result if result else {'pertinent': False, 'score': 1, 'resume': ''}


# ─────────────────────────────────────────────
# CRAWL FALLBACK
# crawl4ai retiré — incompatible GitHub Actions
# ─────────────────────────────────────────────

def crawl_article(url: str) -> str:
    """Récupère le contenu textuel d'une URL via BeautifulSoup."""
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


# ─────────────────────────────────────────────
# JORF — Journal Officiel
# ─────────────────────────────────────────────

def list_jorf_files() -> list:
    try:
        resp = requests.get(JORF_BASE_URL, timeout=TIMEOUT)
        resp.raise_for_status()
        return sorted(re.findall(r'href="(JORF[^"]*\.tar\.gz)"', resp.text))
    except Exception as e:
        log.warning(f"JORF listing error : {e}")
        return []


def get_today_jorf_url():
    files = list_jorf_files()
    today_compact = datetime.now().strftime('%Y%m%d')
    today_files = [f for f in files if today_compact in f]
    chosen = today_files[-1] if today_files else (files[-1] if files else None)
    if chosen:
        log.info(f"Fichier JORF sélectionné : {chosen}")
        return urljoin(JORF_BASE_URL, chosen)
    return None


def parse_jorf_xml(content: bytes):
    import xml.etree.ElementTree as ET
    articles = []
    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        log.debug(f"XML parse error : {e}")
        return articles

    seen_nor = set()

    for texte in root.iter('TEXTE'):
        nor = texte.get('nor', '').strip()
        titre_el = texte.find('TITRE_TXT')
        titre = (titre_el.text or '').strip() if titre_el is not None else ''

        if not titre:
            continue
        if nor and nor in seen_nor:
            continue
        if nor:
            seen_nor.add(nor)

        nature    = texte.get('nature', '')
        ministere = texte.get('ministere', '')
        date_pub  = texte.get('date_publi', TODAY)
        cid       = texte.get('cid', '')

        url = f'https://www.legifrance.gouv.fr/jorf/id/{cid}' if cid else (
              f'https://www.legifrance.gouv.fr/jorf/id/{nor}' if nor else '')
        contenu = ' — '.join(filter(None, [nature, ministere]))

        if keyword_match(titre + ' ' + contenu):
            articles.append({
                'titre'   : titre,
                'contenu' : contenu[:500],
                'url'     : url,
                'nor'     : nor,
                'date'    : date_pub,
            })

    return articles


def fetch_jorf():
    log.info("=== JORF ===")
    items, autres, total_analysed = [], [], 0

    try:
        url = get_today_jorf_url()
        if not url:
            log.warning("Aucun fichier JORF disponible")
            return items, autres, 0

        log.info(f"Téléchargement : {url}")
        resp = requests.get(url, timeout=180)
        resp.raise_for_status()

        with tarfile.open(fileobj=io.BytesIO(resp.content), mode='r:gz') as tar:
            xml_members = [m for m in tar.getmembers() if m.name.endswith('.xml')]
            log.info(f"JORF : {len(xml_members)} fichiers XML")

            all_articles = []
            seen_nor_global = set()
            for member in xml_members:
                try:
                    f = tar.extractfile(member)
                    if f:
                        for art in parse_jorf_xml(f.read()):
                            nor = art.get('nor', '')
                            if nor and nor in seen_nor_global:
                                continue
                            if nor:
                                seen_nor_global.add(nor)
                            all_articles.append(art)
                except Exception as e:
                    log.debug(f"Erreur XML {member.name} : {e}")

            total_analysed = len(all_articles)
            log.info(f"JORF : {total_analysed} textes keywords généraux")

            def jorf_strict_match(titre: str) -> bool:
                t = titre.lower()
                return any(k.lower() in t for k in JORF_KEYWORDS_STRICT)

            gsf_candidates = [a for a in all_articles if jorf_strict_match(a['titre'])]
            rest = [a for a in all_articles if not jorf_strict_match(a['titre'])]
            log.info(f"JORF : {len(gsf_candidates)} candidats GSF, {len(rest)} autres")

            for art in rest:
                autres.append({
                    'nor'      : art.get('nor', ''),
                    'titre'    : art['titre'],
                    'nature'   : art.get('contenu', '').split(' — ')[0],
                    'ministere': art.get('contenu', '').split(' — ')[1] if ' — ' in art.get('contenu', '') else '',
                    'url'      : art.get('url', ''),
                    'date'     : art.get('date', TODAY),
                })

            for art in gsf_candidates:
                try:
                    contenu = art['contenu']
                    if art.get('url') and 'legifrance' in art.get('url', ''):
                        full = crawl_article(art['url'])
                        if full:
                            contenu = full

                    analysis = groq_summarise(art['titre'], contenu)

                    items.append({
                        'id'       : make_id('JORF', art['titre']),
                        'source'   : 'JORF',
                        'categorie': categorise(art['titre'] + ' ' + contenu),
                        'titre'    : art['titre'],
                        'resume'   : analysis.get('resume') or art['titre'],
                        'criticite': int(analysis.get('score', 2)),
                        'impact_gsf': True,
                        'url'      : art.get('url', ''),
                        'date'     : TODAY,
                    })
                except Exception as e:
                    log.debug(f"Erreur résumé JORF : {e}")

    except Exception as e:
        log.error(f"JORF fatal : {e}", exc_info=True)

    log.info(f"JORF : {len(items)} retenus / {total_analysed} analysés ({len(autres)} autres)")
    return items, autres, total_analysed


# ─────────────────────────────────────────────
# RSS / PRESSE
# ─────────────────────────────────────────────

def fetch_rss_source(source: dict):
    items = []
    name  = source['name']

    try:
        # Passage par requests pour forcer un User-Agent navigateur
        # (feedparser natif est bloqué par certains serveurs sur GitHub Actions)
        try:
            rss_resp = requests.get(
                source['url'],
                timeout=TIMEOUT,
                headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'},
            )
            feed = feedparser.parse(rss_resp.content)
        except Exception:
            feed = feedparser.parse(source['url'])  # fallback feedparser natif

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

            pub = entry.get('published_parsed') or entry.get('updated_parsed')
            if pub:
                import time as _time
                try:
                    article_dt   = datetime.fromtimestamp(_time.mktime(pub))
                    article_date = article_dt.strftime('%Y-%m-%d')
                except Exception:
                    article_dt   = None
                    article_date = TODAY
            else:
                article_dt   = None
                article_date = TODAY

            # Filtre temporel souple : exclure uniquement les articles > 7 jours
            # (certaines sources RSS publient avec des dates décalées)
            # La fraîcheur du feed est garantie par le cron quotidien
            now = datetime.now()
            cutoff_dt = now - timedelta(days=7)
            if article_dt and article_dt < cutoff_dt:
                log.debug(f"Article trop ancien ({article_date}), exclu : {titre[:60]}")
                continue

            if len(contenu) < 100 and url:
                contenu = crawl_article(url) or contenu

            if not keyword_match(titre + ' ' + contenu):
                continue

            analysis = groq_summarise(titre, contenu)
            # Pré-filtre pertinence : exclure si le LLM juge non pertinent
            if analysis.get('pertinent') is False:
                log.debug(f"Non pertinent GSF, exclu : {titre[:60]}")
                continue
            items.append({
                'id'        : make_id(name, titre),
                'source'    : name,
                'categorie' : categorise(titre + ' ' + contenu),
                'titre'     : titre,
                'resume'    : analysis.get('resume') or titre,
                'criticite' : int(analysis.get('score', 1)),
                'impact_gsf': int(analysis.get('score', 1)) >= 2,
                'url'       : url,
                'date'      : article_date,
            })

    except Exception as e:
        log.warning(f"RSS {name} error : {e}")

        if source.get('fallback_crawl'):
            try:
                log.info(f"Fallback → {source['fallback_crawl']}")
                contenu = crawl_article(source['fallback_crawl'])
                if contenu:
                    analysis = groq_summarise(f"Actualités {name}", contenu)
                    items.append({
                        'id'        : make_id(name, 'fallback'),
                        'source'    : name,
                        'categorie' : 'Presse',
                        'titre'     : f'Actualités {name}',
                        'resume'    : analysis.get('resume') or 'Source consultée via fallback.',
                        'criticite' : 1,
                        'impact_gsf': False,
                        'url'       : source['fallback_crawl'],
                        'date'      : TODAY,
                    })
            except Exception as e2:
                log.warning(f"Fallback {name} error : {e2}")

    return items


def fetch_rss():
    log.info("=== RSS ===")
    all_items = []
    for source in RSS_SOURCES:
        items = fetch_rss_source(source)
        all_items.extend(items)
        log.info(f"RSS {source['name']} : {len(items)} retenus")
    return all_items


# ─────────────────────────────────────────────
# VIGIEAU
# ─────────────────────────────────────────────

def fetch_vigieau():
    log.info("=== VigiEau ===")
    restrictions = []

    try:
        resp = requests.get(VIGIEAU_DEPTS_URL, timeout=TIMEOUT)
        resp.raise_for_status()

        for dept in resp.json():
            niv = dept.get('niveauGraviteMax')
            if not niv:
                continue

            restrictions.append({
                'dept_code': dept.get('code', ''),
                'dept_nom' : dept.get('nom', ''),
                'niveau'   : niv,
                'niveauSup': dept.get('niveauGraviteSupMax'),
                'niveauSou': dept.get('niveauGraviteSouMax'),
                'niveauAep': dept.get('niveauGraviteAepMax'),
            })

        restrictions.sort(key=lambda x: NIVEAUX_ORDRE.get(x['niveau'], 0), reverse=True)
        log.info(f"VigiEau : {len(restrictions)} départements en restriction")

    except Exception as e:
        log.error(f"VigiEau fatal : {e}", exc_info=True)

    return restrictions


# ─────────────────────────────────────────────
# VIGIEAU HISTORIQUE — data.gouv.fr
# ─────────────────────────────────────────────

DATAGOUV_DATASET_ID = 'donnee-secheresse-vigieau'
DATAGOUV_API        = 'https://www.data.gouv.fr/api/1/datasets/'
VIGIEAU_HISTORY_YEARS = list(range(2020, datetime.now().year + 1))

NIVEAUX_GRAVITE = ['vigilance', 'alerte', 'alerte renforcée', 'crise']


def _parse_vigieau_csv(content: str, year: int) -> dict:
    """
    Parse un CSV arrêtés VigiEau et retourne les stats agrégées :
    - par_mois : {YYYY-MM: {vigilance: N, alerte: N, alerte renforcée: N, crise: N}}
    - par_dept : {code_dept: {nom, jours_alerte, jours_alerte_renforcee, jours_crise, total_jours_graves}}
    """
    import csv
    import io
    from collections import defaultdict

    par_mois = defaultdict(lambda: {n: 0 for n in NIVEAUX_GRAVITE})
    par_dept = defaultdict(lambda: {'nom': '', 'jours': {n: 0 for n in NIVEAUX_GRAVITE}})

    reader = csv.DictReader(io.StringIO(content), delimiter=',')

    for row in reader:
        try:
            date_debut_str = row.get('date_debut', '').strip()
            date_fin_str   = row.get('date_fin', '').strip()
            niveau         = row.get('zones_alerte.type', '').strip().lower()
            depts_str      = row.get('departements', '').strip()

            if not date_debut_str or niveau not in NIVEAUX_GRAVITE:
                continue

            # Parse dates
            date_debut = datetime.strptime(date_debut_str[:10], '%Y-%m-%d')
            if date_fin_str and date_fin_str != 'None':
                date_fin = datetime.strptime(date_fin_str[:10], '%Y-%m-%d')
            else:
                date_fin = datetime.now()

            # Limiter à l'année concernée
            year_start = datetime(year, 1, 1)
            year_end   = datetime(year, 12, 31)
            d_start = max(date_debut, year_start)
            d_end   = min(date_fin, year_end)

            if d_start > d_end:
                continue

            nb_jours = (d_end - d_start).days + 1

            # Agréger par mois
            current = d_start
            while current <= d_end:
                mois_key = current.strftime('%Y-%m')
                par_mois[mois_key][niveau] += 1
                current = current.replace(day=28) + timedelta(days=4)
                current = current.replace(day=1)

            # Agréger par département
            depts = [d.strip() for d in depts_str.split(';') if d.strip()]
            if not depts:
                dept_code = row.get('departement_pilote', '').strip()
                dept_nom  = dept_code
                if dept_code:
                    depts = [dept_code]

            for dept in depts:
                # Format attendu: "01 - Ain" ou juste "01"
                parts = dept.split(' - ', 1)
                code  = parts[0].strip().zfill(2)
                nom   = parts[1].strip() if len(parts) > 1 else code
                par_dept[code]['nom'] = nom
                par_dept[code]['jours'][niveau] += nb_jours

        except Exception as e:
            log.debug(f"Ligne CSV ignorée : {e}")
            continue

    # Sérialiser
    mois_out = {}
    for mois, niveaux in sorted(par_mois.items()):
        mois_out[mois] = niveaux

    dept_out = {}
    for code, data in par_dept.items():
        graves = (data['jours'].get('alerte', 0) +
                  data['jours'].get('alerte renforcée', 0) +
                  data['jours'].get('crise', 0))
        dept_out[code] = {
            'nom'          : data['nom'],
            'jours'        : data['jours'],
            'total_graves' : graves,
        }

    return {'par_mois': mois_out, 'par_dept': dept_out}


def fetch_vigieau_history() -> dict:
    """
    Télécharge et agrège l'historique des arrêtés VigiEau depuis data.gouv.fr.
    Cache intelligent : ne re-télécharge que l'année en cours si le cache existe.
    Retourne un dict prêt à écrire dans vigieau_history.json.
    """
    log.info("=== VigiEau Historique ===")

    history_file = SCRIPT_DIR / 'vigieau_history.json'
    current_year = datetime.now().year

    # Charger le cache existant
    existing = {}
    if history_file.exists():
        try:
            existing = json.loads(history_file.read_text(encoding='utf-8'))
            log.info(f"Cache existant : années {list(existing.get('annees', {}).keys())}")
        except Exception:
            existing = {}

    annees_cache = existing.get('annees', {})

    # Récupérer la liste des ressources du dataset via l'API data.gouv.fr
    try:
        resp = requests.get(
            f"{DATAGOUV_API}{DATAGOUV_DATASET_ID}/",
            timeout=TIMEOUT,
            headers={'User-Agent': 'GSF-Veille/2.0'}
        )
        resp.raise_for_status()
        dataset = resp.json()
        resources = dataset.get('resources', [])
        log.info(f"Dataset VigiEau : {len(resources)} ressources trouvées")
    except Exception as e:
        log.error(f"VigiEau history — impossible de lister les ressources : {e}")
        return existing or {}

    # Identifier les CSV arrêtés par année
    csv_par_annee = {}
    for res in resources:
        title  = res.get('title', '')
        format = res.get('format', '').lower()
        url    = res.get('latest', '') or res.get('url', '')

        if format not in ('csv', 'text/csv') and not url.endswith('.csv'):
            continue

        # "Arrêtés" = année en cours, "Arrêtés 2020" = historique
        if title.strip() == 'Arrêtés':
            csv_par_annee[current_year] = url
        else:
            for y in VIGIEAU_HISTORY_YEARS:
                if str(y) in title and 'Arrêtés' in title:
                    csv_par_annee[y] = url
                    break

    log.info(f"CSV identifiés : {sorted(csv_par_annee.keys())}")

    # Télécharger uniquement ce qui manque ou l'année en cours
    for year in sorted(csv_par_annee.keys()):
        url = csv_par_annee[year]
        year_str = str(year)

        # Skip les années passées déjà en cache
        if year_str in annees_cache and year != current_year:
            log.info(f"Année {year} : cache OK, skip")
            continue

        try:
            log.info(f"Téléchargement CSV {year} : {url}")
            r = requests.get(url, timeout=120, headers={'User-Agent': 'GSF-Veille/2.0'})
            r.raise_for_status()
            # Décoder proprement (le CSV peut être en latin-1)
            try:
                content = r.content.decode('utf-8')
            except UnicodeDecodeError:
                content = r.content.decode('latin-1')

            stats = _parse_vigieau_csv(content, year)
            annees_cache[year_str] = stats
            log.info(f"Année {year} : {len(stats['par_mois'])} mois, {len(stats['par_dept'])} depts")

        except Exception as e:
            log.warning(f"VigiEau CSV {year} erreur : {e}")

    # Calculer le top 10 départements toutes années confondues
    dept_total = {}
    for year_str, data in annees_cache.items():
        for code, info in data.get('par_dept', {}).items():
            if code not in dept_total:
                dept_total[code] = {'nom': info['nom'], 'total_graves': 0,
                                    'jours': {n: 0 for n in NIVEAUX_GRAVITE}}
            dept_total[code]['total_graves'] += info.get('total_graves', 0)
            for niv in NIVEAUX_GRAVITE:
                dept_total[code]['jours'][niv] += info['jours'].get(niv, 0)

    top10 = sorted(dept_total.items(), key=lambda x: x[1]['total_graves'], reverse=True)[:10]
    top10_out = [{'code': c, 'nom': d['nom'], 'total_graves': d['total_graves'],
                  'jours': d['jours']} for c, d in top10]

    result = {
        'updated_at' : datetime.now().isoformat(),
        'annees'     : annees_cache,
        'top10_depts': top10_out,
    }

    # Écrire le cache
    history_file.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    log.info(f"vigieau_history.json écrit — {len(annees_cache)} années, top10 calculé")

    return result


# ─────────────────────────────────────────────
# ÉCRITURE JSON
# Le commit/push est géré par le workflow GitHub Actions
# ─────────────────────────────────────────────

def write_output(json_data: dict, date_str: str):
    """Écrit les fichiers de sortie dans le workspace GitHub Actions."""
    data_dir = SCRIPT_DIR / 'data'
    data_dir.mkdir(exist_ok=True)

    day_file = data_dir / f'{date_str}.json'
    day_file.write_text(json.dumps(json_data, ensure_ascii=False, indent=2), encoding='utf-8')
    log.info(f"Écrit : {day_file}")

    archive_file = SCRIPT_DIR / 'archive.json'
    archive: dict = json.loads(archive_file.read_text()) if archive_file.exists() else {'dates': []}
    archive.setdefault('dates', [])
    if date_str not in archive['dates']:
        archive['dates'].append(date_str)
    archive['dates'] = sorted(set(archive['dates']), reverse=True)
    archive['updated_at'] = datetime.now().isoformat()

    # Nettoyage JSON > 90 jours
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

    archive_file.write_text(json.dumps(archive, ensure_ascii=False, indent=2), encoding='utf-8')
    log.info(f"archive.json mis à jour ({len(archive['dates'])} dates)")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main() -> int:
    log.info('=' * 60)
    log.info(f'Pipeline GSF Veille Environnementale — {TODAY}')
    log.info(f'Modèle LLM : {GROQ_MODEL} via Groq API')
    log.info('=' * 60)

    start  = datetime.now()
    errors = []
    items  = []
    stats  = {}
    restrictions = []
    jorf_autres  = []

    # 1. JORF
    try:
        jorf_items, jorf_autres, jorf_total = fetch_jorf()
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

    # 3. VigiEau
    try:
        restrictions = fetch_vigieau()
        stats['depts_restriction'] = len(restrictions)
    except Exception as e:
        log.error(f"VigiEau fatal : {e}")
        errors.append('VigiEau')
        stats['depts_restriction'] = 0

    # 4. VigiEau historique (data.gouv.fr — cache intelligent)
    try:
        fetch_vigieau_history()
    except Exception as e:
        log.error(f"VigiEau history fatal : {e}")
        errors.append("VigiEau_history")

    # Déduplication + tri criticité décroissante
    seen   = set()
    unique = []
    for item in sorted(items, key=lambda x: -x.get('criticite', 1)):
        if item['id'] not in seen:
            seen.add(item['id'])
            unique.append(item)

    for i, item in enumerate(unique):
        item['top5'] = i < 5

    # Générer le JSON
    elapsed = round((datetime.now() - start).total_seconds())
    json_data = {
        'date'            : TODAY,
        'generated_at'    : datetime.now().strftime('%H:%M'),
        'elapsed_seconds' : elapsed,
        'errors'          : errors,
        'stats'           : stats,
        'items'           : unique,
        'restrictions_eau': restrictions,
        'jo_autres'       : jorf_autres,
    }

    log.info(f"JSON : {len(unique)} items, {len(restrictions)} départements eau")

    # Écriture fichiers (commit géré par le workflow)
    write_output(json_data, TODAY)

    duration = (datetime.now() - start).total_seconds()
    status   = 'OK' if not errors else 'PARTIEL'
    log.info(f"Pipeline terminé en {duration:.1f}s — {status}")

    return 0 if not errors else 1


if __name__ == '__main__':
    sys.exit(main())
