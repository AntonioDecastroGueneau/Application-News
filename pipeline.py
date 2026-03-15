#!/usr/bin/env python3
"""
Veille Environnementale GSF — Pipeline quotidien
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
GHPAGES_WORKTREE = Path('/tmp/gsf-veille-ghpages')
LOG_PATH     = SCRIPT_DIR / 'pipeline.log'

JORF_BASE_URL     = 'https://echanges.dila.gouv.fr/OPENDATA/JORF/'
VIGIEAU_DEPTS_URL = 'https://api.vigieau.gouv.fr/api/departements'

# Mots-clés de pertinence GSF
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

# Mots-clés stricts pour le pré-filtre JORF (Option B)
# Seuls les textes dont le titre contient l'une de ces expressions passent à Ollama
JORF_KEYWORDS_STRICT = [
    # Activités cœur GSF — nettoyage / propreté
    'nettoyage industriel', 'nettoyage tertiaire', 'nettoyage des locaux',
    'propreté industrielle', 'agent de propreté', 'entreprise de propreté',
    'branche propreté', 'convention collective', 'propreté et services',
    'désinfection', 'décontamination', 'hygiène des locaux',
    # Déchets
    'déchet dangereux', 'déchets dangereux', 'déchet industriel', 'déchets industriels',
    'DASRI', 'déchet infectieux', 'déchet de soins',
    'collecte de déchets', 'traitement de déchets', 'élimination de déchets',
    'responsabilité élargie du producteur', 'filière REP',
    # Produits chimiques
    'biocide', 'détergent', 'substance dangereuse', 'produit chimique',
    'CMR', 'composé organique volatil', 'COV', 'solvant',
    'REACH', 'CLP', 'fiche de données de sécurité', 'FDS',
    # ICPE / risques industriels
    'installation classée', 'ICPE', 'SEVESO', 'rubrique ICPE',
    # Nucléaire / milieux contrôlés
    'radioprotection', 'zone contrôlée', 'zone surveillée',
    # Amiante / plomb
    'amiante', 'désamiantage', 'plomb', 'saturnisme',
]

OLLAMA_URL   = 'http://localhost:11434/api/generate'
OLLAMA_MODEL = 'mistral'
OLLAMA_OK    = None  # None = pas encore testé, True/False après check

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
    # ── Nouvelles sources ajoutées ──────────────────────────────────
    {
        'name': 'INERIS',
        'url': 'https://www.ineris.fr/rss.xml',
        'categorie': 'Réglementation',
        # Tout le contenu INERIS est pertinent (risques industriels, ICPE,
        # substances dangereuses, PFAS) — pas de filtre supplémentaire
    },
    {
        'name': 'ANSES',
        'url': 'https://www.anses.fr/fr/rss.xml',
        'categorie': 'Réglementation',
        # Flux très actif mais mélange biocides/REACH/eau et vétérinaire —
        # require_keywords exclut les AMM vétérinaires hors scope GSF
        'require_keywords': [
            'biocide', 'reach', 'clp', 'substance chimique', 'produit chimique',
            'pfas', 'perturbateur endocrinien', 'pesticide', 'phytosanitaire',
            'eau potable', 'eau souterraine', 'déchet', 'icpe', 'air intérieur',
            'nanomatériau', 'amiante', 'plomb', 'solvant',
        ],
    },
    {
        'name': 'BRGM Eaux souterraines',
        'url': 'https://www.brgm.fr/fr/rss/term/eau-souterraine-preservation-ressource',
        'categorie': 'Eau',
        # Flux thématique ciblé : état des nappes phréatiques,
        # risque contamination sites industriels
    },
    {
        'name': 'Euractiv France',
        'url': 'https://www.euractiv.fr/feed/',
        'categorie': 'Réglementation',
        # Très généraliste (toute la politique UE) — filtre strict
        # pour ne garder que les textes réglementaires environnement/énergie
        'require_keywords': [
            'règlement', 'directive', 'reach', 'taxonomie', 'csrd',
            'déchet', 'émission', 'pollution', 'icpe', 'climat',
            'énergie', 'renouvelable', 'carbone', 'neutralité',
            'biocide', 'substance', 'chimique', 'eau',
        ],
    },
    {
        'name': 'FNE',
        'url': 'https://fne.asso.fr/flux.rss',
        'categorie': 'Presse',
        # Source militante — utile pour anticiper contentieux ICPE
        # et pressions réglementaires issues de la société civile
        'require_keywords': [
            'icpe', 'installation classée', 'déchet', 'pesticide',
            'chimique', 'nucléaire', 'polluti', 'eau', 'amiante',
            'seveso', 'industriel', 'recours', 'tribunal', 'arrêté',
        ],
    },
    # Légifrance RSS supprimé (403) — couvert par le JORF DILA
    # ECHA (REACH UE) — 403 sur le flux RSS public
    # AIDA INERIS — authentification requise
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
# OLLAMA
# ─────────────────────────────────────────────

def check_ollama() -> bool:
    """Vérifie si Ollama est disponible (timeout court)."""
    global OLLAMA_OK
    if OLLAMA_OK is not None:
        return OLLAMA_OK
    try:
        resp = requests.get('http://localhost:11434/', timeout=5)
        OLLAMA_OK = resp.status_code == 200
    except Exception:
        OLLAMA_OK = False
    log.info(f"Ollama {'disponible' if OLLAMA_OK else 'indisponible — mode dégradé (résumés automatiques)'}")
    return OLLAMA_OK


def call_ollama(prompt: str) -> str:
    """Appelle Ollama (Mistral local) et retourne la réponse texte."""
    if not check_ollama():
        return ''
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={'model': OLLAMA_MODEL, 'prompt': prompt, 'stream': False},
            timeout=180,
        )
        resp.raise_for_status()
        return resp.json().get('response', '').strip()
    except requests.exceptions.Timeout:
        # Timeout sur un article isolé : on log mais on ne désactive pas Ollama
        log.warning(f"Ollama timeout (180s) sur cet article — ignoré, Ollama reste actif")
        return ''
    except Exception as e:
        # Erreur réseau/connexion : Ollama probablement down, on passe en mode dégradé
        global OLLAMA_OK
        OLLAMA_OK = False
        log.warning(f"Ollama indisponible : {e} — mode dégradé pour la suite")
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


def ollama_impact_gsf(titre: str, contenu: str) -> dict:
    """Évalue la pertinence et l'impact d'un texte JO pour GSF."""
    prompt = (
        "CONTEXTE : GSF est le 2e groupe français de nettoyage et propreté (42 000 salariés, 1,6 Md€ CA). "
        "GSF intervient dans : usines agroalimentaires, industrie, nucléaire, pharmaceutique, hôpitaux, "
        "bureaux, surfaces de vente, transports. "
        "GSF gère aussi des déchets industriels, utilise des produits chimiques "
        "(détergents, désinfectants, biocides) et exploite des installations ICPE.\n\n"

        "RÈGLE 1 — EXCLUSIONS ABSOLUES (répondre pertinent=false sans exception) :\n"
        "- Textes sur des professions réglementées (médecins, avocats, notaires, vétérinaires, pharmaciens...)\n"
        "- Nominations, mutations, retraites, concours de la fonction publique\n"
        "- Régies de recettes ou d'avances, finances publiques, fiscalité\n"
        "- Défense nationale, armées, police, justice\n"
        "- Sécurité sociale, assurance maladie, prestations sociales\n"
        "- Urbanisme, permis de construire, cadastre\n"
        "- Agriculture, sylviculture, pêche (sauf si lien explicite avec nettoyage ou déchets)\n\n"

        "RÈGLE 2 — PERTINENT pour GSF UNIQUEMENT si le texte modifie directement :\n"
        "- Les règles sur les produits biocides, détergents, CMR, solvants, REACH\n"
        "- La réglementation ICPE (nouvelles rubriques, seuils, obligations déclaratives)\n"
        "- La gestion, collecte ou traitement des déchets industriels ou dangereux\n"
        "- Les normes d'hygiène dans les secteurs où GSF travaille (agroalimentaire, pharma, santé, nucléaire)\n"
        "- Les obligations employeur en santé-sécurité des agents de nettoyage (TMS, produits chimiques, EPI)\n"
        "- La réglementation sur les rejets aqueux, les eaux usées industrielles\n"
        "- Les conventions collectives ou accords de branche propreté\n\n"

        "RÈGLE 3 — EN CAS DE DOUTE : pertinent=false. Ne jamais inventer de lien indirect.\n\n"

        f"TITRE : {titre}\n"
        f"EXTRAIT : {contenu[:600]}\n\n"

        "Applique d'abord la RÈGLE 1. Si exclusion → pertinent=false immédiatement.\n"
        "Sinon applique la RÈGLE 2. Si aucun point ne correspond exactement → pertinent=false.\n\n"
        "Réponds UNIQUEMENT en JSON valide, sans texte avant ou après :\n"
        '{"pertinent": true/false, "score": 1, "resume": "2 phrases max si pertinent, sinon vide"}\n'
        "score : 1=veille, 2=à surveiller, 3=obligation directe sur opérations GSF"
    )
    raw = call_ollama(prompt)
    result = extract_json(raw)
    return result if result else {'pertinent': False, 'score': 1, 'resume': ''}


def ollama_summarise(titre: str, contenu: str) -> dict:
    """Résume un article de presse et évalue son score pour GSF."""
    prompt = (
        "Tu travailles pour GSF, entreprise de propreté et services.\n\n"
        f"TITRE : {titre}\n"
        f"CONTENU : {contenu[:800]}\n\n"
        "Réponds UNIQUEMENT en JSON valide :\n"
        '{"resume": "2 phrases max", "score": 1}\n'
        "score : 1=intéressant, 2=important, 3=impact direct GSF"
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
        resp = requests.get(url, timeout=TIMEOUT, headers={'User-Agent': 'GSF-Veille/1.0'})
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


def get_today_jorf_url():
    """Retourne l'URL du fichier JORF le plus récent pour aujourd'hui."""
    files = list_jorf_files()
    today_compact = datetime.now().strftime('%Y%m%d')
    today_files = [f for f in files if today_compact in f]
    chosen = today_files[-1] if today_files else (files[-1] if files else None)
    if chosen:
        log.info(f"Fichier JORF sélectionné : {chosen}")
        return urljoin(JORF_BASE_URL, chosen)
    return None


def parse_jorf_xml(content: bytes):
    """Parse un fichier XML JORF (format DILA) et extrait les articles filtrés par mots-clés.

    Structure DILA :
      <SECTION_TA>
        <TITRE_TA>...</TITRE_TA>
        <CONTEXTE>
          <TEXTE nor="TECK..." nature="ARRETE" ministere="..." date_publi="YYYY-MM-DD">
            <TITRE_TXT>Arrêté du …</TITRE_TXT>
          </TEXTE>
        </CONTEXTE>
      </SECTION_TA>
    """
    import xml.etree.ElementTree as ET
    articles = []
    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        log.debug(f"XML parse error : {e}")
        return articles

    seen_nor = set()

    # Chercher tous les éléments TEXTE portant un attribut nor (NOR du texte)
    for texte in root.iter('TEXTE'):
        nor = texte.get('nor', '').strip()

        # Titre dans <TITRE_TXT>
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
        cid       = texte.get('cid', '')  # JORFTEXT000... — vrai ID Légifrance

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
    """Télécharge le JORF, filtre et analyse via Ollama. Retourne (pertinents, autres, nb_analysés)."""
    log.info("=== JORF ===")
    items, autres, total_analysed = [], [], 0

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

            # ── Option B : pré-filtre strict avant Ollama ──────────────
            def jorf_strict_match(titre: str) -> bool:
                t = titre.lower()
                return any(k.lower() in t for k in JORF_KEYWORDS_STRICT)

            gsf_candidates = [a for a in all_articles if jorf_strict_match(a['titre'])]
            rest = [a for a in all_articles if not jorf_strict_match(a['titre'])]
            log.info(f"JORF : {len(gsf_candidates)} candidats GSF après pré-filtre strict, {len(rest)} autres")

            # Les textes hors-filtre vont directement dans "autres"
            for art in rest:
                autres.append({
                    'nor'      : art.get('nor', ''),
                    'titre'    : art['titre'],
                    'nature'   : art.get('contenu', '').split(' — ')[0],
                    'ministere': art.get('contenu', '').split(' — ')[1] if ' — ' in art.get('contenu', '') else '',
                    'url'      : art.get('url', ''),
                    'date'     : art.get('date', TODAY),
                })

            # Les candidats GSF → Ollama pour résumé uniquement (pas de filtre)
            for art in gsf_candidates:
                try:
                    contenu = art['contenu']
                    if art.get('url') and 'legifrance' in art.get('url', ''):
                        full = crawl_article(art['url'])
                        if full:
                            contenu = full

                    analysis = ollama_summarise(art['titre'], contenu)

                    items.append({
                        'id': make_id('JORF', art['titre']),
                        'source': 'JORF',
                        'categorie': categorise(art['titre'] + ' ' + contenu),
                        'titre': art['titre'],
                        'resume': analysis.get('resume') or art['titre'],
                        'criticite': int(analysis.get('score', 2)),
                        'impact_gsf': True,
                        'url': art.get('url', ''),
                        'date': TODAY,
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

            # Date de publication réelle de l'article
            pub = entry.get('published_parsed') or entry.get('updated_parsed')
            if pub:
                import time as _time
                try:
                    article_date = datetime.fromtimestamp(_time.mktime(pub)).strftime('%Y-%m-%d')
                except Exception:
                    article_date = TODAY
            else:
                article_date = TODAY

            # Exclure les articles de plus de 2 jours
            cutoff_rss = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
            if article_date < cutoff_rss:
                continue

            # Enrichissement si contenu insuffisant
            if len(contenu) < 100 and url:
                contenu = crawl_article(url) or contenu

            if not keyword_match(titre + ' ' + contenu):
                continue

            # Filtre strict par source si défini (ex: ANSES, Euractiv)
            require_kw = source.get('require_keywords')
            if require_kw:
                text_lower = (titre + ' ' + contenu).lower()
                if not any(kw.lower() in text_lower for kw in require_kw):
                    continue

            analysis = ollama_summarise(titre, contenu)
            items.append({
                'id': make_id(name, titre),
                'source': name,
                'categorie': categorise(titre + ' ' + contenu),
                'titre': titre,
                'resume': analysis.get('resume') or titre,
                'criticite': int(analysis.get('score', 1)),
                'impact_gsf': int(analysis.get('score', 1)) >= 2,
                'url': url,
                'date': article_date,
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
                        'impact_gsf': False,
                        'url': source['fallback_crawl'],
                        'date': TODAY,
                    })
            except Exception as e2:
                log.warning(f"Fallback {name} error : {e2}")

    return items


def fetch_rss():
    """Collecte et agrège tous les flux RSS."""
    log.info("=== RSS ===")
    all_items = []
    for source in RSS_SOURCES:
        items = fetch_rss_source(source)
        all_items.extend(items)
        log.info(f"RSS {source['name']} : {len(items)} retenus")
    return all_items


# ─────────────────────────────────────────────
# VIGIEAU — RESTRICTIONS EAU
# ─────────────────────────────────────────────

def fetch_vigieau():
    """Récupère les restrictions eau par département via l'API VigiEau."""
    log.info("=== VigiEau ===")
    restrictions = []

    try:
        resp = requests.get(VIGIEAU_DEPTS_URL, timeout=TIMEOUT)
        resp.raise_for_status()

        for dept in resp.json():
            niv = dept.get('niveauGraviteMax')
            if not niv:
                continue  # null = aucune restriction active

            restrictions.append({
                'dept_code': dept.get('code', ''),
                'dept_nom': dept.get('nom', ''),
                'niveau': niv,
                'niveauSup': dept.get('niveauGraviteSupMax'),  # eaux de surface
                'niveauSou': dept.get('niveauGraviteSouMax'),  # eaux souterraines
                'niveauAep': dept.get('niveauGraviteAepMax'),  # eau potable
            })

        restrictions.sort(
            key=lambda x: NIVEAUX_ORDRE.get(x['niveau'], 0),
            reverse=True,
        )
        log.info(f"VigiEau : {len(restrictions)} départements en restriction")

    except Exception as e:
        log.error(f"VigiEau fatal : {e}", exc_info=True)

    return restrictions


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
        archive: dict = json.loads(archive_file.read_text()) if archive_file.exists() else {'dates': [], 'counts': {}}
        archive.setdefault('dates', [])
        archive.setdefault('counts', {})
        if date_str not in archive['dates']:
            archive['dates'].append(date_str)
        archive['dates'] = sorted(set(archive['dates']), reverse=True)
        archive['counts'][date_str] = len(json_data.get('items', []))
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
    log.info(f'Pipeline GSF Veille Environnementale — {TODAY}')
    log.info('=' * 60)

    start   = datetime.now()
    errors  = []
    items   = []
    stats   = {}
    restrictions = []

    # 1. JORF
    try:
        jorf_items, jorf_autres, jorf_total = fetch_jorf()
        items.extend(jorf_items)
        stats['jo_analyses'] = jorf_total
        stats['jo_retenus']  = len(jorf_items)
    except Exception as e:
        log.error(f"JORF fatal : {e}")
        errors.append('JORF')
        jorf_autres = []
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

    # Déduplication + tri par criticité décroissante
    seen = set()
    unique = []
    for item in sorted(items, key=lambda x: -x.get('criticite', 1)):
        if item['id'] not in seen:
            seen.add(item['id'])
            unique.append(item)

    # Marquer le top 5 par criticité (signaux forts du jour)
    for i, item in enumerate(unique):
        item['top5'] = i < 5

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
        'jo_autres': jorf_autres,
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
