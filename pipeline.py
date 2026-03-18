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
GROQ_MODEL      = 'llama-3.3-70b-versatile'
GROQ_MAX_RETRY  = 4
GROQ_RETRY_WAIT = 20

# ─────────────────────────────────────────────
# DESCRIPTION GSF — injectée dans tous les prompts LLM
# ─────────────────────────────────────────────

GSF_CONTEXT = """
GSF est un groupe français de propreté et de services aux entreprises (42 000 salariés,
~1,27 Md€ de CA en 2023), positionné comme acteur majeur du soft FM (facility management).

Ce que fait GSF :
- Nettoyage et propreté des locaux : bureaux, sites tertiaires, industriels, milieux
  sensibles (santé, agroalimentaire, chimie, pétrochimie, nucléaire).
- Propreté industrielle : décapage et traitement des sols, nettoyage de lignes de
  production, interventions en environnements à risques (certifications MASE).
- Gestion des déchets : collecte interne, tri, gestion déléguée des flux, optimisation
  des filières et des coûts sur les sites clients.
- Entretien des espaces verts : gestion paysagère des abords de sites industriels,
  tertiaires ou logistiques.
- Services de logistique interne : manutention légère, gestion de flux, préparation de
  commandes et de salles.
- Soft FM / services généraux : accueil et hospitality management, courrier et gestion
  documentaire, factotum / petits travaux (maintenance de premier niveau).

Comment GSF opère :
- Environ 160 établissements en France, maillage territorial dense.
- Modèle multiservices et multisectoriel, solutions sur mesure pour chaque client.
- Utilise des biocides, détergents, produits CMR et solvants dans ses prestations.
- Exploite des sites classés ICPE (installations classées pour la protection
  de l'environnement).
- Gère une flotte de véhicules (électrification en cours).
- Engagé dans une trajectoire de décarbonation : BGES annuel, plan climat,
  suivi des risques climatiques sur ses 3 000+ sites clients.
""".strip()


# ─────────────────────────────────────────────
# KEYWORDS — premier filtre large (présence dans titre/contenu)
# ─────────────────────────────────────────────

KEYWORDS = [
    # Réglementation environnementale opérationnelle
    'ICPE', 'installation classée', 'eau', 'rejet aqueux', 'émissions',
    'biodiversité', 'espèce protégée', 'énergie', 'déchet', 'déchets',
    'pollution', 'environnement', 'écologique', 'REACH', 'biocide',
    'bruit', 'sol', 'air', 'risque industriel', 'Natura 2000',
    'nettoyage', 'propreté', 'entretien', 'désinfection',
    'produit chimique', 'COV', 'composé organique volatil',
    'SEVESO', 'PPR', 'DPE', 'amiante', 'plomb', 'REP',
    'responsabilité élargie', 'VHU', 'bâtiment tertiaire',
    # Climat & décarbonation stratégique
    'climat', 'climatique', 'réchauffement', 'décarbonation', 'carbone',
    'GES', 'gaz à effet de serre', 'neutralité carbone', 'net zéro',
    'SNBC', 'PNACC', 'adaptation', 'mitigation', 'atténuation',
    'trajectoire', 'transition énergétique', 'transition écologique',
    'CSRD', 'reporting extra-financier', 'taxonomie verte',
    'Accord de Paris', 'COP', 'GIEC', 'IPCC',
    'catastrophe naturelle', 'inondation', 'sécheresse', 'canicule',
    'événement extrême', 'submersion', 'feu de forêt',
    'décarboner', 'bas-carbone', 'scope 1', 'scope 2', 'scope 3',
    'bilan carbone', 'empreinte carbone', 'plan climat',
    'France 2030', 'loi énergie', 'loi climat',
]


# ─────────────────────────────────────────────
# SOURCES RSS
# ─────────────────────────────────────────────

RSS_SOURCES = [
    # ── Presse environnement généraliste ─────────────────────────────
    {
        'name': 'Actu-Environnement',
        'url': 'https://www.actu-environnement.com/flux/rss/environnement/',
        'categorie': 'Presse',
        'fallback_crawl': 'https://www.actu-environnement.com/ae/news/',
    },
    {
        'name': 'Reporterre',
        'url': 'https://reporterre.net/spip.php?page=backend',
        'categorie': 'Presse',
        'fallback_crawl': 'https://reporterre.net/',
    },
    {
        'name': 'Novethic',
        'url': 'https://www.novethic.fr/feed',
        'categorie': 'Presse',
        'fallback_crawl': 'https://www.novethic.fr/',
    },
    # ── Institutions françaises climat ───────────────────────────────
    {
        'name': 'Min. Transition Écologique',
        'url': 'https://www.ecologie.gouv.fr/rss-actualites.xml',
        'categorie': 'Réglementation',
        'fallback_crawl': 'https://www.ecologie.gouv.fr/actualites',
    },
    {
        'name': 'ADEME',
        'url': 'https://www.ademe.fr/feed/',
        'categorie': 'Climat',
        'fallback_crawl': 'https://www.ademe.fr/actualites/',
    },
    {
        'name': 'Haut Conseil pour le Climat',
        'url': 'https://www.hautconseilclimat.fr/feed/',
        'categorie': 'Climat',
        'fallback_crawl': 'https://www.hautconseilclimat.fr/actualites/',
    },
    {
        'name': 'France Stratégie',
        'url': 'https://www.strategie.gouv.fr/rss.xml',
        'categorie': 'Climat',
        'fallback_crawl': 'https://www.strategie.gouv.fr/publications',
        'require_keywords': [
            'climat', 'carbone', 'transition', 'énergie', 'décarbonation',
            'environnement', 'adaptation', 'neutralité', 'SNBC', 'empreinte',
            'biodiversité', 'trajectoire', 'bas-carbone', 'GES',
        ],
    },
    {
        'name': 'Vie-publique.fr',
        'url': 'https://www.vie-publique.fr/rss/actualites.xml',
        'categorie': 'Réglementation',
        'fallback_crawl': 'https://www.vie-publique.fr/loi',
        'require_keywords': [
            'climat', 'énergie', 'transition', 'environnement', 'carbone',
            'décarbonation', 'renouvelable', 'biodiversité', 'CSRD',
            'adaptation', 'neutralité', 'trajectoire', 'émissions',
        ],
    },
    # ── Think tanks & recherche climat ──────────────────────────────
    {
        'name': 'The Shift Project',
        'url': 'https://theshiftproject.org/feed/',
        'categorie': 'Climat',
        'fallback_crawl': 'https://theshiftproject.org/articles/',
    },
    {
        'name': 'I4CE',
        'url': 'https://www.i4ce.org/feed/',
        'categorie': 'Climat',
        'fallback_crawl': 'https://www.i4ce.org/publications/',
        'require_keywords': [
            'climat', 'carbone', 'transition', 'financement', 'investissement',
            'décarbonation', 'politique climatique', 'trajectoire', 'adaptation',
        ],
    },
    {
        'name': 'Carbone 4',
        'url': 'https://www.carbone4.com/feed',
        'categorie': 'Climat',
        'fallback_crawl': 'https://www.carbone4.com/publications',
    },
    # ── Europe & politique ───────────────────────────────────────────
    {
        'name': 'Politico Energy EU',
        'url': 'https://www.politico.eu/section/energy-fr/feed/',
        'categorie': 'Réglementation',
        'fallback_crawl': 'https://www.politico.eu/section/energy-fr/',
        'require_keywords': [
            'climat', 'énergie', 'carbone', 'taxonomie', 'CSRD', 'Green Deal',
            'transition', 'renouvelable', 'émissions', 'règlement', 'directive',
        ],
    },
    {
        'name': 'Contexte Environnement',
        'url': 'https://www.contexte.com/articles/rss/edition/environnement',
        'categorie': 'Réglementation',
        'fallback_crawl': 'https://www.contexte.com/environnement/',
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
    if any(k in t for k in ['snbc', 'pnacc', 'accord de paris', 'cop ', 'giec', 'ipcc',
                             'neutralité carbone', 'net zéro', 'trajectoire carbone',
                             'plan national adaptation', 'stratégie nationale bas-carbone',
                             'canicule', 'inondation', 'feu de forêt', 'submersion',
                             'événement extrême', 'catastrophe climatique', 'réchauffement',
                             'catastrophe naturelle']):
        return 'Climat'
    if any(k in t for k in ['csrd', 'taxonomie', 'reporting durabilité', 'devoir de vigilance',
                             'décarbonation', 'bilan carbone', 'scope', 'bas-carbone',
                             'transition énergétique', 'transition écologique']):
        return 'Climat'
    if any(k in t for k in ['icpe', 'installation classée', 'seveso', 'autorisation', 'enregistrement']):
        return 'ICPE'
    if any(k in t for k in ['eau', 'rejet', 'assainissement', 'captage', 'nappe', 'sécheresse']):
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
# GROQ
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


def call_groq(prompt: str, system: str = '', max_tokens: int = 300) -> str:
    """Appelle l'API Groq avec retry sur erreur 429 (rate limit)."""
    client = get_groq_client()
    sys_content = system if 'json' in system.lower() else (system + ' Reponds en JSON.').strip()
    messages = [
        {'role': 'system', 'content': sys_content},
        {'role': 'user',   'content': prompt},
    ]
    for attempt in range(1, GROQ_MAX_RETRY + 1):
        try:
            resp = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=messages,
                max_tokens=max_tokens,
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


# ─────────────────────────────────────────────
# LLM — ANALYSE JORF
# Un seul passage Groq par texte, pas de double filtre.
# Le LLM est le seul juge de pertinence.
# ─────────────────────────────────────────────

def groq_analyse_jorf(titre: str, contenu: str) -> dict:
    """
    Évalue un texte du JORF pré-filtré par mots-clés larges.

    Le LLM décide seul de la pertinence pour GSF, sans liste stricte en amont.
    Retourne :
        pertinent  : bool
        resume     : str   — ce que ça change pour GSF (1 phrase, si pertinent)
        pourquoi   : str   — pourquoi c'est un signal important (si score >= 2)
        score      : int   — 1 (veille) | 2 (à anticiper) | 3 (obligation/risque immédiat)
    """
    system = 'Tu es juriste RSE senior conseillant le Responsable Climat de GSF. JSON uniquement.'
    prompt = (
        f"{GSF_CONTEXT}\n\n"
        "Ce texte du Journal Officiel a passé un premier filtre par mots-clés environnementaux.\n"
        "Détermine s'il est pertinent pour le Responsable Climat & Environnement de GSF.\n\n"
        "PERTINENT si le texte concerne directement :\n"
        "  - Obligations réglementaires (ICPE, biocides, REACH, déchets, CMR, amiante, plomb)\n"
        "  - Convention collective propreté / conditions de travail\n"
        "  - Politique climatique nationale ou européenne (SNBC, PNACC, CSRD, taxonomie, loi climat)\n"
        "  - Risques climatiques : arrêtés CatNat, PPR, sécheresse, inondations, événements extrêmes\n"
        "    (utile pour le suivi des sites GSF exposés et le dashboard risques climatiques)\n"
        "  - Transition énergétique, décarbonation, bilan carbone, reporting durabilité\n"
        "  - Eau : restrictions, qualité, rejets aqueux\n"
        "  - Biodiversité, air, bruit sur sites industriels\n\n"
        "NON PERTINENT si le texte concerne exclusivement : agriculture/élevage, pêche, défense,\n"
        "médecine humaine sans lien avec les sites GSF, enseignement, culture, sport,\n"
        "BTP résidentiel, finance de marché — même si 'nettoyage' ou 'environnement' y apparaît.\n\n"
        "EN CAS DE DOUTE : pertinent = true.\n\n"
        "Score :\n"
        "  1 = information de veille, tendance à connaître\n"
        "  2 = évolution réglementaire ou politique à anticiper pour GSF\n"
        "  3 = obligation immédiate, risque direct ou décision stratégique urgente\n\n"
        f"TITRE: {titre}\n"
        f"CONTENU: {contenu[:500]}\n\n"
        'JSON: {"pertinent": true, "resume": "ce que ça change pour GSF en 1 phrase", '
        '"pourquoi": "pourquoi c\'est un signal important (obligatoire si score >= 2, sinon vide)", '
        '"score": 2}'
    )
    raw = call_groq(prompt, system, max_tokens=350)
    result = extract_json(raw)
    return result if result else {'pertinent': False, 'resume': '', 'pourquoi': '', 'score': 1}


# ─────────────────────────────────────────────
# LLM — ANALYSE RSS / PRESSE
# ─────────────────────────────────────────────

def _gsf_est_pertinent(titre: str, contenu: str) -> bool:
    """Filtre binaire rapide : l'article vaut-il la peine d'être analysé ?"""
    system = 'Tu es un filtre. Reponds uniquement {"ok": true} ou {"ok": false}.'
    prompt = (
        f"{GSF_CONTEXT}\n\n"
        "GARDER si le sujet concerne :\n"
        "- Politique climatique : lois, plans gouvernementaux (SNBC, PNACC, loi climat, loi énergie)\n"
        "- Trajectoires décarbonation : objectifs nationaux/européens, net zero, neutralité carbone\n"
        "- Rapports et études : GIEC/IPCC, Haut Conseil Climat, France Stratégie, ADEME, I4CE, Shift Project\n"
        "- Réglementation ESG/RSE : CSRD, taxonomie verte, bilan carbone, reporting extra-financier\n"
        "- Événements climatiques : canicule, inondation, sécheresse, feu de forêt, submersion\n"
        "- Adaptation climatique : risques entreprises, résilience, vulnérabilité sectorielle\n"
        "- Réglementation environnementale : ICPE, REACH, déchets, air, eau, biodiversité\n"
        "- Transition énergétique : renouvelables, efficacité énergétique, scope 1/2/3\n\n"
        "REJETER si CLAIREMENT : élections/partis politiques, guerre/géopolitique sans lien climatique,\n"
        "immobilier résidentiel, agriculture/élevage, finance de marché, faits divers, sport, culture,\n"
        "santé humaine sans lien environnemental.\n"
        "EN CAS DE DOUTE : garder (ok: true).\n\n"
        f"TITRE: {titre}\n"
        f"DEBUT: {contenu[:200]}\n\n"
        'Reponds: {"ok": true} ou {"ok": false}'
    )
    raw = call_groq(prompt, system)
    result = extract_json(raw)
    return result.get('ok', True)


def _gsf_resumer(titre: str, contenu: str) -> dict:
    """
    Résume et score un article pertinent.
    Retourne resume, pourquoi (si score >= 2) et score.
    """
    system = 'Analyste climat et environnement GSF. JSON valide uniquement.'
    prompt = (
        f"{GSF_CONTEXT}\n\n"
        "Ta mission : suivre la politique climatique, la réglementation environnementale,\n"
        "les trajectoires de décarbonation et les risques climatiques.\n\n"
        "Score :\n"
        "  1 = information de veille, tendance à connaître\n"
        "  2 = évolution réglementaire ou politique à anticiper pour GSF\n"
        "  3 = obligation immédiate, risque direct ou décision stratégique urgente pour GSF\n\n"
        f"TITRE: {titre}\n"
        f"CONTENU: {contenu[:600]}\n\n"
        'JSON: {"resume": "1-2 phrases concises sur ce que ça change pour GSF", '
        '"pourquoi": "pourquoi c\'est un signal important (obligatoire si score >= 2, sinon vide)", '
        '"score": 1}'
    )
    raw = call_groq(prompt, system, max_tokens=350)
    result = extract_json(raw)
    return result if result else {'resume': titre[:200], 'pourquoi': '', 'score': 1}


def groq_analyse_rss(titre: str, contenu: str) -> dict:
    """2 appels Groq : filtre rapide, puis résumé complet si pertinent."""
    if not _gsf_est_pertinent(titre, contenu):
        log.debug(f'Exclu (non pertinent GSF) : {titre[:60]}')
        return {'pertinent': False, 'resume': '', 'pourquoi': '', 'score': 1}
    analysis = _gsf_resumer(titre, contenu)
    analysis['pertinent'] = True
    # Vider pourquoi si score = 1 (pas un signal fort)
    if int(analysis.get('score', 1)) < 2:
        analysis['pourquoi'] = ''
    return analysis


# ─────────────────────────────────────────────
# LLM — BRIEFING JORF
# ─────────────────────────────────────────────

_BRIEFING_SKIP = [
    'nomination', 'délégation de signature', 'désignation',
    'portant nomination', 'portant délégation', 'délégation de pouvoir',
    'cessation de fonctions',
]


def groq_briefing_jorf(articles: list) -> str:
    """Génère un briefing rédigé des textes JORF significatifs du jour."""
    if not articles:
        return ''
    candidates = [
        a for a in articles
        if not any(p in a['titre'].lower() for p in _BRIEFING_SKIP)
    ]
    if not candidates:
        return ''

    lines = []
    for i, a in enumerate(candidates[:80]):
        nature = a.get('contenu', '').split(' — ')[0] or 'Texte'
        lines.append(f"{i+1}. [{nature}] {a['titre']}")

    system = 'Tu es juriste senior conseillant un directeur RSE. Reponds uniquement en JSON valide.'
    prompt = (
        f"Textes publiés au Journal Officiel aujourd'hui ({TODAY}) :\n\n"
        + '\n'.join(lines)
        + "\n\nRédige un briefing TRÈS court (3-5 phrases max, ou 2-3 bullets si plusieurs sujets distincts) "
        "à destination d'un Responsable Climat et Environnement. "
        "Synthétise ce qui est réellement significatif : décrets d'application de lois majeures, "
        "arrêtés à portée sectorielle large, évolutions réglementaires importantes. "
        "Si les textes du jour sont mineurs ou purement administratifs, dis-le clairement en 1 phrase. "
        "NE copie PAS les titres. Rédige en français, style direct et concis. "
        'JSON: {"briefing": "texte rédigé ici"}'
    )
    raw = call_groq(prompt, system, max_tokens=400)
    result = extract_json(raw)
    if isinstance(result, dict) and 'briefing' in result:
        return result['briefing']
    log.warning("briefing_jorf: réponse Groq inattendue")
    return ''


# ─────────────────────────────────────────────
# CRAWL FALLBACK
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
#
# Architecture du filtre :
#   1. keyword_match() large → capte tout ce qui contient un terme environnemental
#   2. groq_analyse_jorf()   → le LLM décide de la pertinence et du score
#   Pas de liste stricte intermédiaire : le LLM est l'unique juge de pertinence.
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
    items, autres, total_analysed, briefing = [], [], 0, []

    try:
        url = get_today_jorf_url()
        if not url:
            log.warning("Aucun fichier JORF disponible")
            return items, autres, 0, briefing

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
            log.info(f"JORF : {total_analysed} textes pré-filtrés par keywords")

            # Briefing exécutif — sur l'ensemble des textes du jour
            briefing = groq_briefing_jorf(all_articles)
            log.info(f"JORF briefing : {'OK' if briefing else 'vide'} ({len(briefing)} cars)")

            # ── Analyse LLM de chaque texte — pas de liste stricte ──────────
            for art in all_articles:
                try:
                    contenu = art['contenu']
                    # Enrichissement : récupérer le texte complet si possible
                    if art.get('url') and 'legifrance' in art.get('url', ''):
                        full = crawl_article(art['url'])
                        if full:
                            contenu = full

                    analysis = groq_analyse_jorf(art['titre'], contenu)

                    if analysis.get('pertinent') is False:
                        # Texte non pertinent pour GSF → dans jo_autres (visible, non alerté)
                        autres.append({
                            'nor'      : art.get('nor', ''),
                            'titre'    : art['titre'],
                            'nature'   : art.get('contenu', '').split(' — ')[0],
                            'ministere': art.get('contenu', '').split(' — ')[1]
                                         if ' — ' in art.get('contenu', '') else '',
                            'url'      : art.get('url', ''),
                            'date'     : art.get('date', TODAY),
                        })
                        log.debug(f"JORF → autres : {art['titre'][:60]}")
                        continue

                    score = int(analysis.get('score', 2))
                    pourquoi = analysis.get('pourquoi', '')
                    # Vider pourquoi si score = 1 (veille simple, pas un signal)
                    if score < 2:
                        pourquoi = ''

                    items.append({
                        'id'        : make_id('JORF', art['titre']),
                        'source'    : 'JORF',
                        'categorie' : categorise(art['titre'] + ' ' + contenu),
                        'titre'     : art['titre'],
                        'resume'    : analysis.get('resume') or art['titre'],
                        'pourquoi'  : pourquoi,
                        'criticite' : score,
                        'impact_gsf': True,
                        'url'       : art.get('url', ''),
                        'date'      : TODAY,
                    })
                    log.debug(f"JORF retenu (score={score}) : {art['titre'][:60]}")

                except Exception as e:
                    log.debug(f"Erreur analyse JORF : {e}")

    except Exception as e:
        log.error(f"JORF fatal : {e}", exc_info=True)

    log.info(f"JORF : {len(items)} retenus / {total_analysed} analysés ({len(autres)} autres)")
    return items, autres, total_analysed, briefing


# ─────────────────────────────────────────────
# RSS / PRESSE
# ─────────────────────────────────────────────

def fetch_rss_source(source: dict):
    items = []
    name  = source['name']

    try:
        try:
            rss_resp = requests.get(
                source['url'],
                timeout=TIMEOUT,
                headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                                       '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'},
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

            # Filtre temporel : lundi → accepte depuis vendredi 18h, sinon 24h
            now = datetime.now()
            if now.weekday() == 0:
                cutoff_dt = (now - timedelta(days=3)).replace(
                    hour=18, minute=0, second=0, microsecond=0)
            else:
                cutoff_dt = now - timedelta(hours=24)
            if article_dt and article_dt < cutoff_dt:
                log.debug(f"Article trop ancien ({article_date}), exclu : {titre[:60]}")
                continue

            if len(contenu) < 100 and url:
                contenu = crawl_article(url) or contenu

            # Filtre source avec require_keywords (France Stratégie, Vie-publique, etc.)
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

            score    = int(analysis.get('score', 1))
            pourquoi = analysis.get('pourquoi', '')
            if score < 2:
                pourquoi = ''

            items.append({
                'id'        : make_id(name, titre),
                'source'    : name,
                'categorie' : categorise(titre + ' ' + contenu),
                'titre'     : titre,
                'resume'    : analysis.get('resume') or titre,
                'pourquoi'  : pourquoi,
                'criticite' : score,
                'impact_gsf': score >= 2,
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
                    analysis = groq_analyse_rss(f"Actualités {name}", contenu)
                    items.append({
                        'id'        : make_id(name, 'fallback'),
                        'source'    : name,
                        'categorie' : 'Presse',
                        'titre'     : f'Actualités {name}',
                        'resume'    : analysis.get('resume') or 'Source consultée via fallback.',
                        'pourquoi'  : '',
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

DATAGOUV_DATASET_ID   = 'donnee-secheresse-vigieau'
DATAGOUV_API          = 'https://www.data.gouv.fr/api/1/datasets/'
VIGIEAU_HISTORY_YEARS = list(range(2020, datetime.now().year + 1))

NIVEAUX_GRAVITE = ['vigilance', 'alerte', 'alerte renforcée', 'crise']

VIGIEAU_FALLBACK_URLS = {
    2026: 'https://www.data.gouv.fr/api/1/datasets/r/0732e970-c12c-4e6a-adca-5ac9dbc3fdfa',
}

DEPT_NOMS = {
    '01':'Ain','02':'Aisne','03':'Allier','04':'Alpes-de-Haute-Provence','05':'Hautes-Alpes',
    '06':'Alpes-Maritimes','07':'Ardèche','08':'Ardennes','09':'Ariège','10':'Aube',
    '11':'Aude','12':'Aveyron','13':'Bouches-du-Rhône','14':'Calvados','15':'Cantal',
    '16':'Charente','17':'Charente-Maritime','18':'Cher','19':'Corrèze','2A':'Corse-du-Sud',
    '2B':'Haute-Corse','21':"Côte-d'Or",'22':'Côtes-d\'Armor','23':'Creuse','24':'Dordogne',
    '25':'Doubs','26':'Drôme','27':'Eure','28':'Eure-et-Loir','29':'Finistère',
    '30':'Gard','31':'Haute-Garonne','32':'Gers','33':'Gironde','34':'Hérault',
    '35':'Ille-et-Vilaine','36':'Indre','37':'Indre-et-Loire','38':'Isère','39':'Jura',
    '40':'Landes','41':'Loir-et-Cher','42':'Loire','43':'Haute-Loire','44':'Loire-Atlantique',
    '45':'Loiret','46':'Lot','47':'Lot-et-Garonne','48':'Lozère','49':'Maine-et-Loire',
    '50':'Manche','51':'Marne','52':'Haute-Marne','53':'Mayenne','54':'Meurthe-et-Moselle',
    '55':'Meuse','56':'Morbihan','57':'Moselle','58':'Nièvre','59':'Nord',
    '60':'Oise','61':'Orne','62':'Pas-de-Calais','63':'Puy-de-Dôme','64':'Pyrénées-Atlantiques',
    '65':'Hautes-Pyrénées','66':'Pyrénées-Orientales','67':'Bas-Rhin','68':'Haut-Rhin','69':'Rhône',
    '70':'Haute-Saône','71':'Saône-et-Loire','72':'Sarthe','73':'Savoie','74':'Haute-Savoie',
    '75':'Paris','76':'Seine-Maritime','77':'Seine-et-Marne','78':'Yvelines','79':'Deux-Sèvres',
    '80':'Somme','81':'Tarn','82':'Tarn-et-Garonne','83':'Var','84':'Vaucluse',
    '85':'Vendée','86':'Vienne','87':'Haute-Vienne','88':'Vosges','89':'Yonne',
    '90':'Territoire de Belfort','91':'Essonne','92':'Hauts-de-Seine','93':'Seine-Saint-Denis',
    '94':'Val-de-Marne','95':"Val-d'Oise",'971':'Guadeloupe','972':'Martinique',
    '973':'Guyane','974':'La Réunion','976':'Mayotte',
}


def _normalize_niveau(niveau: str) -> str:
    n = niveau.lower().strip().replace('_', ' ')
    n = n.replace('renforcee', 'renforcée').replace('renforcé', 'renforcée')
    return n


def _parse_vigieau_csv(content: str, year: int, compute_daily: bool = False) -> dict:
    import csv as _csv
    import json as _json
    from collections import defaultdict

    par_mois = defaultdict(lambda: {n: 0 for n in NIVEAUX_GRAVITE})
    par_dept = defaultdict(lambda: {'nom': '', 'jours': {n: 0 for n in NIVEAUX_GRAVITE}})
    par_jour = defaultdict(lambda: {n: 0 for n in NIVEAUX_GRAVITE}) if compute_daily else None

    reader   = _csv.DictReader(io.StringIO(content), delimiter=',')
    headers  = reader.fieldnames or []
    log.debug(f"CSV colonnes ({year}): {headers[:15]}")

    rows_ok = rows_skip = 0

    for row in reader:
        try:
            date_debut_str = row.get('date_debut', '').strip()
            date_fin_str   = row.get('date_fin', '').strip()

            dept_code_raw = (
                row.get('departement', '') or row.get('departement_pilote', '') or ''
            ).strip().strip('"')
            dept_code = dept_code_raw.zfill(2) if dept_code_raw else ''

            niveau_raw_col = (
                row.get('zones_alerte.niveau_gravite', '')
                or row.get('niveau_gravite', '')
                or row.get('niveauGravite', '')
                or row.get('niveauAlerte', '')
                or ''
            ).strip()

            niveaux_liste = []
            if niveau_raw_col.startswith('['):
                try:
                    parsed = _json.loads(niveau_raw_col)
                    niveaux_liste = [_normalize_niveau(n) for n in parsed if n]
                except Exception:
                    pass
            elif niveau_raw_col:
                niveaux_liste = [_normalize_niveau(niveau_raw_col)]

            ordre_gravite  = {n: i for i, n in enumerate(NIVEAUX_GRAVITE)}
            niveaux_valides = [n for n in niveaux_liste if n in NIVEAUX_GRAVITE]
            if not niveaux_valides:
                rows_skip += 1
                continue
            niveau = max(niveaux_valides, key=lambda n: ordre_gravite.get(n, 0))

            if not date_debut_str:
                rows_skip += 1
                continue

            try:
                date_debut = datetime.strptime(date_debut_str[:10], '%Y-%m-%d')
            except ValueError:
                rows_skip += 1
                continue

            if date_fin_str and date_fin_str not in ('', 'None', 'null', 'NaT', 'undefined'):
                try:
                    date_fin = datetime.strptime(date_fin_str[:10], '%Y-%m-%d')
                except ValueError:
                    date_fin = datetime.now()
            else:
                date_fin = datetime.now()

            year_start = datetime(year, 1, 1)
            year_end   = datetime(year, 12, 31)
            d_start    = max(date_debut, year_start)
            d_end      = min(date_fin, year_end)

            if d_start > d_end:
                rows_skip += 1
                continue

            nb_jours = (d_end - d_start).days + 1

            cur = d_start
            while cur <= d_end:
                mois_key = cur.strftime('%Y-%m')
                par_mois[mois_key][niveau] += 1
                if cur.month == 12:
                    cur = cur.replace(year=cur.year + 1, month=1, day=1)
                else:
                    cur = cur.replace(month=cur.month + 1, day=1)

            if compute_daily:
                jour_cur = d_start
                while jour_cur <= d_end:
                    par_jour[jour_cur.strftime('%Y-%m-%d')][niveau] += 1
                    jour_cur += timedelta(days=1)

            if dept_code:
                if not par_dept[dept_code]['nom']:
                    par_dept[dept_code]['nom'] = DEPT_NOMS.get(dept_code, dept_code)
                par_dept[dept_code]['jours'][niveau] += nb_jours

            rows_ok += 1

        except Exception as e:
            log.debug(f"Ligne CSV ignorée : {e}")
            rows_skip += 1

    log.info(f"  Parse CSV {year}: {rows_ok} lignes traitées, {rows_skip} ignorées, "
             f"{len(par_mois)} mois, {len(par_dept)} depts")

    mois_out = {k: dict(v) for k, v in sorted(par_mois.items())}
    dept_out = {}
    for code, data in par_dept.items():
        graves = (data['jours'].get('alerte', 0)
                  + data['jours'].get('alerte renforcée', 0)
                  + data['jours'].get('crise', 0))
        dept_out[code] = {
            'nom'         : data['nom'],
            'jours'       : dict(data['jours']),
            'total_graves': graves,
        }

    jour_out = {k: dict(v) for k, v in sorted(par_jour.items())} if compute_daily else {}
    return {'par_mois': mois_out, 'par_dept': dept_out, 'par_jour': jour_out}


def _save_and_return_history(history_file: Path, annees_cache: dict) -> dict:
    dept_total = {}
    for _, data in annees_cache.items():
        for code, info in data.get('par_dept', {}).items():
            if code not in dept_total:
                dept_total[code] = {
                    'nom'         : DEPT_NOMS.get(code, info.get('nom', code)),
                    'total_graves': 0,
                    'jours'       : {n: 0 for n in NIVEAUX_GRAVITE},
                }
            dept_total[code]['total_graves'] += info.get('total_graves', 0)
            for niv in NIVEAUX_GRAVITE:
                dept_total[code]['jours'][niv] += info.get('jours', {}).get(niv, 0)

    top10 = sorted(dept_total.items(), key=lambda x: x[1]['total_graves'], reverse=True)[:10]
    top10_out = [
        {'code': c, 'nom': d['nom'], 'total_graves': d['total_graves'], 'jours': d['jours']}
        for c, d in top10
    ]
    result = {
        'updated_at' : datetime.now().isoformat(),
        'annees'     : annees_cache,
        'top10_depts': top10_out,
        'comparaison': {
            str(y): annees_cache.get(str(y), {}).get('par_jour', {})
            for y in [datetime.now().year - 1, datetime.now().year]
        },
    }
    history_file.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    log.info(f"vigieau_history.json : {len(annees_cache)} années, {len(top10_out)} depts top10")
    return result


def fetch_vigieau_history() -> dict:
    """Télécharge et agrège l'historique des arrêtés VigiEau depuis data.gouv.fr."""
    log.info("=== VigiEau Historique ===")

    history_file = SCRIPT_DIR / 'vigieau_history.json'
    current_year = datetime.now().year

    existing = {}
    if history_file.exists():
        try:
            existing = json.loads(history_file.read_text(encoding='utf-8'))
            log.info(f"Cache existant : années {sorted(existing.get('annees', {}).keys())}")
        except Exception as e:
            log.warning(f"Cache VigiEau illisible : {e}")

    annees_cache    = existing.get('annees', {})
    csv_par_annee   = {}
    comprehensive_url = None

    # ── Étape 1 : Lister les ressources via l'API data.gouv.fr ──────────
    try:
        resp = requests.get(
            f"{DATAGOUV_API}{DATAGOUV_DATASET_ID}/",
            timeout=TIMEOUT,
            headers={'User-Agent': 'GSF-Veille/2.0'}
        )
        resp.raise_for_status()
        resources = resp.json().get('resources', [])
        log.info(f"API data.gouv.fr : {len(resources)} ressources")

        for res in resources:
            title    = res.get('title', '') or ''
            fmt      = (res.get('format', '') or '').lower()
            url      = res.get('url', '') or ''
            filetype = res.get('filetype', '') or ''

            log.debug(f"  Ressource: title='{title}' | format='{fmt}' | filetype='{filetype}'")

            is_csv = (
                fmt in ('csv', 'text/csv')
                or url.lower().endswith('.csv')
                or filetype.lower() == 'file'
            )
            if not is_csv and 'csv' not in url.lower():
                continue

            title_lower = title.lower()
            if 'cadre' in title_lower:
                continue
            if 'arret' not in title_lower and 'arrêt' not in title_lower:
                continue

            if title.strip() == 'Arrêtés':
                comprehensive_url = url
                log.info(f"  Fichier compréhensif 'Arrêtés' identifié → {url[-50:]}")
                continue

            found_year = None
            for y in VIGIEAU_HISTORY_YEARS:
                if str(y) in title:
                    found_year = y
                    break
            if found_year is None:
                found_year = current_year

            if found_year not in csv_par_annee:
                csv_par_annee[found_year] = url
                log.info(f"  CSV {found_year} identifié : '{title}' → {url[-50:]}")

    except Exception as e:
        log.warning(f"API data.gouv.fr inaccessible : {e}")

    # ── Étape 2 : Fallback URLs hardcodées ──────────────────────────────
    if not csv_par_annee:
        log.info("Fallback sur URLs hardcodées")
        for year, url in VIGIEAU_FALLBACK_URLS.items():
            csv_par_annee[year] = url

    if not csv_par_annee:
        log.warning("Aucun CSV VigiEau identifié — historique non mis à jour")
        return _save_and_return_history(history_file, annees_cache)

    log.info(f"CSV à traiter : {sorted(csv_par_annee.keys())}")

    # ── Étape 3 : Télécharger et parser ─────────────────────────────────
    for year in sorted(csv_par_annee.keys()):
        url      = csv_par_annee[year]
        year_str = str(year)

        if year_str in annees_cache and year != current_year:
            cached = annees_cache[year_str]
            if cached.get('par_mois') and cached.get('par_dept'):
                log.info(f"Année {year} : cache OK ({len(cached['par_mois'])} mois)")
                continue

        try:
            log.info(f"Téléchargement CSV {year} : {url[-60:]}")
            r = requests.get(url, timeout=120, headers={'User-Agent': 'GSF-Veille/2.0'},
                             allow_redirects=True)
            r.raise_for_status()
            log.info(f"  → {len(r.content)} octets")

            try:
                content = r.content.decode('utf-8')
            except UnicodeDecodeError:
                content = r.content.decode('latin-1')

            first_line = content.split('\n')[0].lower()
            if 'date_debut' not in first_line and 'date' not in first_line:
                log.warning(f"CSV {year} : colonnes inattendues ({first_line[:80]})")
                continue

            stats = _parse_vigieau_csv(content, year, compute_daily=(year >= 2024))
            if stats['par_mois'] or stats['par_dept']:
                annees_cache[year_str] = stats
            else:
                log.warning(f"CSV {year} : aucune donnée parsée")

        except Exception as e:
            log.warning(f"CSV {year} erreur : {e}")

    # ── Étape 3b : Fichier compréhensif pour années récentes manquantes ─
    if comprehensive_url:
        missing_years = [
            y for y in range(current_year - 1, current_year + 1)
            if str(y) not in annees_cache
            or not annees_cache[str(y)].get('par_mois')
        ]
        if missing_years:
            log.info(f"Années manquantes {missing_years} → téléchargement fichier compréhensif")
            try:
                r = requests.get(comprehensive_url, timeout=180,
                                 headers={'User-Agent': 'GSF-Veille/2.0'}, allow_redirects=True)
                r.raise_for_status()
                log.info(f"  → fichier compréhensif : {len(r.content)} octets")
                try:
                    comp_content = r.content.decode('utf-8')
                except UnicodeDecodeError:
                    comp_content = r.content.decode('latin-1')

                first_line = comp_content.split('\n')[0].lower()
                if 'date_debut' not in first_line and 'date' not in first_line:
                    log.warning(f"Fichier compréhensif : colonnes inattendues ({first_line[:80]})")
                else:
                    for y in missing_years:
                        log.info(f"  Parsing année {y} depuis fichier compréhensif")
                        stats = _parse_vigieau_csv(comp_content, y, compute_daily=True)
                        if stats['par_mois'] or stats['par_dept']:
                            annees_cache[str(y)] = stats
                            log.info(f"  Année {y} ajoutée : {len(stats['par_mois'])} mois, "
                                     f"{len(stats.get('par_jour', {}))} jours")
                        else:
                            log.warning(f"  Année {y} : aucune donnée parsée depuis le fichier compréhensif")
            except Exception as e:
                log.warning(f"Fichier compréhensif erreur : {e}")

    return _save_and_return_history(history_file, annees_cache)


# ─────────────────────────────────────────────
# ÉCRITURE JSON
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
    restrictions  = []
    jorf_autres   = []
    jorf_items    = []
    briefing_jorf = []

    # 1. JORF
    try:
        jorf_items, jorf_autres, jorf_total, briefing_jorf = fetch_jorf()
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

    # 4. VigiEau historique (cache intelligent)
    try:
        fetch_vigieau_history()
    except Exception as e:
        log.error(f"VigiEau history fatal : {e}")
        errors.append('VigiEau_history')

    # Déduplication + tri criticité décroissante
    seen   = set()
    unique = []
    for item in sorted(items, key=lambda x: (x.get('date', '2000-01-01'),
                                              x.get('criticite', 1)), reverse=True):
        if item['id'] not in seen:
            seen.add(item['id'])
            unique.append(item)

    for i, item in enumerate(unique):
        item['top5'] = i < 5

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
        'jo_retenus'      : jorf_items,
        'briefing_jorf'   : briefing_jorf,
    }

    log.info(f"JSON : {len(unique)} items RSS, {len(jorf_items)} items JORF, "
             f"{len(restrictions)} depts eau")
    write_output(json_data, TODAY)

    duration = (datetime.now() - start).total_seconds()
    status   = 'OK' if not errors else 'PARTIEL'
    log.info(f"Pipeline terminé en {duration:.1f}s — {status}")
    return 0 if not errors else 1


if __name__ == '__main__':
    sys.exit(main())
