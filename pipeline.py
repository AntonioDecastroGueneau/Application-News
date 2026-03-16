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

JORF_KEYWORDS_STRICT = [
    # Nettoyage / propreté (opérationnel)
    'nettoyage industriel', 'nettoyage tertiaire', 'nettoyage des locaux',
    'propreté industrielle', 'agent de propreté', 'entreprise de propreté',
    'branche propreté', 'convention collective', 'propreté et services',
    'désinfection', 'décontamination', 'hygiène des locaux',
    # Déchets
    'déchet dangereux', 'déchets dangereux', 'déchet industriel', 'déchets industriels',
    'DASRI', 'déchet infectieux', 'collecte de déchets', 'traitement de déchets',
    'responsabilité élargie du producteur', 'filière REP',
    # Produits chimiques
    'biocide', 'détergent', 'substance dangereuse', 'produit chimique',
    'CMR', 'composé organique volatil', 'COV', 'solvant',
    'REACH', 'CLP', 'fiche de données de sécurité',
    # ICPE / risques industriels
    'installation classée', 'ICPE', 'SEVESO', 'rubrique ICPE',
    'radioprotection', 'zone contrôlée', 'amiante', 'plomb',
    # Climat & énergie (législatif)
    'stratégie nationale bas-carbone', 'SNBC', 'plan national adaptation',
    'PNACC', 'transition énergétique', 'neutralité carbone', 'net zéro',
    'loi climat', 'loi énergie', 'décarbonation', 'gaz à effet de serre',
    'reporting durabilité', 'CSRD', 'taxonomie', 'plan climat',
    'objectif climatique', 'trajectoire carbone', 'bilan carbone',
]

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
        # Tout le contenu HCC est pertinent — pas de filtre
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
                             'événement extrême', 'catastrophe climatique', 'réchauffement']):
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
    """Filtre binaire : l'article vaut-il la peine d'être lu par le resp. Climat/Env GSF ?"""
    system = 'Tu es un filtre. Reponds uniquement {"ok": true} ou {"ok": false}.'
    prompt = (
        'Responsable Climat et Environnement chez GSF (groupe de services, 42000 salaries).\n\n'
        'GARDER si le sujet concerne :\n'
        '- Politique climatique : lois, projets de loi, plans gouvernementaux (SNBC, PNACC, loi climat, loi energie)\n'
        '- Trajectoires decarbonation : objectifs nationaux/europeens, net zero, neutralite carbone\n'
        '- Rapports et etudes : GIEC/IPCC, Haut Conseil Climat, France Strategie, ADEME, I4CE, Shift Project\n'
        '- Reglementation ESG/RSE : CSRD, taxonomie verte, bilan carbone, reporting extra-financier\n'
        '- Evenements climatiques : canicule, inondation, secheresse, feu de foret, submersion\n'
        '- Adaptation climatique : risques entreprises, resilience, vulnerabilite sectorielle\n'
        '- Reglementation environnementale : ICPE, REACH, dechets, air, eau, biodiversite\n'
        '- Transition energetique : renouvelables, efficacite energetique, hydrogene, scope 1/2/3\n\n'
        'REJETER si le sujet est CLAIREMENT : elections/partis politiques, guerre/geopolitique sans lien climatique, '
        'immobilier residentiel, agriculture/elevage, finance de marche, faits divers, sport, culture, sante humaine sans lien environnemental.\n'
        'EN CAS DE DOUTE : garder (ok: true).\n\n'
        f'TITRE: {titre}\n'
        f'DEBUT: {contenu[:200]}\n\n'
        'Reponds: {"ok": true} ou {"ok": false}'
    )
    raw = call_groq(prompt, system)
    result = extract_json(raw)
    # Par défaut garder si le LLM ne répond pas clairement
    return result.get('ok', True)



def _gsf_resumer(titre: str, contenu: str) -> dict:
    """Resume et donne un score de priorite a un article pertinent pour GSF."""
    system = 'Analyste climat et environnement GSF. JSON valide uniquement.'
    prompt = (
        'Tu travailles pour le Responsable Climat et Environnement de GSF (groupe de services, 42000 salaries, '
        'secteurs industrie/tertiaire/sante/nucleaire). Ta mission : suivre la politique climatique, '
        'la reglementation environnementale, les trajectoires de decarbonation et les risques climatiques.\n\n'
        'Score:\n'
        '  1 = information de veille, tendance a connaitre\n'
        '  2 = evolution reglementaire ou politique a anticiper pour GSF\n'
        '  3 = obligation immediate, risque direct ou decision strategique urgente pour GSF\n\n'
        f'TITRE: {titre}\n'
        f'CONTENU: {contenu[:600]}\n\n'
        'JSON: {"resume": "1-2 phrases concises sur ce que ca change pour GSF", "score": 1}'
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
        "Tu es un analyste réglementaire et climat pour GSF (42 000 salariés, services). "
        "Réponds UNIQUEMENT en JSON valide, sans texte avant ou après."
    )
    prompt = (
        "GSF est un groupe de services (nettoyage industriel, tertiaire, nucléaire, santé). "
        "Le Responsable Climat et Environnement suit la réglementation opérationnelle ET la politique climatique.\n\n"
        "EXCLUSIONS ABSOLUES (pertinent=false) :\n"
        "- Professions réglementées (médecins, avocats, notaires...)\n"
        "- Nominations, mutations, concours fonction publique\n"
        "- Finances publiques, défense, justice, agriculture pure\n\n"
        "PERTINENT si le texte concerne :\n"
        "- Réglementation climatique : loi climat/énergie, SNBC, PNACC, objectifs GES\n"
        "- CSRD, reporting durabilité, taxonomie verte, bilan carbone obligatoire\n"
        "- ICPE, REACH, biocides, déchets industriels, CMR\n"
        "- Transition énergétique, efficacité énergétique bâtiments\n"
        "- Biodiversité réglementaire (Natura 2000, espèces protégées)\n"
        "- Santé-sécurité agents nettoyage, conventions collectives propreté\n\n"
        "EN CAS DE DOUTE : pertinent=false.\n\n"
        f"TITRE : {titre}\n"
        f"EXTRAIT : {contenu[:600]}\n\n"
        '{"pertinent": true, "score": 2, "resume": "2 phrases max si pertinent, sinon vide"}\n'
        "score : 1=veille, 2=évolution à anticiper, 3=obligation directe immédiate"
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
# VIGIEAU HISTORIQUE — data.gouv.fr
# ─────────────────────────────────────────────

DATAGOUV_DATASET_ID   = 'donnee-secheresse-vigieau'
DATAGOUV_API          = 'https://www.data.gouv.fr/api/1/datasets/'
VIGIEAU_HISTORY_YEARS = list(range(2020, datetime.now().year + 1))

# Niveaux exacts dans les CSV VigiEau (sensibles à la casse après .lower())
NIVEAUX_GRAVITE = ['vigilance', 'alerte', 'alerte renforcée', 'crise']

# URLs stables de secours si l'API data.gouv.fr ne répond pas
# Format : https://www.data.gouv.fr/api/1/datasets/r/<resource_id>
# Ces IDs proviennent de la page du dataset VigiEau sur data.gouv.fr
VIGIEAU_FALLBACK_URLS = {
    2026: 'https://www.data.gouv.fr/api/1/datasets/r/0732e970-c12c-4e6a-adca-5ac9dbc3fdfa',
}


def _normalize_niveau(niveau: str) -> str:
    """Normalise un niveau de gravité VigiEau pour correspondre à NIVEAUX_GRAVITE."""
    n = niveau.lower().strip()
    n = n.replace('_', ' ')
    n = n.replace('renforcee', 'renforcée')
    n = n.replace('renforcé', 'renforcée')
    return n


def _parse_vigieau_csv(content: str, year: int) -> dict:
    """
    Parse un CSV arrêtés VigiEau (format data.gouv.fr).
    Colonnes : id, numero, date_debut, date_fin, statut, departement,
               zones_alerte.niveau_gravite (JSON array), ...
    Retourne {'par_mois': {...}, 'par_dept': {...}}
    """
    import csv as _csv
    import io
    import json as _json
    from collections import defaultdict

    par_mois = defaultdict(lambda: {n: 0 for n in NIVEAUX_GRAVITE})
    par_dept = defaultdict(lambda: {'nom': '', 'jours': {n: 0 for n in NIVEAUX_GRAVITE}})

    reader = _csv.DictReader(io.StringIO(content), delimiter=',')
    headers = reader.fieldnames or []
    log.debug(f"CSV colonnes ({year}): {headers[:15]}")

    rows_ok = 0
    rows_skip = 0

    for row in reader:
        try:
            date_debut_str = row.get('date_debut', '').strip()
            date_fin_str   = row.get('date_fin', '').strip()

            # Département — colonne simple (code numérique ex: "76")
            dept_code_raw = (
                row.get('departement', '')
                or row.get('departement_pilote', '')
                or ''
            ).strip().strip('"')
            dept_code = dept_code_raw.zfill(2) if dept_code_raw else ''

            # Niveau de gravité — zones_alerte.niveau_gravite est un tableau JSON
            # ex: ["vigilance","vigilance"] ou "vigilance"
            niveau_raw_col = (
                row.get('zones_alerte.niveau_gravite', '')
                or row.get('niveau_gravite', '')
                or row.get('niveauGravite', '')
                or row.get('niveauAlerte', '')
                or ''
            ).strip()

            # Parser le tableau JSON ou prendre la valeur directe
            niveaux_liste = []
            if niveau_raw_col.startswith('['):
                try:
                    parsed = _json.loads(niveau_raw_col)
                    niveaux_liste = [_normalize_niveau(n) for n in parsed if n]
                except Exception:
                    pass
            elif niveau_raw_col:
                niveaux_liste = [_normalize_niveau(niveau_raw_col)]

            # Prendre le niveau le plus grave
            ordre_gravite = {n: i for i, n in enumerate(NIVEAUX_GRAVITE)}
            niveaux_valides = [n for n in niveaux_liste if n in NIVEAUX_GRAVITE]
            if not niveaux_valides:
                rows_skip += 1
                continue
            niveau = max(niveaux_valides, key=lambda n: ordre_gravite.get(n, 0))

            if not date_debut_str:
                rows_skip += 1
                continue

            # Parse dates
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

            # Limiter à l'année concernée
            year_start = datetime(year, 1, 1)
            year_end   = datetime(year, 12, 31)
            d_start = max(date_debut, year_start)
            d_end   = min(date_fin, year_end)

            if d_start > d_end:
                rows_skip += 1
                continue

            nb_jours = (d_end - d_start).days + 1

            # Agréger par mois
            cur = d_start
            while cur <= d_end:
                mois_key = cur.strftime('%Y-%m')
                par_mois[mois_key][niveau] += 1
                if cur.month == 12:
                    cur = cur.replace(year=cur.year + 1, month=1, day=1)
                else:
                    cur = cur.replace(month=cur.month + 1, day=1)

            # Agréger par département
            if dept_code:
                if not par_dept[dept_code]['nom']:
                    par_dept[dept_code]['nom'] = dept_code
                par_dept[dept_code]['jours'][niveau] += nb_jours

            rows_ok += 1

        except Exception as e:
            log.debug(f"Ligne CSV ignorée : {e}")
            rows_skip += 1
            continue

    log.info(f"  Parse CSV {year}: {rows_ok} lignes traitées, {rows_skip} ignorées, "
             f"{len(par_mois)} mois, {len(par_dept)} depts")

    # Sérialiser en dict plain (pas defaultdict)
    mois_out = {k: dict(v) for k, v in sorted(par_mois.items())}
    dept_out = {}
    for code, data in par_dept.items():
        graves = (data['jours'].get('alerte', 0)
                  + data['jours'].get('alerte renforcée', 0)
                  + data['jours'].get('crise', 0))
        dept_out[code] = {
            'nom'          : data['nom'],
            'jours'        : dict(data['jours']),
            'total_graves' : graves,
        }

    return {'par_mois': mois_out, 'par_dept': dept_out}


def fetch_vigieau_history() -> dict:
    """
    Télécharge et agrège l'historique des arrêtés VigiEau depuis data.gouv.fr.
    Stratégie :
    1. Appelle l'API data.gouv.fr pour lister les ressources
    2. Identifie les CSV annuels (matching souple sur le titre)
    3. Cache intelligent : ne re-télécharge que l'année en cours
    4. Fallback sur URLs hardcodées si l'API ne répond pas
    """
    log.info("=== VigiEau Historique ===")

    history_file = SCRIPT_DIR / 'vigieau_history.json'
    current_year = datetime.now().year

    # Charger le cache existant
    existing = {}
    if history_file.exists():
        try:
            existing = json.loads(history_file.read_text(encoding='utf-8'))
            log.info(f"Cache existant : années {sorted(existing.get('annees', {}).keys())}")
        except Exception as e:
            log.warning(f"Cache VigiEau illisible : {e}")

    annees_cache = existing.get('annees', {})

    # ── Étape 1 : Lister les ressources via l'API data.gouv.fr ──────────────
    csv_par_annee = {}
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
            title   = res.get('title', '') or ''
            fmt     = (res.get('format', '') or '').lower()
            url     = res.get('url', '') or ''
            filetype = res.get('filetype', '') or ''

            # Logger pour débug
            log.debug(f"  Ressource: title='{title}' | format='{fmt}' | filetype='{filetype}'")

            # Filtre : doit être un CSV (format ou url)
            is_csv = (
                fmt in ('csv', 'text/csv')
                or url.lower().endswith('.csv')
                or filetype.lower() == 'file'
            )
            if not is_csv and 'csv' not in url.lower():
                continue

            title_lower = title.lower()

            # Matching souple sur le titre — on cherche "arrêt" + éventuellement une année
            # Exclure "arrêtés cadre" (c'est la réglementation, pas les arrêtés de restriction)
            if 'cadre' in title_lower:
                continue
            if 'arret' not in title_lower and 'arrêt' not in title_lower:
                continue

            # Identifier l'année
            found_year = None
            for y in VIGIEAU_HISTORY_YEARS:
                if str(y) in title:
                    found_year = y
                    break

            if found_year is None:
                # Pas d'année dans le titre → fichier de l'année en cours
                found_year = current_year

            if found_year not in csv_par_annee:
                csv_par_annee[found_year] = url
                log.info(f"  CSV {found_year} identifié : '{title}' → {url[-50:]}")

    except Exception as e:
        log.warning(f"API data.gouv.fr inaccessible : {e}")

    # ── Étape 2 : Fallback sur URLs hardcodées si rien trouvé ────────────────
    if not csv_par_annee:
        log.info("Fallback sur URLs hardcodées")
        for year, url in VIGIEAU_FALLBACK_URLS.items():
            csv_par_annee[year] = url

    if not csv_par_annee:
        log.warning("Aucun CSV VigiEau identifié — historique non mis à jour")
        return _save_and_return_history(history_file, annees_cache)

    log.info(f"CSV à traiter : {sorted(csv_par_annee.keys())}")

    # ── Étape 3 : Télécharger et parser ──────────────────────────────────────
    for year in sorted(csv_par_annee.keys()):
        url      = csv_par_annee[year]
        year_str = str(year)

        # Skip les années passées déjà en cache (sauf si cache vide ou corrompu)
        if year_str in annees_cache and year != current_year:
            cached = annees_cache[year_str]
            if cached.get('par_mois') and cached.get('par_dept'):
                log.info(f"Année {year} : cache OK ({len(cached['par_mois'])} mois)")
                continue

        try:
            log.info(f"Téléchargement CSV {year} : {url[-60:]}")
            r = requests.get(
                url, timeout=120,
                headers={'User-Agent': 'GSF-Veille/2.0'},
                allow_redirects=True
            )
            r.raise_for_status()
            log.info(f"  → {len(r.content)} octets")

            # Décoder (UTF-8 ou latin-1)
            try:
                content = r.content.decode('utf-8')
            except UnicodeDecodeError:
                content = r.content.decode('latin-1')

            # Vérifier que c'est bien un CSV avec les bonnes colonnes
            first_line = content.split('\n')[0].lower()
            if 'date_debut' not in first_line and 'date' not in first_line:
                log.warning(f"CSV {year} : colonnes inattendues ({first_line[:80]})")
                continue

            stats = _parse_vigieau_csv(content, year)

            if stats['par_mois'] or stats['par_dept']:
                annees_cache[year_str] = stats
            else:
                log.warning(f"CSV {year} : aucune donnée parsée")

        except Exception as e:
            log.warning(f"CSV {year} erreur : {e}")

    return _save_and_return_history(history_file, annees_cache)


def _save_and_return_history(history_file, annees_cache: dict) -> dict:
    """Calcule le top10, sauvegarde et retourne le résultat."""
    # Top 10 départements toutes années confondues
    dept_total = {}
    for year_str, data in annees_cache.items():
        for code, info in data.get('par_dept', {}).items():
            if code not in dept_total:
                dept_total[code] = {
                    'nom': info.get('nom', code),
                    'total_graves': 0,
                    'jours': {n: 0 for n in NIVEAUX_GRAVITE}
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
    }

    history_file.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    log.info(f"vigieau_history.json : {len(annees_cache)} années, "
             f"{len(top10_out)} depts top10")

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
