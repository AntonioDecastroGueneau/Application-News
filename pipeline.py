#!/usr/bin/env python3
"""
Veille Environnementale GSF — Pipeline quotidien
Exécuté chaque matin à 07:20 UTC via GitHub Actions

Dépendances :
    pip install requests feedparser groq mistralai beautifulsoup4
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
from mistralai.client import Mistral


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

TODAY      = datetime.now().strftime('%Y-%m-%d')
SCRIPT_DIR = Path(__file__).parent.resolve()
LOG_PATH   = SCRIPT_DIR / 'pipeline.log'

JORF_BASE_URL     = 'https://echanges.dila.gouv.fr/OPENDATA/JORF/'
VIGIEAU_DEPTS_URL = 'https://api.vigieau.gouv.fr/api/departements'

# ── LLM — providers et modèles ───────────────────────────────────────────────
# Mistral  : principal  (1B tokens/mois, 1 req/s, free tier Experiment)
# Groq     : fallback   (si Mistral 429 ou indisponible)
#
# Modèle unique Mistral : mistral-small-latest
#   → filtre, résumé, enrichissement — tout en un seul provider
# Groq fallback mixte :
#   8b  (llama-3.1-8b-instant)    → filtrage/résumé
#   70b (llama-3.3-70b-versatile) → enrichissement score ≥ 2

MISTRAL_MODEL      = 'mistral-small-latest'
MISTRAL_MIN_INTERVAL = 1.2          # 1 req/s max → on prend 1.2s avec marge

GROQ_MODEL_FILTER  = 'llama-3.1-8b-instant'
GROQ_MODEL_ENRICH  = 'llama-3.3-70b-versatile'
GROQ_MODEL         = GROQ_MODEL_FILTER            # alias compat
GROQ_MAX_RETRY     = 3
GROQ_RETRY_WAIT    = 62
GROQ_MIN_INTERVAL  = 10.0   # secondes entre appels Groq (calibré TPM 6k)

# ─────────────────────────────────────────────
# DESCRIPTION GSF — injectée dans tous les prompts LLM
# ─────────────────────────────────────────────

GSF_CONTEXT = """
GSF est un prestataire multiservices (propreté, maintenance, FM) qui intervient sur les
sites de ses clients — il n'est PAS exploitant ICPE.

Enjeux environnement / climat / carbone :
- Flotte de véhicules (scope 1 significatif) : électrification en cours
- Consommation énergétique locaux et prestations (eau, électricité, chauffage)
- Déchets produits lors des prestations (tri, traçabilité, éco-organismes)
- Obligation BGES et plan climat / trajectoire de réduction des émissions
- CSRD / reporting extra-financier (grande entreprise de services)
- Certifications ISO 14001, démarches RSE et bas-carbone

Risques indirects et chaîne de valeur :
- 3 000+ sites clients exposés aux aléas climatiques (inondations, sécheresse, canicules)
  → impacts sur volumes de prestations, contrats, résilience opérationnelle
- Exigences carbone croissantes des clients (nucléaire, santé, agroalimentaire, industrie)
  sur empreinte, réduction déchets, éco-efficacité des prestations
""".strip()

# Version courte injectée dans les prompts LLM (~60 tokens)
GSF_CONTEXT_SHORT = (
    "GSF : prestataire propreté/FM, intervient chez ses clients (non exploitant ICPE). "
    "Enjeux : BGES/plan climat, flotte véhicules (scope 1), énergie, déchets prestations, "
    "CSRD/reporting, ISO 14001. "
    "Risques indirects : aléas climatiques sur 3 000+ sites clients, "
    "exigences carbone des clients (nucléaire, santé, industrie)."
)


# ─────────────────────────────────────────────
# KEYWORDS — premier filtre large (présence dans titre/contenu)
# Resserré sur les enjeux réels de GSF : climat, carbone, énergie,
# déchets de services, reporting ESG, risques climatiques.
# ─────────────────────────────────────────────

KEYWORDS = [
    # Climat & décarbonation
    'climat', 'climatique', 'réchauffement', 'décarbonation', 'carbone',
    'GES', 'gaz à effet de serre', 'neutralité carbone', 'net zéro',
    'SNBC', 'PNACC', 'adaptation', 'atténuation', 'trajectoire',
    'transition énergétique', 'transition écologique',
    'Accord de Paris', 'COP', 'GIEC', 'IPCC',
    'décarboner', 'bas-carbone', 'scope 1', 'scope 2', 'scope 3',
    'bilan carbone', 'empreinte carbone', 'plan climat',
    'loi énergie', 'loi climat', 'France 2030',
    # Reporting & obligations ESG
    'CSRD', 'reporting extra-financier', 'taxonomie verte',
    'reporting environnemental', 'reporting durabilité',
    'bilan GES', 'BGES',
    # Énergie
    'énergie', 'efficacité énergétique', 'renouvelable', 'DPE',
    'bâtiment tertiaire', 'rénovation énergétique',
    # Déchets (angle prestataire de services)
    'déchet', 'déchets', 'REP', 'responsabilité élargie',
    'tri', 'recyclage', 'éco-organisme', 'traçabilité déchets',
    # Risques climatiques physiques
    'inondation', 'sécheresse', 'canicule', 'événement extrême',
    'submersion', 'feu de forêt', 'catastrophe naturelle', 'PPR',
    # Mobilité & flotte
    'véhicule électrique', 'électrification', 'flotte',
    'mobilité durable', 'ZFE',
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
# LLM — clients et appel unifié
#
# call_llm() : point d'entrée unique pour tout le pipeline
#   1. Mistral  (principal, 1 req/s)
#   2. Groq 8b  (fallback si Mistral 429 ou erreur)
# ─────────────────────────────────────────────

_mistral_client = None
_groq_client    = None
_mistral_last_call: float = 0.0
_groq_last_call:    float = 0.0



def _get_mistral_client() -> Mistral:
    global _mistral_client
    if _mistral_client is None:
        api_key = os.environ.get('MISTRAL_API_KEY', '')
        if not api_key:
            raise RuntimeError("MISTRAL_API_KEY manquant")
        _mistral_client = Mistral(api_key=api_key)
    return _mistral_client


def _get_groq_client() -> Groq:
    global _groq_client
    if _groq_client is None:
        api_key = os.environ.get('GROQ_API_KEY', '')
        if not api_key:
            raise RuntimeError("GROQ_API_KEY manquant")
        _groq_client = Groq(api_key=api_key)
    return _groq_client


def _call_mistral(prompt: str, system: str, max_tokens: int) -> str:
    """Appelle Mistral avec rate limiter 1 req/s."""
    global _mistral_last_call
    elapsed = time.time() - _mistral_last_call
    if elapsed < MISTRAL_MIN_INTERVAL:
        time.sleep(MISTRAL_MIN_INTERVAL - elapsed)
    _mistral_last_call = time.time()

    client = _get_mistral_client()
    resp = client.chat.complete(
        model=MISTRAL_MODEL,
        messages=[
            {'role': 'system', 'content': system},
            {'role': 'user',   'content': prompt},
        ],
        max_tokens=max_tokens,
        temperature=0.2,
        response_format={"type": "json_object"},
    )
    content = resp.choices[0].message.content
    # mistralai v1+ peut retourner un dict déjà parsé avec json_object
    if isinstance(content, dict):
        return json.dumps(content, ensure_ascii=False)
    return str(content).strip()


def _call_groq_fallback(prompt: str, system: str, max_tokens: int,
                        model: str) -> str:
    """Fallback Groq avec rate limiter 10s et retry × 3."""
    global _groq_last_call
    elapsed = time.time() - _groq_last_call
    if elapsed < GROQ_MIN_INTERVAL:
        time.sleep(GROQ_MIN_INTERVAL - elapsed)

    client = _get_groq_client()
    sys_content = system if 'json' in system.lower() else (system + ' Reponds en JSON.').strip()
    messages = [
        {'role': 'system', 'content': sys_content},
        {'role': 'user',   'content': prompt},
    ]
    for attempt in range(1, GROQ_MAX_RETRY + 1):
        _groq_last_call = time.time()
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=0.2,
                response_format={"type": "json_object"},
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            err_str = str(e)
            if '429' in err_str or 'rate_limit' in err_str.lower():
                log.warning(f"Groq fallback rate limit [{model}] "
                            f"(tentative {attempt}/{GROQ_MAX_RETRY}) — attente {GROQ_RETRY_WAIT}s")
                time.sleep(GROQ_RETRY_WAIT)
                _groq_last_call = time.time()
            else:
                log.warning(f"Groq fallback erreur [{model}] : {e}")
                return ''
    log.error(f"Groq fallback [{model}] : toutes les tentatives épuisées")
    return ''


def call_llm(prompt: str, system: str = '', max_tokens: int = 300,
             enrich: bool = False) -> str:
    """Point d'entrée unique pour tous les appels LLM du pipeline.

    Tente Mistral en premier. Si 429 ou erreur → fallback Groq automatique.
    enrich=True : utilise Groq 70b pour l'enrichissement (si Mistral échoue).
    """
    # ── Tentative Mistral (principal) ───────────────────────────────────
    try:
        result = _call_mistral(prompt, system, max_tokens)
        if result:
            return result
    except Exception as e:
        err_str = str(e)
        if '429' in err_str or 'rate_limit' in err_str.lower() or 'quota' in err_str.lower():
            log.warning(f"Mistral rate limit → fallback Groq")
        else:
            log.warning(f"Mistral erreur → fallback Groq : {e}")

    # ── Fallback Groq ────────────────────────────────────────────────────
    groq_model = GROQ_MODEL_ENRICH if enrich else GROQ_MODEL_FILTER
    return _call_groq_fallback(prompt, system, max_tokens, groq_model)


# Alias de compatibilité — call_groq redirige vers call_llm
def call_groq(prompt: str, system: str = '', max_tokens: int = 300,
              model: str = None) -> str:
    enrich = (model == GROQ_MODEL_ENRICH)
    return call_llm(prompt, system, max_tokens, enrich=enrich)


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
    Évalue un texte du JORF.
    Passe 1 (8b)  : filtrage + résumé + score
    Passe 2 (70b) : enrichissement pourquoi uniquement si score >= 2
    """
    system = 'Tu es expert climat et RSE conseillant le Responsable Environnement de GSF. JSON uniquement.'
    prompt = (
        f"{GSF_CONTEXT_SHORT}\n\n"
        "Ce texte du Journal Officiel a passé un premier filtre par mots-clés.\n"
        "Détermine s'il est VRAIMENT pertinent pour le Responsable Environnement / Climat de GSF.\n\n"
        "PERTINENT si le texte crée ou modifie une obligation s'appliquant DIRECTEMENT à GSF :\n"
        "  - Obligations BGES, bilan GES, plan climat pour grandes entreprises de services\n"
        "  - CSRD / reporting extra-financier / taxonomie verte\n"
        "  - Réglementation déchets : nouvelles filières REP, tri, traçabilité\n"
        "  - Réglementation énergie : efficacité énergétique, DPE tertiaire, renouvelables\n"
        "  - Mobilité et flotte : ZFE, véhicules électriques, réglementation transport\n"
        "  - Arrêtés CatNat, PPR, risques naturels (risques sur sites clients GSF)\n"
        "  - Loi ou décret structurant la politique climatique (SNBC, PNACC, loi énergie-climat)\n\n"
        "REJETER IMPÉRATIVEMENT :\n"
        "  - Réglementation ICPE, SEVESO, biocides, REACH, amiante (GSF n'est pas exploitant)\n"
        "  - Nominations, délégations de signature, textes purement administratifs\n"
        "  - Conventions collectives hors branche propreté\n"
        "  - Réglementation sectorielle sans rapport : agriculture, pêche, défense, santé humaine\n\n"
        "RÈGLE : si le lien avec GSF est indirect ou nécessite plusieurs déductions → NON PERTINENT.\n\n"
        "Score (si pertinent) :\n"
        "  1 = information de veille\n"
        "  2 = évolution réglementaire à anticiper\n"
        "  3 = obligation immédiate ou risque direct\n\n"
        f"TITRE: {titre}\n"
        f"CONTENU: {contenu[:500]}\n\n"
        'JSON: {"pertinent": true/false, "resume": "ce que ça change pour GSF en 1 phrase (vide si non pertinent)", '
        '"pourquoi": "", "score": 1}'
    )
    # Passe 1 — 8b : filtrage + résumé + score
    raw    = call_groq(prompt, system, max_tokens=300, model=GROQ_MODEL_FILTER)
    result = extract_json(raw)
    if not result:
        return {'pertinent': False, 'resume': '', 'pourquoi': '', 'score': 1}

    score = int(result.get('score') or 1)
    if not result.get('pertinent') or score < 2:
        result['pourquoi'] = ''
        return result

    # Passe 2 — 70b : enrichissement pourquoi sur signaux forts uniquement
    enrich_prompt = (
        f"{GSF_CONTEXT}\n\n"
        f"Texte JO retenu (score {score}/3) : {titre}\n"
        f"Résumé : {result.get('resume','')}\n\n"
        "En 1-2 phrases précises, explique POURQUOI c'est un signal stratégique important pour GSF : "
        "quelle obligation concrète, quel délai, quel risque ou quelle opportunité.\n"
        'JSON: {"pourquoi": "..."}'
    )
    raw2 = call_groq(enrich_prompt, 'Expert RSE GSF. JSON uniquement.',
                     max_tokens=150, model=GROQ_MODEL_ENRICH)
    enrich = extract_json(raw2)
    if enrich.get('pourquoi'):
        result['pourquoi'] = enrich['pourquoi']
    log.debug(f"JORF enrichi 70b (score={score}) : {titre[:50]}")
    return result


# ─────────────────────────────────────────────
# LLM — ANALYSE RSS / PRESSE
# Un seul appel Groq par article (filtre + résumé fusionnés).
# ─────────────────────────────────────────────

def groq_analyse_rss(titre: str, contenu: str) -> dict:
    """
    Évalue un article RSS pour GSF.
    Passe 1 (8b)  : filtrage + résumé + score
    Passe 2 (70b) : enrichissement pourquoi si score >= 2
    """
    system = 'Tu es analyste climat et RSE pour GSF. JSON valide uniquement.'
    prompt = (
        f"{GSF_CONTEXT_SHORT}\n\n"
        "Évalue cet article : est-il pertinent pour le Responsable Environnement / Climat de GSF ?\n\n"
        "PERTINENT si le sujet concerne directement GSF :\n"
        "- Politique climatique : SNBC, PNACC, loi énergie-climat, trajectoires nationales\n"
        "- Décarbonation entreprises : objectifs net zéro, scope 1/2/3, bilan GES, BGES\n"
        "- Reporting ESG : CSRD, taxonomie verte, reporting extra-financier\n"
        "- Réglementation déchets : REP, tri, traçabilité, éco-organismes\n"
        "- Énergie & bâtiment : efficacité énergétique, DPE tertiaire, renouvelables\n"
        "- Mobilité durable : ZFE, électrification flottes, véhicules propres\n"
        "- Risques climatiques : canicule, inondation, sécheresse, événements extrêmes\n"
        "- Rapports de référence : GIEC, Haut Conseil Climat, ADEME, I4CE, Shift Project\n\n"
        "NON PERTINENT : ICPE/SEVESO/REACH (GSF non exploitant), élections, géopolitique,\n"
        "agriculture, immobilier résidentiel, finance de marché, faits divers, sport.\n"
        "EN CAS DE DOUTE : pertinent = true.\n\n"
        "Score :\n"
        "  1 = veille, tendance à connaître\n"
        "  2 = évolution à anticiper pour GSF\n"
        "  3 = obligation immédiate ou risque direct\n\n"
        f"TITRE: {titre}\n"
        f"CONTENU: {contenu[:600]}\n\n"
        'JSON: {"pertinent": true/false, '
        '"resume": "1-2 phrases sur ce que ça change pour GSF (vide si non pertinent)", '
        '"pourquoi": "", "score": 1}'
    )
    # Passe 1 — 8b : filtrage + résumé + score
    raw    = call_groq(prompt, system, max_tokens=300, model=GROQ_MODEL_FILTER)
    result = extract_json(raw)
    if not result:
        return {'pertinent': False, 'resume': '', 'pourquoi': '', 'score': 1}

    score = int(result.get('score') or 1)
    if not result.get('pertinent') or score < 2:
        result['pourquoi'] = ''
        return result

    # Passe 2 — 70b : enrichissement pourquoi sur signaux forts uniquement
    enrich_prompt = (
        f"{GSF_CONTEXT}\n\n"
        f"Article retenu (score {score}/3) : {titre}\n"
        f"Résumé : {result.get('resume','')}\n\n"
        "En 1-2 phrases précises, explique POURQUOI c'est un signal stratégique important pour GSF : "
        "quelle implication concrète, quel délai d'action, quel risque ou quelle opportunité.\n"
        'JSON: {"pourquoi": "..."}'
    )
    raw2 = call_groq(enrich_prompt, 'Expert RSE GSF. JSON uniquement.',
                     max_tokens=150, model=GROQ_MODEL_ENRICH)
    enrich = extract_json(raw2)
    if enrich.get('pourquoi'):
        result['pourquoi'] = enrich['pourquoi']
    log.debug(f"RSS enrichi 70b (score={score}) : {titre[:50]}")
    return result


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

    system = 'Tu es expert RSE. Réponds UNIQUEMENT avec ce JSON exact : {"briefing": "ton texte ici"}'
    prompt = (
        f"Textes du Journal Officiel du {TODAY} :\n\n"
        + '\n'.join(lines)
        + "\n\nEn 2-3 phrases maximum, résume ce qui est significatif pour un "
        "Responsable Environnement/Climat d'une grande entreprise de services. "
        "Si rien n'est pertinent, écris une seule phrase le disant. "
        "RÉPONDS UNIQUEMENT avec ce JSON, sans autre texte : "
        '{"briefing": "ton résumé ici"}'
    )
    raw = call_groq(prompt, system, max_tokens=300)
    result = extract_json(raw)
    if not isinstance(result, dict):
        return ''

    # Chercher la valeur texte dans n'importe quelle clé du dict
    briefing = ''
    for key in ('briefing', 'pertinence', 'summary', 'résumé', 'texte', 'content'):
        val = result.get(key, '')
        if isinstance(val, str) and len(val.strip()) > 10:
            briefing = val.strip()
            break
    # Si toujours rien, prendre la première valeur string du dict
    if not briefing:
        for val in result.values():
            if isinstance(val, str) and len(val.strip()) > 10:
                briefing = val.strip()
                break

    return briefing or "Aucun texte réglementaire significatif pour GSF dans ce JO."


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


def crawl_article_links(listing_url: str, base_url: str, max_links: int = 5) -> list:
    """
    Crawle une page de listing et extrait les liens d'articles individuels.
    Retourne une liste de dicts {titre, url}.
    Stratégie générique : cherche les <a> avec href contenant des segments d'article.
    """
    try:
        from bs4 import BeautifulSoup
        resp = requests.get(listing_url, timeout=TIMEOUT,
                           headers={'User-Agent': 'GSF-Veille/2.0'})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')

        links = []
        seen  = set()

        # Cherche les liens qui ressemblent à des articles
        for a in soup.find_all('a', href=True):
            href  = a['href'].strip()
            titre = a.get_text(strip=True)

            # Ignorer liens vides, navigation, pagination
            if not titre or len(titre) < 15:
                continue
            if any(skip in href for skip in [
                '#', 'mailto:', 'javascript:', '/tag/', '/category/',
                '/page/', '/feed', '/rss', '/newsletter', '/contact',
                '/about', '/team', '/equipe', '/linstitut',
            ]):
                continue

            # Construire l'URL absolue
            if href.startswith('http'):
                full_url = href
            elif href.startswith('/'):
                full_url = base_url.rstrip('/') + href
            else:
                continue

            # Dédupliquer
            if full_url in seen:
                continue
            seen.add(full_url)

            # Garder seulement les liens du même domaine
            if base_url.split('/')[2] not in full_url:
                continue

            links.append({'titre': titre[:200], 'url': full_url})
            if len(links) >= max_links:
                break

        return links
    except Exception as e:
        log.debug(f"crawl_article_links error {listing_url}: {e}")
    return []


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

            # Briefing exécutif — isolé pour ne pas bloquer l'analyse si erreur
            try:
                briefing = groq_briefing_jorf(all_articles)
                log.info(f"JORF briefing : {'OK' if briefing else 'vide'} ({len(briefing)} cars)")
            except Exception as e:
                log.warning(f"JORF briefing erreur (non bloquant) : {e}")
                briefing = ''

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
            fallback_url = source['fallback_crawl']
            base_url     = '/'.join(fallback_url.split('/')[:3])
            try:
                log.info(f"Fallback → {fallback_url}")
                article_links = crawl_article_links(fallback_url, base_url, max_links=5)

                if not article_links:
                    # Dernier recours : fallback générique page entière
                    contenu = crawl_article(fallback_url)
                    if contenu and keyword_match(contenu):
                        analysis = groq_analyse_rss(f"Actualités {name}", contenu)
                        if analysis.get('pertinent') is not False:
                            items.append({
                                'id'        : make_id(name, 'fallback'),
                                'source'    : name,
                                'categorie' : 'Presse',
                                'titre'     : f'Actualités {name}',
                                'resume'    : analysis.get('resume') or 'Source consultée via fallback.',
                                'pourquoi'  : '',
                                'criticite' : 1,
                                'impact_gsf': False,
                                'url'       : fallback_url,
                                'date'      : TODAY,
                            })
                else:
                    # Crawl et analyse de chaque article individuel
                    for link in article_links:
                        try:
                            contenu = crawl_article(link['url'])
                            if not contenu:
                                continue

                            titre_art = link['titre']
                            txt       = titre_art + ' ' + contenu

                            # Filtre require_keywords si présent
                            if source.get('require_keywords'):
                                if not any(k.lower() in txt.lower()
                                           for k in source['require_keywords']):
                                    continue

                            if not keyword_match(txt):
                                continue

                            analysis = groq_analyse_rss(titre_art, contenu)
                            if analysis.get('pertinent') is False:
                                continue

                            score    = int(analysis.get('score') or 1)
                            pourquoi = analysis.get('pourquoi', '') if score >= 2 else ''

                            items.append({
                                'id'        : make_id(name, titre_art),
                                'source'    : name,
                                'categorie' : categorise(txt),
                                'titre'     : titre_art,
                                'resume'    : analysis.get('resume') or titre_art,
                                'pourquoi'  : pourquoi,
                                'criticite' : score,
                                'impact_gsf': score >= 2,
                                'url'       : link['url'],
                                'date'      : TODAY,
                            })
                        except Exception as e_art:
                            log.debug(f"Fallback article {link['url']} : {e_art}")

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
# PARLEMENT — Suivi des projets de loi gouvernementaux
#
# Sources : flux RSS officiels AN + Vie-publique.fr
# Périmètre : PJL gouvernementaux environnement/énergie/climat
# Persistance : parlement_fiches.json (commité dans le repo)
#
# Budget Groq : 10 analyses max par run (nouveaux PJL uniquement)
# Suivi d'avancement : 0 appel Groq (diff RSS sur fiches existantes)
#
# Pipeline de filtrage (du moins cher au plus cher) :
#   Filtre 1 — keyword_match() sur titre+description  → 0 appel
#   Filtre 2 — détection "Projet de loi" dans titre   → 0 appel
#   Filtre 3 — groq_analyse_pjl() 1 appel             → quota
# ─────────────────────────────────────────────

PARLEMENT_FICHES   = SCRIPT_DIR / 'parlement_fiches.json'
PARLEMENT_MAX_GROQ = 10   # analyses Groq max par run sur nouveaux PJL

# Stades législatifs dans l'ordre chronologique
STADES_ORDRE = [
    'Dépôt', 'Commission', 'Séance publique AN',
    'Sénat 1ère lecture', 'Commission mixte paritaire',
    'Sénat 2ème lecture', 'AN 2ème lecture', 'Adopté', 'Promulgué',
]

AN_BASE          = 'https://www2.assemblee-nationale.fr'
AN_DOSSIERS_BASE = 'https://www.assemblee-nationale.fr/dyn/17/dossiers'

# Pages AN à scraper — triées par date de mise en ligne
AN_SCRAPER_SOURCES = [
    {
        'name': 'AN Projets de loi',
        'url' : (f'{AN_BASE}/documents/liste'
                 '?limit=30&type=projets-loi&legis=17&type_tri=DATE_MISE_LIGNE'),
    },
    {
        'name': 'AN Textes adoptés',
        'url' : (f'{AN_BASE}/documents/liste'
                 '?limit=30&type=ta&legis=17&type_tri=DATE_MISE_LIGNE'),
    },
]

# Marqueurs de stade détectables dans les titres
_STADE_PATTERNS = [
    ('Promulgué',                  ['promulgu', 'loi n°', 'parue au jo']),
    ('Adopté',                     ['adopté définitivement', 'adoption définitive',
                                    'texte adopté']),
    ('Commission mixte paritaire', ['commission mixte paritaire', 'cmp']),
    ('AN 2ème lecture',            ['2e lecture', 'deuxième lecture', 'nouvelle lecture']),
    ('Sénat 1ère lecture',         ['sénat', 'haute assemblée']),
    ('Séance publique AN',         ['séance publique', 'hémicycle', 'discussion générale']),
    ('Commission',                 ['en commission', 'examiné par la commission',
                                    'rapporteur désigné', 'renvoyé en commission']),
]


def _detect_stade_rss(titre: str, description: str = '') -> str:
    texte = (titre + ' ' + description).lower()
    for stade, patterns in _STADE_PATTERNS:
        if any(p in texte for p in patterns):
            return stade
    return 'Dépôt'


def _is_pjl_gouvernemental(titre: str) -> bool:
    t = titre.lower().strip()
    if not t.startswith('projet de loi'):
        return False
    if any(k in t for k in ['finances', 'financement de la sécurité sociale']):
        return keyword_match(titre)
    return True


def _scrape_an_listing(source: dict) -> list:
    """
    Scrape une page de listing AN (projets-loi ou textes adoptés).
    Retourne une liste d'entrées {titre, url_doc, url_dossier, date, source, fiche_id}.
    """
    from bs4 import BeautifulSoup
    entries = []
    try:
        resp = requests.get(source['url'], timeout=TIMEOUT,
                           headers={'User-Agent': 'GSF-Veille/2.0'})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')

        # Chaque PJL est dans un <li> ou <div> avec un <h3> titre
        # Structure : ### Titre - N° XXXX / Mis en ligne DATE / [Dossier] [Document]
        for item in soup.find_all(['li', 'div'], class_=re.compile(r'document|item|pjl', re.I)):
            h3 = item.find(['h3', 'h2', 'strong'])
            if not h3:
                continue
            titre = h3.get_text(strip=True)
            if not titre or len(titre) < 10:
                continue

            # Date de mise en ligne
            date_str = TODAY
            date_el = item.find(string=re.compile(r'Mis en ligne|mis en ligne', re.I))
            if date_el:
                m = re.search(r'(\d{1,2})\s+(\w+)\s+(\d{4})', str(date_el))
                if m:
                    mois_fr = {
                        'janvier':'01','février':'02','mars':'03','avril':'04',
                        'mai':'05','juin':'06','juillet':'07','août':'08',
                        'septembre':'09','octobre':'10','novembre':'11','décembre':'12'
                    }
                    mois = mois_fr.get(m.group(2).lower(), '01')
                    date_str = f"{m.group(3)}-{mois}-{int(m.group(1)):02d}"

            # Liens dossier et document
            url_dossier, url_doc = '', ''
            for a in item.find_all('a', href=True):
                href = a['href']
                txt  = a.get_text(strip=True).lower()
                if 'dossier' in txt or '/dossiers/' in href:
                    url_dossier = href if href.startswith('http') else AN_BASE + href
                elif 'document' in txt or '/projets/' in href or '/ta/' in href:
                    url_doc = href if href.startswith('http') else AN_BASE + href

            url = url_dossier or url_doc
            if not url:
                continue

            fiche_id = 'pjl-' + hashlib.md5(url.encode()).hexdigest()[:12]
            entries.append({
                'titre'      : titre,
                'description': '',
                'url'        : url,
                'url_dossier': url_dossier,
                'date'       : date_str,
                'source'     : source['name'],
                'fiche_id'   : fiche_id,
            })

        # Fallback : parser les <h3> directement si structure différente
        if not entries:
            for h3 in soup.find_all('h3'):
                titre = h3.get_text(strip=True)
                if not titre or len(titre) < 15:
                    continue
                parent = h3.find_parent(['li', 'div', 'article', 'section'])
                if not parent:
                    continue
                links = parent.find_all('a', href=True)
                url_dossier, url_doc = '', ''
                for a in links:
                    href = a['href']
                    txt  = a.get_text(strip=True).lower()
                    if 'dossier' in txt or '/dossiers/' in href:
                        url_dossier = href if href.startswith('http') else AN_BASE + href
                    elif 'document' in txt or '/projets/' in href or '/ta/' in href:
                        url_doc = href if href.startswith('http') else AN_BASE + href
                url = url_dossier or url_doc
                if not url:
                    continue
                fiche_id = 'pjl-' + hashlib.md5(url.encode()).hexdigest()[:12]
                entries.append({
                    'titre'      : titre,
                    'description': '',
                    'url'        : url,
                    'url_dossier': url_dossier,
                    'date'       : TODAY,
                    'source'     : source['name'],
                    'fiche_id'   : fiche_id,
                })

        log.info(f"Parlement scraper {source['name']} : {len(entries)} entrées")
    except Exception as e:
        log.warning(f"Parlement scraper {source['name']} : {e}")
    return entries


def _scrape_dossier_stade(url_dossier: str) -> str:
    """
    Scrape une page de dossier législatif AN pour détecter le stade actuel.
    Retourne une chaîne normalisée parmi STADES_ORDRE.
    """
    try:
        from bs4 import BeautifulSoup
        resp = requests.get(url_dossier, timeout=TIMEOUT,
                           headers={'User-Agent': 'GSF-Veille/2.0'})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        texte = soup.get_text(separator=' ', strip=True).lower()[:3000]
        return _detect_stade_rss(texte)
    except Exception as e:
        log.debug(f"scrape_dossier_stade {url_dossier}: {e}")
    return ''


def groq_analyse_pjl(titre: str, description: str) -> dict:
    """
    Évalue un PJL gouvernemental pour GSF.
    Passe 1 (8b)  : filtrage + résumé + score + horizon
    Passe 2 (70b) : enrichissement pourquoi si score >= 2
    """
    system = 'Tu es conseiller RSE senior pour GSF. JSON valide uniquement.'
    prompt = (
        f"{GSF_CONTEXT_SHORT}\n\n"
        "Évalue ce projet de loi gouvernemental pour le Responsable Environnement / Climat de GSF.\n\n"
        "PERTINENT si le texte crée ou modifie des obligations s'appliquant à GSF :\n"
        "- Obligations BGES, bilan GES, plan climat pour grandes entreprises\n"
        "- CSRD, taxonomie verte, reporting extra-financier\n"
        "- Réglementation déchets : REP, tri, traçabilité, éco-organismes\n"
        "- Énergie & bâtiment : efficacité énergétique, DPE tertiaire, renouvelables\n"
        "- Mobilité : ZFE, électrification flottes, véhicules propres\n"
        "- Politique climatique structurante : SNBC, PNACC, loi énergie-climat\n\n"
        "NON PERTINENT : ICPE, SEVESO, biocides, REACH (GSF non exploitant), "
        "agriculture, défense, santé humaine, textes sans lien avec GSF.\n"
        "EN CAS DE DOUTE : pertinent = true.\n\n"
        "Score d'urgence pour GSF :\n"
        "  1 = à connaître, horizon > 18 mois\n"
        "  2 = à anticiper, impact probable dans 6-18 mois\n"
        "  3 = impact direct imminent, action requise\n\n"
        f"TITRE: {titre}\n"
        f"DESCRIPTION: {description[:400]}\n\n"
        'JSON: {"pertinent": true/false, '
        '"resume": "ce que ce PJL change pour GSF en 1-2 phrases (vide si non pertinent)", '
        '"pourquoi": "", "score": 1, '
        '"horizon": "estimation entrée en vigueur ex: fin 2026"}'
    )
    # Passe 1 — 8b
    raw    = call_groq(prompt, system, max_tokens=300, model=GROQ_MODEL_FILTER)
    result = extract_json(raw)
    if not result:
        return {'pertinent': False, 'resume': '', 'pourquoi': '', 'score': 1, 'horizon': ''}

    score = int(result.get('score') or 1)
    if not result.get('pertinent') or score < 2:
        result['pourquoi'] = ''
        return result

    # Passe 2 — 70b : enrichissement pourquoi sur PJL à fort impact
    enrich_prompt = (
        f"{GSF_CONTEXT}\n\n"
        f"PJL retenu (score {score}/3) : {titre}\n"
        f"Résumé : {result.get('resume','')}\n"
        f"Horizon estimé : {result.get('horizon','')}\n\n"
        "En 1-2 phrases, explique POURQUOI c'est un signal stratégique pour GSF : "
        "quelle obligation concrète, quel délai d'anticipation, quel risque opérationnel.\n"
        'JSON: {"pourquoi": "..."}'
    )
    raw2 = call_groq(enrich_prompt, 'Expert RSE GSF. JSON uniquement.',
                     max_tokens=150, model=GROQ_MODEL_ENRICH)
    enrich = extract_json(raw2)
    if enrich.get('pourquoi'):
        result['pourquoi'] = enrich['pourquoi']
    log.debug(f"PJL enrichi 70b (score={score}) : {titre[:50]}")
    return result


def _load_fiches() -> dict:
    """Charge parlement_fiches.json ou retourne un dict vide."""
    if PARLEMENT_FICHES.exists():
        try:
            return json.loads(PARLEMENT_FICHES.read_text(encoding='utf-8'))
        except Exception as e:
            log.warning(f"Parlement : impossible de lire les fiches ({e}), reset")
    return {}


def _save_fiches(fiches: dict):
    """Persiste parlement_fiches.json dans le repo."""
    PARLEMENT_FICHES.write_text(
        json.dumps(fiches, ensure_ascii=False, indent=2), encoding='utf-8'
    )
    log.info(f"parlement_fiches.json : {len(fiches)} fiches")


def groq_briefing_parlement(entries: list) -> str:
    """Briefing LLM sur l'activité parlementaire scraped."""
    if not entries:
        return ''
    lines = [f"{i+1}. [{e['source']}] {e['titre']}" for i, e in enumerate(entries[:40])]
    system = 'Tu es expert RSE. Réponds UNIQUEMENT avec ce JSON exact : {"briefing": "ton texte ici"}'
    prompt = (
        f"Activité parlementaire du {TODAY} :\n\n" + '\n'.join(lines) +
        "\n\nEn 2-3 phrases, résume ce qui est significatif pour un Responsable "
        "Environnement/Climat d'une grande entreprise de services (climat, énergie, "
        "déchets, RSE, reporting). Si rien n'est pertinent, dis-le en 1 phrase. "
        'Réponds UNIQUEMENT : {"briefing": "ton résumé"}'
    )
    raw = call_groq(prompt, system, max_tokens=250)
    result = extract_json(raw)
    if not isinstance(result, dict):
        return ''
    for key in ('briefing', 'pertinence', 'summary', 'résumé', 'texte', 'content'):
        val = result.get(key, '')
        if isinstance(val, str) and len(val.strip()) > 10:
            return val.strip()
    for val in result.values():
        if isinstance(val, str) and len(val.strip()) > 10:
            return val.strip()
    return ''


def fetch_parlement() -> tuple:
    """
    Scrape les pages AN (projets de loi + textes adoptés), filtre les PJL
    environnementaux, met à jour les fiches de suivi et traite les dossiers
    ajoutés manuellement.

    Retourne : (fiches_list, pjl_autres, pjl_briefing)
    """
    log.info("=== Parlement ===")
    fiches        = _load_fiches()
    nouveaux      = 0
    maj           = 0
    groq_used     = 0
    groq_skipped  = 0

    # ── Étape 1 : Scraper les pages AN ──────────────────────────────────
    all_entries = []
    for source in AN_SCRAPER_SOURCES:
        all_entries.extend(_scrape_an_listing(source))

    # Dédupliquer par fiche_id
    seen_ids = set()
    entries  = []
    for e in all_entries:
        if e['fiche_id'] not in seen_ids:
            seen_ids.add(e['fiche_id'])
            entries.append(e)

    log.info(f"Parlement : {len(entries)} entrées uniques après déduplication")

    # Briefing LLM
    try:
        pjl_briefing = groq_briefing_parlement(entries)
    except Exception as e:
        log.warning(f"Parlement briefing erreur : {e}")
        pjl_briefing = ''

    # ── Étape 2 : Traiter chaque entrée scrapée ──────────────────────────
    for entry in entries:
        titre       = entry['titre']
        description = entry.get('description', '')
        fiche_id    = entry['fiche_id']
        stade       = _detect_stade_rss(titre, description)

        # Fiche existante → mise à jour stade via scrape du dossier
        if fiche_id in fiches:
            fiche = fiches[fiche_id]
            fiche['nouveau_stade'] = False
            url_dossier = fiche.get('url_dossier') or entry.get('url_dossier', '')
            if url_dossier:
                stade_actuel = _scrape_dossier_stade(url_dossier) or stade
                if stade_actuel and stade_actuel != fiche['stade']:
                    log.info(f"Parlement avancement : {titre[:50]} "
                             f"[{fiche['stade']} → {stade_actuel}]")
                    fiche['historique'].append({
                        'date' : TODAY,
                        'stade': stade_actuel,
                        'event': f"Avancement : {fiche['stade']} → {stade_actuel}",
                    })
                    fiche['stade']        = stade_actuel
                    fiche['stade_index']  = STADES_ORDRE.index(stade_actuel) \
                                            if stade_actuel in STADES_ORDRE else 0
                    fiche['nouveau_stade'] = True
                    fiche['updated_at']   = TODAY
                    maj += 1
            continue

        # Filtre 1 : keyword_match
        if not keyword_match(titre + ' ' + description):
            continue

        # Filtre 2 : PJL gouvernemental
        if not _is_pjl_gouvernemental(titre):
            continue

        # Filtre 3 : Groq (quota)
        if groq_used >= PARLEMENT_MAX_GROQ:
            groq_skipped += 1
            continue

        analysis = groq_analyse_pjl(titre, description)
        groq_used += 1

        if not analysis.get('pertinent'):
            continue

        fiches[fiche_id] = {
            'id'          : fiche_id,
            'titre'       : titre,
            'date_depot'  : entry['date'],
            'stade'       : stade,
            'stade_index' : STADES_ORDRE.index(stade) if stade in STADES_ORDRE else 0,
            'url_an'      : entry['url'],
            'url_dossier' : entry.get('url_dossier', ''),
            'source_rss'  : entry['source'],
            'resume_gsf'  : analysis.get('resume', ''),
            'pourquoi'    : analysis.get('pourquoi', ''),
            'score'       : int(analysis.get('score', 1)),
            'horizon'     : analysis.get('horizon', ''),
            'nouveau_stade': False,
            'manuel'      : False,
            'historique'  : [{'date': TODAY, 'stade': stade, 'event': 'Découverte'}],
            'created_at'  : TODAY,
            'updated_at'  : TODAY,
        }
        nouveaux += 1
        log.info(f"Parlement PJL retenu (score={analysis['score']}) : {titre[:60]}")

    # ── Étape 3 : Dossiers ajoutés manuellement ──────────────────────────
    # Les fiches avec manuel=True et url_dossier sont scraped pour MAJ stade
    for fiche_id, fiche in fiches.items():
        if not fiche.get('manuel'):
            continue
        fiche_id in seen_ids or seen_ids.add(fiche_id)
        fiche['nouveau_stade'] = False
        url_dossier = fiche.get('url_dossier', '')
        if not url_dossier:
            continue
        try:
            stade_actuel = _scrape_dossier_stade(url_dossier)
            if stade_actuel and stade_actuel != fiche.get('stade', ''):
                log.info(f"Parlement manuel avancement : {fiche['titre'][:50]} "
                         f"[{fiche.get('stade')} → {stade_actuel}]")
                fiche.setdefault('historique', []).append({
                    'date' : TODAY,
                    'stade': stade_actuel,
                    'event': f"Avancement : {fiche.get('stade','?')} → {stade_actuel}",
                })
                fiche['stade']        = stade_actuel
                fiche['stade_index']  = STADES_ORDRE.index(stade_actuel) \
                                        if stade_actuel in STADES_ORDRE else 0
                fiche['nouveau_stade'] = True
                fiche['updated_at']   = TODAY
                maj += 1
        except Exception as e:
            log.debug(f"Parlement manuel MAJ {fiche_id}: {e}")

    # Reset nouveau_stade pour les fiches non vues
    for fiche_id, fiche in fiches.items():
        if fiche_id not in seen_ids and not fiche.get('manuel'):
            fiche['nouveau_stade'] = False

    _save_fiches(fiches)
    log.info(f"Parlement : {nouveaux} nouveaux, {maj} avancements, "
             f"{groq_used} appels Groq, {groq_skipped} PJL différés (quota)")

    # pjl_autres groupés par source
    groups = {}
    for e in entries:
        src = e['source']
        if src not in groups:
            groups[src] = []
        groups[src].append({
            'titre'      : e['titre'],
            'url'        : e['url'],
            'url_dossier': e.get('url_dossier', ''),
            'date'       : e['date'],
        })
    pjl_autres = [{'source': src, 'items': items} for src, items in groups.items()]

    fiches_list = sorted(
        fiches.values(),
        key=lambda f: (f.get('score', 1), f.get('stade_index', 0)),
        reverse=True,
    )
    return fiches_list, pjl_autres, pjl_briefing


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

    # 5. Parlement — suivi des PJL environnementaux
    pjl_fiches   = []
    pjl_autres   = []
    pjl_briefing = ''
    try:
        pjl_fiches, pjl_autres, pjl_briefing = fetch_parlement()
        stats['pjl_suivis']      = len(pjl_fiches)
        stats['pjl_avancements'] = sum(1 for f in pjl_fiches if f.get('nouveau_stade'))
    except Exception as e:
        log.error(f"Parlement fatal : {e}")
        errors.append('Parlement')
        stats.update({'pjl_suivis': 0, 'pjl_avancements': 0})

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
        'pjl_fiches'      : pjl_fiches,
        'pjl_autres'      : pjl_autres,
        'pjl_briefing'    : pjl_briefing,
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
