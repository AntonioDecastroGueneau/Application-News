import json
import logging
import os
import re
import time

from groq import Groq
from mistralai.client import Mistral

from .config import (
    GSF_CONTEXT,
    GSF_CONTEXT_SHORT,
    GROQ_MAX_RETRY,
    GROQ_MODEL_ENRICH,
    GROQ_MODEL_FILTER,
    GROQ_MIN_INTERVAL,
    GROQ_RETRY_WAIT,
    MISTRAL_MODEL,
    MISTRAL_MIN_INTERVAL,
)

log = logging.getLogger(__name__)


_mistral_client = None
_groq_client = None
_mistral_last_call: float = 0.0
_groq_last_call: float = 0.0


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
    """Call Mistral with a 1 req/s rate limiter."""
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
            {'role': 'user', 'content': prompt},
        ],
        max_tokens=max_tokens,
        temperature=0.2,
        response_format={"type": "json_object"},
    )
    content = resp.choices[0].message.content
    # Some SDKs can return a parsed dict when using json_object
    if isinstance(content, dict):
        return json.dumps(content, ensure_ascii=False)
    return str(content).strip()


def _call_groq_fallback(prompt: str, system: str, max_tokens: int, model: str) -> str:
    """Groq fallback with rate limiter and retry."""
    global _groq_last_call
    elapsed = time.time() - _groq_last_call
    if elapsed < GROQ_MIN_INTERVAL:
        time.sleep(GROQ_MIN_INTERVAL - elapsed)

    client = _get_groq_client()
    sys_content = system if 'json' in system.lower() else (system + ' Reponds en JSON.').strip()
    messages = [
        {'role': 'system', 'content': sys_content},
        {'role': 'user', 'content': prompt},
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
                log.warning(
                    f"Groq fallback rate limit [{model}] "
                    f"(tentative {attempt}/{GROQ_MAX_RETRY}) — attente {GROQ_RETRY_WAIT}s"
                )
                time.sleep(GROQ_RETRY_WAIT)
                _groq_last_call = time.time()
            else:
                log.warning(f"Groq fallback erreur [{model}] : {e}")
                return ''

    log.error(f"Groq fallback [{model}] : toutes les tentatives épuisées")
    return ''


def call_llm(prompt: str, system: str = '', max_tokens: int = 300, enrich: bool = False) -> str:
    """Unified LLM entry-point used by the whole pipeline."""
    try:
        result = _call_mistral(prompt, system, max_tokens)
        if result:
            return result
    except Exception as e:
        err_str = str(e)
        if '429' in err_str or 'rate_limit' in err_str.lower() or 'quota' in err_str.lower():
            log.warning("Mistral rate limit → fallback Groq")
        else:
            log.warning(f"Mistral erreur → fallback Groq : {e}")

    groq_model = GROQ_MODEL_ENRICH if enrich else GROQ_MODEL_FILTER
    return _call_groq_fallback(prompt, system, max_tokens, groq_model)


def call_groq(prompt: str, system: str = '', max_tokens: int = 300, model: str = None) -> str:
    """Compatibility alias (pipeline called `call_groq` originally)."""
    enrich = (model == GROQ_MODEL_ENRICH)
    return call_llm(prompt, system, max_tokens, enrich=enrich)


def extract_json(text: str) -> dict:
    """Extract the first valid JSON object from a string."""
    try:
        return json.loads(text)
    except Exception:
        pass

    # Fallback: try to locate the first {...} block
    match = re.search(r'\{.*?\}', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except Exception:
            pass
    return {}


# ─────────────────────────────────────────────
# LLM — JORF analysis
# ─────────────────────────────────────────────

def groq_analyse_jorf(titre: str, contenu: str) -> dict:
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

    raw = call_groq(prompt, system, max_tokens=300, model=GROQ_MODEL_FILTER)
    result = extract_json(raw)
    if not result:
        return {'pertinent': False, 'resume': '', 'pourquoi': '', 'score': 1}

    score = int(result.get('score') or 1)
    if not result.get('pertinent') or score < 2:
        result['pourquoi'] = ''
        return result

    enrich_prompt = (
        f"{GSF_CONTEXT}\n\n"
        f"Texte JO retenu (score {score}/3) : {titre}\n"
        f"Résumé : {result.get('resume','')}\n\n"
        "En 1-2 phrases précises, explique POURQUOI c'est un signal stratégique important pour GSF : "
        "quelle obligation concrète, quel délai, quel risque ou quelle opportunité.\n"
        'JSON: {"pourquoi": "..."}'
    )
    raw2 = call_groq(enrich_prompt, 'Expert RSE GSF. JSON uniquement.', max_tokens=150, model=GROQ_MODEL_ENRICH)
    enrich = extract_json(raw2)
    if enrich.get('pourquoi'):
        result['pourquoi'] = enrich['pourquoi']

    log.debug(f"JORF enrichi 70b (score={score}) : {titre[:50]}")
    return result


# ─────────────────────────────────────────────
# LLM — RSS / Presse analysis
# ─────────────────────────────────────────────

def groq_analyse_rss(titre: str, contenu: str) -> dict:
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
        'JSON: {"pertinent": true/false, "resume": "1-2 phrases sur ce que ça change pour GSF (vide si non pertinent)", "pourquoi": "", "score": 1}'
    )

    raw = call_groq(prompt, system, max_tokens=300, model=GROQ_MODEL_FILTER)
    result = extract_json(raw)
    if not result:
        return {'pertinent': False, 'resume': '', 'pourquoi': '', 'score': 1}

    score = int(result.get('score') or 1)
    if not result.get('pertinent') or score < 2:
        result['pourquoi'] = ''
        return result

    enrich_prompt = (
        f"{GSF_CONTEXT}\n\n"
        f"Article retenu (score {score}/3) : {titre}\n"
        f"Résumé : {result.get('resume','')}\n\n"
        "En 1-2 phrases précises, explique POURQUOI c'est un signal stratégique important pour GSF : "
        "quelle implication concrète, quel délai d'action, quel risque ou quelle opportunité.\n"
        'JSON: {"pourquoi": "..."}'
    )
    raw2 = call_groq(enrich_prompt, 'Expert RSE GSF. JSON uniquement.', max_tokens=150, model=GROQ_MODEL_ENRICH)
    enrich = extract_json(raw2)
    if enrich.get('pourquoi'):
        result['pourquoi'] = enrich['pourquoi']

    log.debug(f"RSS enrichi 70b (score={score}) : {titre[:50]}")
    return result


# ─────────────────────────────────────────────
# LLM — JORF executive briefing
# ─────────────────────────────────────────────

_BRIEFING_SKIP = [
    'nomination', 'délégation de signature', 'désignation',
    'portant nomination', 'portant délégation', 'délégation de pouvoir',
    'cessation de fonctions',
]


def groq_briefing_jorf(articles: list, today_str: str) -> str:
    """Generate a daily executive briefing from JORF significant texts."""
    if not articles:
        return ''

    candidates = [a for a in articles if not any(p in a['titre'].lower() for p in _BRIEFING_SKIP)]
    if not candidates:
        return ''

    lines = []
    for i, a in enumerate(candidates[:80]):
        nature = a.get('contenu', '').split(' — ')[0] or 'Texte'
        lines.append(f"{i + 1}. [{nature}] {a['titre']}")

    system = 'Tu es expert RSE. Réponds UNIQUEMENT avec ce JSON exact : {"briefing": "ton texte ici"}'
    prompt = (
        f"Textes du Journal Officiel du {today_str} :\n\n"
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

    briefing = ''
    for key in ('briefing', 'pertinence', 'summary', 'résumé', 'texte', 'content'):
        val = result.get(key, '')
        if isinstance(val, str) and len(val.strip()) > 10:
            briefing = val.strip()
            break

    if not briefing:
        for val in result.values():
            if isinstance(val, str) and len(val.strip()) > 10:
                briefing = val.strip()
                break

    return briefing or "Aucun texte réglementaire significatif pour GSF dans ce JO."


# ─────────────────────────────────────────────
# LLM — PJL analysis (Parlement)
# ─────────────────────────────────────────────

def groq_analyse_pjl(titre: str, description: str) -> dict:
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
        'JSON: {"pertinent": true/false, "resume": "ce que ce PJL change pour GSF en 1-2 phrases (vide si non pertinent)", '
        '"pourquoi": "", "score": 1, "horizon": "estimation entrée en vigueur ex: fin 2026"}'
    )

    raw = call_groq(prompt, system, max_tokens=300, model=GROQ_MODEL_FILTER)
    result = extract_json(raw)
    if not result:
        return {'pertinent': False, 'resume': '', 'pourquoi': '', 'score': 1, 'horizon': ''}

    score = int(result.get('score') or 1)
    if not result.get('pertinent') or score < 2:
        result['pourquoi'] = ''
        return result

    enrich_prompt = (
        f"{GSF_CONTEXT}\n\n"
        f"PJL retenu (score {score}/3) : {titre}\n"
        f"Résumé : {result.get('resume','')}\n"
        f"Horizon estimé : {result.get('horizon','')}\n\n"
        "En 1-2 phrases, explique POURQUOI c'est un signal stratégique pour GSF : "
        "quelle obligation concrète, quel délai d'anticipation, quel risque opérationnel.\n"
        'JSON: {"pourquoi": "..."}'
    )
    raw2 = call_groq(enrich_prompt, 'Expert RSE GSF. JSON uniquement.', max_tokens=150, model=GROQ_MODEL_ENRICH)
    enrich = extract_json(raw2)
    if enrich.get('pourquoi'):
        result['pourquoi'] = enrich['pourquoi']

    log.debug(f"PJL enrichi 70b (score={score}) : {titre[:50]}")
    return result


# ─────────────────────────────────────────────
# LLM — Parlement executive briefing
# ─────────────────────────────────────────────

def groq_briefing_parlement(entries: list, today_str: str) -> str:
    if not entries:
        return ''

    lines = [f"{i + 1}. [{e['source']}] {e['titre']}" for i, e in enumerate(entries[:40])]
    system = 'Tu es expert RSE. Réponds UNIQUEMENT avec ce JSON exact : {"briefing": "ton texte ici"}'
    prompt = (
        f"Activité parlementaire du {today_str} :\n\n" + '\n'.join(lines) +
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

