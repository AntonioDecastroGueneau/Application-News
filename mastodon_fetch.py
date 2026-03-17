#!/usr/bin/env python3
"""
Récupère la home timeline Mastodon, filtre par pertinence via Groq,
et écrit data/mastodon.json.
Exécuté via GitHub Actions — MASTODON_TOKEN et GROQ_API_KEY en variables d'environnement.
"""

import os
import sys
import json
import html
import re
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
from groq import Groq

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

MASTODON_API = 'https://mastodon.social/api/v1/timelines/home'
GROQ_MODEL   = 'llama-3.3-70b-versatile'
SCRIPT_DIR   = Path(__file__).parent.resolve()
OUT_FILE     = SCRIPT_DIR / 'data' / 'mastodon.json'
LIMIT        = 40   # on en prend plus, on filtre ensuite


def strip_html(text: str) -> str:
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html.unescape(text)
    return re.sub(r'\n{3,}', '\n\n', text).strip()


def parse_post(status: dict) -> dict:
    reblog = status.get('reblog')
    src    = reblog if reblog else status
    return {
        'id'               : status['id'],
        'url'              : status.get('url') or status.get('uri', ''),
        'created_at'       : status['created_at'],
        'is_reblog'        : reblog is not None,
        'reblogged_by'     : status['account']['display_name'] if reblog else None,
        'reblogged_by_acct': status['account']['acct']         if reblog else None,
        'account_name'     : src['account']['display_name'],
        'account_acct'     : src['account']['acct'],
        'content'          : strip_html(src.get('content', '')),
        'reblogs_count'    : src.get('reblogs_count', 0),
        'favourites_count' : src.get('favourites_count', 0),
    }


def groq_filter(posts: list, groq_key: str) -> list:
    """
    Envoie tous les posts à Groq en un seul appel batch.
    Retourne la liste filtrée aux posts pertinents, avec un champ 'raison' ajouté.
    """
    if not posts:
        return []

    client = Groq(api_key=groq_key)

    lines = []
    for i, p in enumerate(posts):
        preview = p['content'][:200].replace('\n', ' ')
        lines.append(f"{i}. [{p['account_acct']}] {preview}")

    system = (
        "Tu es un filtre de pertinence pour un Responsable Climat et Environnement. "
        "Reponds uniquement en JSON valide."
    )
    prompt = (
        "Voici des posts Mastodon issus de la timeline d'un professionnel climat/environnement :\n\n"
        + '\n'.join(lines)
        + "\n\nGARDER uniquement les posts qui apportent une information substantielle :\n"
        "- Publication d'un nouveau rapport ou étude (GIEC, HCC, ADEME, I4CE, etc.)\n"
        "- Annonce de politique publique, loi, décret, règlement\n"
        "- Donnée chiffrée ou statistique notable\n"
        "- Événement climatique significatif\n"
        "- Thread ou analyse de fond sur un sujet climat/environnement/énergie\n\n"
        "REJETER : messages personnels, bonjour/bonsoir, humour sans info, "
        "opinions sans substance, rediffusions de vieux contenus, promotions d'événements mineurs.\n\n"
        "Pour chaque post retenu, donne un indice de pertinence (1=utile, 2=important, 3=incontournable).\n"
        'JSON: {"retenu": [{"idx": 0, "score": 1}]}'
    )

    try:
        resp = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {'role': 'system', 'content': system},
                {'role': 'user',   'content': prompt},
            ],
            max_tokens=400,
            temperature=0.1,
            response_format={'type': 'json_object'},
        )
        result = json.loads(resp.choices[0].message.content)
        retenu = result.get('retenu', [])
        log.info(f"Groq : {len(retenu)}/{len(posts)} posts retenus")

        idx_score = {item['idx']: item.get('score', 1) for item in retenu}
        filtered = []
        for i, p in enumerate(posts):
            if i in idx_score:
                p['score'] = idx_score[i]
                filtered.append(p)
        # Trier par score décroissant
        filtered.sort(key=lambda x: -x.get('score', 1))
        return filtered

    except Exception as e:
        log.warning(f"Groq filter error : {e} — retour sans filtrage")
        return posts  # fallback : tout garder si Groq échoue


def main() -> int:
    token     = os.environ.get('MASTODON_TOKEN', '').strip()
    groq_key  = os.environ.get('GROQ_API_KEY', '').strip()

    if not token:
        log.error("MASTODON_TOKEN manquant")
        return 1
    if not groq_key:
        log.warning("GROQ_API_KEY absent — filtrage Groq désactivé")

    # 1. Fetch timeline
    try:
        log.info(f"Récupération timeline Mastodon ({LIMIT} posts)…")
        resp = requests.get(
            MASTODON_API,
            headers={'Authorization': f'Bearer {token}'},
            params={'limit': LIMIT},
            timeout=30,
        )
        resp.raise_for_status()
        statuses = resp.json()
    except requests.HTTPError as e:
        log.error(f"HTTP {e.response.status_code} : {e.response.text[:200]}")
        return 1
    except Exception as e:
        log.error(f"Erreur réseau : {e}")
        return 1

    # 2. Parse
    posts = [parse_post(s) for s in statuses]
    log.info(f"{len(posts)} posts récupérés")

    # 3. Filtre Groq
    if groq_key:
        posts = groq_filter(posts, groq_key)

    # 4. Écriture
    OUT_FILE.parent.mkdir(exist_ok=True)
    OUT_FILE.write_text(
        json.dumps({
            'fetched_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'posts'     : posts,
        }, ensure_ascii=False, indent=2),
        encoding='utf-8',
    )
    log.info(f"OK — {len(posts)} posts pertinents écrits dans {OUT_FILE}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
