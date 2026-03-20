import logging
import sys
from datetime import datetime
from pathlib import Path

from .output import write_output
from .sources.jorf import fetch_jorf
from .sources.parlement import fetch_parlement
from .sources.rss import fetch_rss
from .sources.vigieau import fetch_vigieau, fetch_vigieau_history
from .config import GROQ_MODEL


def _setup_logging(log_path: Path):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_path, encoding='utf-8'),
            logging.StreamHandler(sys.stdout),
        ],
    )


def main() -> int:
    today_str = datetime.now().strftime('%Y-%m-%d')

    # The package lives in `NEWS/gsf_pipeline`, so the workspace root is parents[1].
    script_dir = Path(__file__).resolve().parents[1]
    log_path = script_dir / 'pipeline.log'
    _setup_logging(log_path)
    log = logging.getLogger(__name__)

    log.info('=' * 60)
    log.info(f'Pipeline GSF Veille Environnementale — {today_str}')
    log.info(f'Modèle LLM : {GROQ_MODEL} via Groq API')
    log.info('=' * 60)

    start = datetime.now()
    errors = []
    items = []
    stats = {}
    restrictions = []
    jorf_autres = []
    jorf_items = []
    briefing_jorf = []

    # 1. JORF
    try:
        jorf_items, jorf_autres, jorf_total, briefing_jorf = fetch_jorf(today_str)
        stats['jo_analyses'] = jorf_total
        stats['jo_retenus'] = len(jorf_items)
    except Exception as e:
        log.error(f"JORF fatal : {e}")
        errors.append('JORF')
        stats.update({'jo_analyses': 0, 'jo_retenus': 0})

    # 2. RSS / Presse
    try:
        rss_items = fetch_rss(today_str)
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

    # 4. VigiEau historique
    try:
        fetch_vigieau_history(script_dir)
    except Exception as e:
        log.error(f"VigiEau history fatal : {e}")
        errors.append('VigiEau_history')

    # 5. Parlement
    pjl_fiches = []
    pjl_autres = []
    pjl_briefing = ''
    try:
        pjl_fiches, pjl_autres, pjl_briefing = fetch_parlement(script_dir, today_str)
        stats['pjl_suivis'] = len(pjl_fiches)
        stats['pjl_avancements'] = sum(1 for f in pjl_fiches if f.get('nouveau_stade'))
    except Exception as e:
        log.error(f"Parlement fatal : {e}")
        errors.append('Parlement')
        stats.update({'pjl_suivis': 0, 'pjl_avancements': 0})

    # Dedup + sort by criticite
    seen = set()
    unique = []
    for item in sorted(
        items,
        key=lambda x: (x.get('date', '2000-01-01'), x.get('criticite', 1)),
        reverse=True,
    ):
        if item['id'] not in seen:
            seen.add(item['id'])
            unique.append(item)

    for i, item in enumerate(unique):
        item['top5'] = i < 5

    elapsed = round((datetime.now() - start).total_seconds())
    json_data = {
        'date': today_str,
        'generated_at': datetime.now().strftime('%H:%M'),
        'elapsed_seconds': elapsed,
        'errors': errors,
        'stats': stats,
        'items': unique,
        'restrictions_eau': restrictions,
        'jo_autres': jorf_autres,
        'jo_retenus': jorf_items,
        'briefing_jorf': briefing_jorf,
        'pjl_fiches': pjl_fiches,
        'pjl_autres': pjl_autres,
        'pjl_briefing': pjl_briefing,
    }

    log.info(
        f"JSON : {len(unique)} items RSS, {len(jorf_items)} items JORF, "
        f"{len(restrictions)} depts eau"
    )
    write_output(script_dir, json_data, today_str)

    duration = (datetime.now() - start).total_seconds()
    status = 'OK' if not errors else 'PARTIEL'
    log.info(f"Pipeline terminé en {duration:.1f}s — {status}")
    return 0 if not errors else 1


if __name__ == '__main__':
    sys.exit(main())

