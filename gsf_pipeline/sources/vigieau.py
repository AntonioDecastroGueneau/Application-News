import csv
import io
import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import requests

from ..config import (
    DATAGOUV_API,
    DATAGOUV_DATASET_ID,
    DEPT_NOMS,
    NIVEAUX_GRAVITE,
    NIVEAUX_ORDRE,
    VIGIEAU_DEPTS_URL,
    VIGIEAU_FALLBACK_URLS,
    TIMEOUT,
)

log = logging.getLogger(__name__)


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
                'dept_nom': dept.get('nom', ''),
                'niveau': niv,
                'niveauSup': dept.get('niveauGraviteSupMax'),
                'niveauSou': dept.get('niveauGraviteSouMax'),
                'niveauAep': dept.get('niveauGraviteAepMax'),
            })
        restrictions.sort(key=lambda x: NIVEAUX_ORDRE.get(x['niveau'], 0), reverse=True)
        log.info(f"VigiEau : {len(restrictions)} départements en restriction")
    except Exception as e:
        log.error(f"VigiEau fatal : {e}", exc_info=True)
    return restrictions


def _normalize_niveau(niveau: str) -> str:
    n = (niveau or '').lower().strip().replace('_', ' ')
    n = n.replace('renforcee', 'renforcée').replace('renforcé', 'renforcée')
    return n


def _parse_vigieau_csv(content: str, year: int, compute_daily: bool = False) -> dict:
    par_mois = defaultdict(lambda: {n: 0 for n in NIVEAUX_GRAVITE})
    par_dept = defaultdict(lambda: {'nom': '', 'jours': {n: 0 for n in NIVEAUX_GRAVITE}})
    par_jour = defaultdict(lambda: {n: 0 for n in NIVEAUX_GRAVITE}) if compute_daily else None

    reader = csv.DictReader(io.StringIO(content), delimiter=',')
    headers = reader.fieldnames or []
    log.debug(f"CSV colonnes ({year}): {headers[:15]}")

    rows_ok = 0
    rows_skip = 0

    for row in reader:
        try:
            date_debut_str = (row.get('date_debut', '') or '').strip()
            date_fin_str = (row.get('date_fin', '') or '').strip()

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
                    parsed = json.loads(niveau_raw_col)
                    niveaux_liste = [_normalize_niveau(n) for n in parsed if n]
                except Exception:
                    pass
            elif niveau_raw_col:
                niveaux_liste = [_normalize_niveau(niveau_raw_col)]

            ordre_gravite = {n: i for i, n in enumerate(NIVEAUX_GRAVITE)}
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
            year_end = datetime(year, 12, 31)
            d_start = max(date_debut, year_start)
            d_end = min(date_fin, year_end)

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

    log.info(
        f"  Parse CSV {year}: {rows_ok} lignes traitées, {rows_skip} ignorées, "
        f"{len(par_mois)} mois, {len(par_dept)} depts"
    )

    mois_out = {k: dict(v) for k, v in sorted(par_mois.items())}
    dept_out = {}
    for code, data in par_dept.items():
        graves = (
            data['jours'].get('alerte', 0)
            + data['jours'].get('alerte renforcée', 0)
            + data['jours'].get('crise', 0)
        )
        dept_out[code] = {
            'nom': data['nom'],
            'jours': dict(data['jours']),
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
                    'nom': DEPT_NOMS.get(code, info.get('nom', code)),
                    'total_graves': 0,
                    'jours': {n: 0 for n in NIVEAUX_GRAVITE},
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
        'updated_at': datetime.now().isoformat(),
        'annees': annees_cache,
        'top10_depts': top10_out,
        'comparaison': {
            str(y): annees_cache.get(str(y), {}).get('par_jour', {})
            for y in [datetime.now().year - 1, datetime.now().year]
        },
    }
    history_file.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    log.info(f"vigieau_history.json : {len(annees_cache)} années, {len(top10_out)} depts top10")
    return result


def fetch_vigieau_history(script_dir: Path) -> dict:
    """Download and aggregate historical VigiEau restrictions (data.gouv.fr)."""
    log.info("=== VigiEau Historique ===")

    history_file = script_dir / 'vigieau_history.json'
    current_year = datetime.now().year
    vigieau_history_years = list(range(2020, current_year + 1))

    existing = {}
    if history_file.exists():
        try:
            existing = json.loads(history_file.read_text(encoding='utf-8'))
            log.info(f"Cache existant : années {sorted(existing.get('annees', {}).keys())}")
        except Exception as e:
            log.warning(f"Cache VigiEau illisible : {e}")

    annees_cache = existing.get('annees', {})
    csv_par_annee = {}
    comprehensive_url = None

    try:
        resp = requests.get(
            f"{DATAGOUV_API}{DATAGOUV_DATASET_ID}/",
            timeout=TIMEOUT,
            headers={'User-Agent': 'GSF-Veille/2.0'},
        )
        resp.raise_for_status()
        resources = resp.json().get('resources', [])
        log.info(f"API data.gouv.fr : {len(resources)} ressources")

        for res in resources:
            title = res.get('title', '') or ''
            fmt = (res.get('format', '') or '').lower()
            url = res.get('url', '') or ''
            filetype = res.get('filetype', '') or ''

            is_csv = (fmt in ('csv', 'text/csv') or url.lower().endswith('.csv') or filetype.lower() == 'file')
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
            for y in vigieau_history_years:
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

    if not csv_par_annee:
        log.info("Fallback sur URLs hardcodées")
        for year, url in VIGIEAU_FALLBACK_URLS.items():
            csv_par_annee[year] = url

    if not csv_par_annee:
        log.warning("Aucun CSV VigiEau identifié — historique non mis à jour")
        return _save_and_return_history(history_file, annees_cache)

    log.info(f"CSV à traiter : {sorted(csv_par_annee.keys())}")

    for year in sorted(csv_par_annee.keys()):
        url = csv_par_annee[year]
        year_str = str(year)

        if year_str in annees_cache and year != current_year:
            cached = annees_cache[year_str]
            if cached.get('par_mois') and cached.get('par_dept'):
                log.info(f"Année {year} : cache OK ({len(cached['par_mois'])} mois)")
                continue

        try:
            log.info(f"Téléchargement CSV {year} : {url[-60:]}")
            r = requests.get(
                url,
                timeout=120,
                headers={'User-Agent': 'GSF-Veille/2.0'},
                allow_redirects=True,
            )
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

    if comprehensive_url:
        missing_years = [
            y for y in range(current_year - 1, current_year + 1)
            if str(y) not in annees_cache or not annees_cache[str(y)].get('par_mois')
        ]
        if missing_years:
            log.info(f"Années manquantes {missing_years} → téléchargement fichier compréhensif")
            try:
                r = requests.get(
                    comprehensive_url,
                    timeout=180,
                    headers={'User-Agent': 'GSF-Veille/2.0'},
                    allow_redirects=True,
                )
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
                            log.info(
                                f"  Année {y} ajoutée : {len(stats['par_mois'])} mois, "
                                f"{len(stats.get('par_jour', {}))} jours"
                            )
                        else:
                            log.warning(f"  Année {y} : aucune donnée parsée depuis le fichier compréhensif")
            except Exception as e:
                log.warning(f"Fichier compréhensif erreur : {e}")

    return _save_and_return_history(history_file, annees_cache)

