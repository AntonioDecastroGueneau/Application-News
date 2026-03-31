import logging

import requests

from ..config import (
    NIVEAUX_ORDRE,
    DEPT_NOMS,
    VIGIEAU_DEPTS_URL,
    VIGIEAU_PMTILES_URL,
    TIMEOUT,
)

log = logging.getLogger(__name__)


def fetch_vigieau():
    """
    Return dept-level restriction summaries for the sidebar and JSON output.
    Derived from PMTiles zones (same source as the map) — the REST API is seasonal.
    One entry per dept, keeping the highest severity level found across zones.
    """
    log.info("=== VigiEau ===")
    zones = fetch_vigieau_zones()
    if not zones:
        log.warning("VigiEau : aucune zone trouvée via PMTiles")
        return []

    # Aggregate to dept level: keep highest niveau per dept
    dept_max: dict = {}
    for z in zones:
        dept = z.get('departement', '')
        if not dept:
            continue
        niveau = z.get('niveau_actuel', '')
        order = NIVEAUX_ORDRE.get(niveau, 0)
        if order > NIVEAUX_ORDRE.get(dept_max.get(dept, {}).get('niveau', ''), 0):
            dept_max[dept] = {'dept_code': dept, 'dept_nom': DEPT_NOMS.get(dept, dept), 'niveau': niveau}

    restrictions = sorted(dept_max.values(), key=lambda x: NIVEAUX_ORDRE.get(x['niveau'], 0), reverse=True)
    log.info(f"VigiEau : {len(restrictions)} départements en restriction")
    return restrictions


def fetch_vigieau_zones() -> list:
    """
    Extract restriction zones from the PMTiles file served by VigiEau.

    The REST API (api.vigieau.gouv.fr) is seasonal and offline in winter.
    The PMTiles file is always up-to-date (same source as the public map).
    Tiles are gzip-compressed MVT; we scan zoom=5 which covers all of France
    in ~12 tiles and already contains all zone polygons with full properties.
    """
    import gzip
    import json
    import math
    import urllib.request

    try:
        from pmtiles.reader import Reader
        import mapbox_vector_tile
    except ImportError:
        log.warning("VigiEau zones: pmtiles ou mapbox-vector-tile non installé — skip")
        return []

    def _lon_to_x(lon, z): return int((lon + 180) / 360 * 2 ** z)
    def _lat_to_y(lat, z):
        r = math.radians(lat)
        return int((1 - math.log(math.tan(r) + 1 / math.cos(r)) / math.pi) / 2 * 2 ** z)

    zones = {}
    try:
        with urllib.request.urlopen(VIGIEAU_PMTILES_URL, timeout=TIMEOUT) as resp:
            data = resp.read()

        buf = data
        reader = Reader(lambda o, l: buf[o:o + l])

        Z = 5
        # Bounding box covering metropolitan France + Corsica
        for x in range(_lon_to_x(-6, Z), _lon_to_x(12, Z) + 1):
            for y in range(_lat_to_y(52, Z), _lat_to_y(40, Z) + 1):
                tile = reader.get(Z, x, y)
                if not tile:
                    continue
                try:
                    raw = gzip.decompress(tile)
                    decoded = mapbox_vector_tile.decode(raw)
                except Exception:
                    continue
                for feat in decoded.get('zones_arretes_en_vigueur', {}).get('features', []):
                    props = feat.get('properties', {})
                    code = props.get('code') or str(props.get('id', ''))
                    if not code or code in zones:
                        continue
                    type_eau = (props.get('type') or '').upper()
                    if type_eau not in ('AEP', 'SUP', 'SOU'):
                        continue
                    niveau = (props.get('niveauGravite') or '').lower().replace('_renforcee', ' renforcée').replace('_', ' ')
                    if not niveau:
                        continue
                    arrete = {}
                    try:
                        arrete = json.loads(props.get('arreteRestriction') or '{}')
                    except Exception:
                        pass
                    dept = {}
                    try:
                        dept = json.loads(props.get('departement') or '{}')
                    except Exception:
                        pass
                    zones[code] = {
                        'code_zone':    code,
                        'nom_zone':     props.get('nom', ''),
                        'type_eau':     type_eau,
                        'niveau_actuel': niveau,
                        'departement':  dept.get('code', '') if isinstance(dept, dict) else str(dept),
                        'url_arrete':   arrete.get('fichier') or arrete.get('cheminFichier', ''),
                        'date_debut':   arrete.get('dateDebut', ''),
                    }

        log.info(f"VigiEau zones (PMTiles) : {len(zones)} zones en restriction")
    except Exception as e:
        log.error(f"VigiEau zones fatal : {e}", exc_info=True)

    return list(zones.values())
