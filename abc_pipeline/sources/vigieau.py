import logging

import requests

from ..config import (
    NIVEAUX_ORDRE,
    VIGIEAU_DEPTS_URL,
    VIGIEAU_RESTRICTIONS_URL,
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
            })
        restrictions.sort(key=lambda x: NIVEAUX_ORDRE.get(x['niveau'], 0), reverse=True)
        log.info(f"VigiEau : {len(restrictions)} départements en restriction")
    except Exception as e:
        log.error(f"VigiEau fatal : {e}", exc_info=True)
    return restrictions


def fetch_vigieau_zones() -> list:
    """
    Fetch zone-level restriction data for Supabase persistence.

    Expected API response shape (verify if the endpoint evolves):
    [
      {
        "niveauGravite": "alerte",
        "cheminFichier": "https://...",
        "zoneAlerte": {
          "code": "75_SUP_001",
          "nom": "Zone SUP Seine Paris",
          "type": "AEP" | "SUP" | "SOU",
          "departement": {"code": "75", "nom": "Paris"}
        }
      }, ...
    ]
    """
    zones = []
    try:
        resp = requests.get(VIGIEAU_RESTRICTIONS_URL, timeout=TIMEOUT)
        resp.raise_for_status()
        for item in resp.json():
            za = item.get('zoneAlerte') or {}
            dept = za.get('departement') or {}
            type_eau = (za.get('type') or '').upper()
            if type_eau not in ('AEP', 'SUP', 'SOU'):
                continue
            # API uses underscores in some niveauGravite values (e.g. "alerte_renforcée")
            niveau = (item.get('niveauGravite') or '').lower().replace('_', ' ')
            if not niveau:
                continue
            code = za.get('code') or str(item.get('id', ''))
            if not code:
                continue
            zones.append({
                'code_zone':    code,
                'nom_zone':     za.get('nom') or '',
                'departement':  dept.get('code') if isinstance(dept, dict) else str(dept),
                'type_eau':     type_eau,
                'niveau_actuel': niveau,
                'url_arrete':   item.get('cheminFichier') or '',
            })
        log.info(f"VigiEau zones : {len(zones)} zones en restriction")
    except Exception as e:
        log.error(f"VigiEau zones fatal : {e}", exc_info=True)
    return zones
