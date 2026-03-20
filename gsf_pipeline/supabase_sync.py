"""
Supabase sync for legislative dossiers (Veille GSF).

Bidirectional bridge between:
  - parlement_fiches.json  → pipeline source of truth for scraping state
  - Supabase               → UI persistence (manual dossiers, comments, stage events)

Usage:
  from .supabase_sync import SupabaseSync
  sync = SupabaseSync()          # connects lazily; safe if env vars absent
  sync.load_manuel_dossiers()    # pull UI-added dossiers → merge into fiches
  sync.upsert_dossier(fiche)     # push pipeline discovery / updates
  sync.record_stage_change(...)  # log stage advancement
"""

import logging
import os
from typing import Optional

log = logging.getLogger(__name__)


class SupabaseSync:
    def __init__(self):
        self._client = None
        self._ready = False
        self._init()

    def _init(self):
        url = os.environ.get('SUPABASE_URL', '').strip()
        key = os.environ.get('SUPABASE_ANON_KEY', '').strip()
        if not url or not key:
            log.info("Supabase: variables SUPABASE_URL / SUPABASE_ANON_KEY absentes — sync désactivé")
            return
        try:
            from supabase import create_client
            self._client = create_client(url, key)
            self._ready = True
            log.info("Supabase: client initialisé")
        except ImportError:
            log.warning("Supabase: package 'supabase' non installé — sync désactivé")
        except Exception as e:
            log.warning(f"Supabase: échec d'initialisation — {e}")

    @property
    def ready(self) -> bool:
        return self._ready

    # ── Read ──────────────────────────────────────────────────────────

    def load_manuel_dossiers(self) -> list:
        """
        Pull dossiers added manually from the UI (source = 'manuel').
        Returns a list of dicts compatible with the parlement_fiches format.
        """
        if not self._ready:
            return []
        try:
            resp = (
                self._client.table('legislative_dossiers')
                .select('*')
                .eq('source', 'manuel')
                .execute()
            )
            rows = resp.data or []
            log.info(f"Supabase: {len(rows)} dossier(s) manuel(s) chargé(s)")
            return rows
        except Exception as e:
            log.warning(f"Supabase load_manuel_dossiers: {e}")
            return []

    def get_dossier_statut(self, url_an: str) -> Optional[str]:
        """
        Return user-defined statut ('critique' | 'a_surveiller' | 'pour_info')
        for a given dossier, or None if not tracked.
        """
        if not self._ready or not url_an:
            return None
        try:
            resp = (
                self._client.table('legislative_dossiers')
                .select('id,statut')
                .eq('url_an', url_an)
                .limit(1)
                .execute()
            )
            if resp.data:
                return resp.data[0].get('statut')
        except Exception as e:
            log.warning(f"Supabase get_dossier_statut: {e}")
        return None

    # ── Write ─────────────────────────────────────────────────────────

    def upsert_dossier(self, fiche: dict) -> Optional[str]:
        """
        Insert or update a dossier in Supabase.
        Returns the Supabase UUID, or None on failure.
        Idempotent: uses url_an as the unique business key.
        """
        if not self._ready:
            return None

        url_an = fiche.get('url_an', '')
        if not url_an:
            return None

        payload = {
            'titre':       fiche.get('titre', ''),
            'url_an':      url_an,
            'url_dossier': fiche.get('url_dossier', ''),
            'stade':       fiche.get('stade', ''),
            'stade_index': fiche.get('stade_index', 0),
            'resume_gsf':  fiche.get('resume_gsf', ''),
            'pourquoi':    fiche.get('pourquoi', ''),
            'score':       int(fiche.get('score') or 1),
            'date_depot':  fiche.get('date_depot') or None,
            'source':      'manuel' if fiche.get('manuel') else 'pipeline',
            'horizon':     fiche.get('horizon', ''),
        }

        try:
            # Check existence
            existing = (
                self._client.table('legislative_dossiers')
                .select('id')
                .eq('url_an', url_an)
                .limit(1)
                .execute()
            )

            if existing.data:
                supabase_id = existing.data[0]['id']
                self._client.table('legislative_dossiers').update(payload).eq('id', supabase_id).execute()
            else:
                resp = self._client.table('legislative_dossiers').insert(payload).execute()
                supabase_id = resp.data[0]['id'] if resp.data else None

            # Cache supabase_id back on the fiche dict
            if supabase_id:
                fiche['supabase_id'] = supabase_id

            return supabase_id

        except Exception as e:
            log.warning(f"Supabase upsert_dossier '{fiche.get('titre','')[:40]}': {e}")
            return None

    def record_stage_change(self, fiche: dict, ancien_stade: str, nouveau_stade: str) -> bool:
        """
        Log a stage advancement event in dossier_events.
        Requires fiche to have 'supabase_id' (call upsert_dossier first).
        """
        if not self._ready:
            return False
        supabase_id = fiche.get('supabase_id')
        if not supabase_id:
            return False
        try:
            self._client.table('dossier_events').insert({
                'dossier_id':    supabase_id,
                'event_type':    'stage_change',
                'ancien_stade':  ancien_stade,
                'nouveau_stade': nouveau_stade,
                'note':          'Détecté automatiquement par le pipeline',
            }).execute()
            return True
        except Exception as e:
            log.warning(f"Supabase record_stage_change: {e}")
            return False

    def record_creation(self, fiche: dict) -> bool:
        """Log a 'created' event when a new PJL is first discovered."""
        if not self._ready:
            return False
        supabase_id = fiche.get('supabase_id')
        if not supabase_id:
            return False
        try:
            self._client.table('dossier_events').insert({
                'dossier_id': supabase_id,
                'event_type': 'created',
                'note':       f"Découvert automatiquement (score {fiche.get('score', 1)}/3)",
            }).execute()
            return True
        except Exception as e:
            log.warning(f"Supabase record_creation: {e}")
            return False
