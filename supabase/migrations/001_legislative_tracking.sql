-- ═══════════════════════════════════════════════════════════════
-- Veille ABC — Suivi Législatif
-- À exécuter dans le SQL Editor du dashboard Supabase
-- ═══════════════════════════════════════════════════════════════

-- ─── 1. Dossiers législatifs suivis ─────────────────────────────
CREATE TABLE IF NOT EXISTS legislative_dossiers (
  id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  titre         TEXT        NOT NULL,
  url_an        TEXT,
  url_dossier   TEXT,
  stade         TEXT,
  stade_index   INTEGER     DEFAULT 0,
  -- Statut défini par l'utilisateur
  statut        TEXT        CHECK (statut IN ('critique', 'a_surveiller', 'pour_info')) DEFAULT 'a_surveiller',
  resume_ABC    TEXT,
  pourquoi      TEXT,
  score         INTEGER     CHECK (score BETWEEN 1 AND 3) DEFAULT 2,
  date_depot    DATE,
  -- Source : 'pipeline' (auto-détecté) ou 'manuel' (ajouté depuis l'UI)
  source        TEXT        DEFAULT 'manuel',
  -- Horizon estimé d'entrée en vigueur
  horizon       TEXT,
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  updated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- ─── 2. Commentaires par dossier ────────────────────────────────
CREATE TABLE IF NOT EXISTS dossier_comments (
  id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  dossier_id    UUID        NOT NULL REFERENCES legislative_dossiers(id) ON DELETE CASCADE,
  content       TEXT        NOT NULL,
  auteur        TEXT        DEFAULT 'ABC',
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- ─── 3. Historique des évènements (changements de stade auto) ───
CREATE TABLE IF NOT EXISTS dossier_events (
  id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  dossier_id    UUID        NOT NULL REFERENCES legislative_dossiers(id) ON DELETE CASCADE,
  event_type    TEXT        NOT NULL,   -- 'stage_change' | 'created' | 'status_change'
  ancien_stade  TEXT,
  nouveau_stade TEXT,
  note          TEXT,
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- ─── Index pour les requêtes fréquentes ─────────────────────────
CREATE INDEX IF NOT EXISTS idx_dossier_comments_dossier_id ON dossier_comments(dossier_id);
CREATE INDEX IF NOT EXISTS idx_dossier_events_dossier_id   ON dossier_events(dossier_id);
CREATE INDEX IF NOT EXISTS idx_legislative_dossiers_statut  ON legislative_dossiers(statut);
CREATE INDEX IF NOT EXISTS idx_legislative_dossiers_stade   ON legislative_dossiers(stade);

-- ─── Trigger : updated_at automatique ───────────────────────────
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_legislative_dossiers_updated_at
  BEFORE UPDATE ON legislative_dossiers
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ─── RLS : accès public (clé anon) ──────────────────────────────
-- L'application est un outil interne ABC, pas d'auth utilisateur.
-- La clé anon suffit avec des policies permissives.

ALTER TABLE legislative_dossiers ENABLE ROW LEVEL SECURITY;
ALTER TABLE dossier_comments      ENABLE ROW LEVEL SECURITY;
ALTER TABLE dossier_events        ENABLE ROW LEVEL SECURITY;

-- legislative_dossiers : lecture + écriture pour anon
CREATE POLICY "anon_select"  ON legislative_dossiers FOR SELECT USING (true);
CREATE POLICY "anon_insert"  ON legislative_dossiers FOR INSERT WITH CHECK (true);
CREATE POLICY "anon_update"  ON legislative_dossiers FOR UPDATE USING (true) WITH CHECK (true);
CREATE POLICY "anon_delete"  ON legislative_dossiers FOR DELETE USING (true);

-- dossier_comments : lecture + écriture pour anon
CREATE POLICY "anon_select"  ON dossier_comments FOR SELECT USING (true);
CREATE POLICY "anon_insert"  ON dossier_comments FOR INSERT WITH CHECK (true);
CREATE POLICY "anon_delete"  ON dossier_comments FOR DELETE USING (true);

-- dossier_events : lecture + écriture pour anon
CREATE POLICY "anon_select"  ON dossier_events FOR SELECT USING (true);
CREATE POLICY "anon_insert"  ON dossier_events FOR INSERT WITH CHECK (true);
