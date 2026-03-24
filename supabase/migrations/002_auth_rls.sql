-- ═══════════════════════════════════════════════════════════════
-- Veille ABC — RLS sécurisées avec Supabase Auth
-- Remplace les policies permissives de la migration 001
-- À exécuter APRÈS 001_legislative_tracking.sql
-- ═══════════════════════════════════════════════════════════════

-- ─── 1. Supprimer les anciennes policies permissives (écritures) ─
DROP POLICY IF EXISTS "anon_insert" ON legislative_dossiers;
DROP POLICY IF EXISTS "anon_update" ON legislative_dossiers;
DROP POLICY IF EXISTS "anon_delete" ON legislative_dossiers;

DROP POLICY IF EXISTS "anon_insert" ON dossier_comments;
DROP POLICY IF EXISTS "anon_delete" ON dossier_comments;

DROP POLICY IF EXISTS "anon_insert" ON dossier_events;

-- ─── 2. Nouvelles policies : lecture publique, écritures auth seules ─

-- legislative_dossiers
CREATE POLICY "auth_insert" ON legislative_dossiers
  FOR INSERT WITH CHECK (auth.role() = 'authenticated');

CREATE POLICY "auth_update" ON legislative_dossiers
  FOR UPDATE USING (auth.role() = 'authenticated') WITH CHECK (auth.role() = 'authenticated');

CREATE POLICY "auth_delete" ON legislative_dossiers
  FOR DELETE USING (auth.role() = 'authenticated');

-- dossier_comments
CREATE POLICY "auth_insert" ON dossier_comments
  FOR INSERT WITH CHECK (auth.role() = 'authenticated');

CREATE POLICY "auth_delete" ON dossier_comments
  FOR DELETE USING (auth.role() = 'authenticated');

-- dossier_events
CREATE POLICY "auth_insert" ON dossier_events
  FOR INSERT WITH CHECK (auth.role() = 'authenticated');

-- ─── 3. Note : le pipeline utilise la service_role key ───────────
-- La service_role key bypass les RLS → le pipeline peut toujours écrire
-- sans être authentifié. Ne jamais exposer cette clé côté client.
