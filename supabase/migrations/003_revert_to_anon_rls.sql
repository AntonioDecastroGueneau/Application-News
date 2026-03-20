-- ═══════════════════════════════════════════════════════════════
-- Veille GSF — Retour aux RLS permissives (anon key)
-- Annule la migration 002 — outil interne GSF, pas d'auth requise
-- ═══════════════════════════════════════════════════════════════

-- ─── Supprimer les policies auth de la migration 002 ────────────
DROP POLICY IF EXISTS "auth_insert" ON legislative_dossiers;
DROP POLICY IF EXISTS "auth_update" ON legislative_dossiers;
DROP POLICY IF EXISTS "auth_delete" ON legislative_dossiers;

DROP POLICY IF EXISTS "auth_insert" ON dossier_comments;
DROP POLICY IF EXISTS "auth_delete" ON dossier_comments;

DROP POLICY IF EXISTS "auth_insert" ON dossier_events;

-- ─── Recréer les policies permissives (anon peut tout faire) ────

-- legislative_dossiers
CREATE POLICY "anon_insert" ON legislative_dossiers FOR INSERT WITH CHECK (true);
CREATE POLICY "anon_update" ON legislative_dossiers FOR UPDATE USING (true) WITH CHECK (true);
CREATE POLICY "anon_delete" ON legislative_dossiers FOR DELETE USING (true);

-- dossier_comments
CREATE POLICY "anon_insert" ON dossier_comments FOR INSERT WITH CHECK (true);
CREATE POLICY "anon_delete" ON dossier_comments FOR DELETE USING (true);

-- dossier_events
CREATE POLICY "anon_insert" ON dossier_events FOR INSERT WITH CHECK (true);
