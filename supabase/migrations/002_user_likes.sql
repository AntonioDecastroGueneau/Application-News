-- ═══════════════════════════════════════════════════════════════
-- Veille GSF — Favoris persistants
-- À exécuter dans le SQL Editor du dashboard Supabase
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS user_likes (
  article_id   TEXT        PRIMARY KEY,
  article_data JSONB       NOT NULL,
  liked_at     TIMESTAMPTZ DEFAULT NOW()
);

-- RLS : accès public (clé anon)
ALTER TABLE user_likes ENABLE ROW LEVEL SECURITY;

CREATE POLICY "anon_select" ON user_likes FOR SELECT USING (true);
CREATE POLICY "anon_insert" ON user_likes FOR INSERT WITH CHECK (true);
CREATE POLICY "anon_update" ON user_likes FOR UPDATE USING (true) WITH CHECK (true);
CREATE POLICY "anon_delete" ON user_likes FOR DELETE USING (true);
