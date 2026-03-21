-- Remove anonymous DELETE on legislative_dossiers (too permissive)
-- Only pipeline (service_role) should delete dossiers
DROP POLICY IF EXISTS "anon_delete" ON legislative_dossiers;
DROP POLICY IF EXISTS "anon can delete" ON legislative_dossiers;

-- Add session_token column to dossier_comments for ownership
ALTER TABLE dossier_comments ADD COLUMN IF NOT EXISTS session_token TEXT;

-- Replace permissive update policy with ownership-based one
DROP POLICY IF EXISTS "anon_update" ON dossier_comments;
CREATE POLICY "owner_update" ON dossier_comments
  FOR UPDATE USING (
    session_token IS NULL OR
    session_token = current_setting('request.jwt.claims', true)::json->>'session_token'
  ) WITH CHECK (true);
