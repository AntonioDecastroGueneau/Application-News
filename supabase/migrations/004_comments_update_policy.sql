-- Allow anon to update comments (for edit feature)
CREATE POLICY "anon_update" ON dossier_comments FOR UPDATE USING (true) WITH CHECK (true);

-- Allow anon to select comments (explicit)
CREATE POLICY "anon_select" ON dossier_comments FOR SELECT USING (true);
