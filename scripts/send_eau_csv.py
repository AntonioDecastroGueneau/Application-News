"""
Export restrictions_eau from Supabase as CSV and send via Resend.
Runs as a post-pipeline step in GitHub Actions.
"""
import csv
import io
import logging
import os
import sys
from datetime import datetime, timezone

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

SUPABASE_URL = os.environ.get('SUPABASE_URL', '').strip()
SUPABASE_KEY = os.environ.get('SUPABASE_ANON_KEY', '').strip()
RESEND_API   = os.environ.get('CSV_API', '').strip()
MAIL_TO      = os.environ.get('CSV_MAIL_TO', '').strip()

COLS = ['code_zone', 'type_eau', 'nom_zone', 'departement',
        'niveau_actuel', 'date_maj', 'url_arrete', 'est_nouveau']

NIVEAU_LABEL = {
    'vigilance':        'Vigilance',
    'alerte':           'Alerte',
    'alerte renforcée': 'Alerte renforcée',
    'crise':            'Crise',
}


def fetch_zones() -> list:
    from supabase import create_client
    client = create_client(SUPABASE_URL, SUPABASE_KEY)
    resp = (
        client.table('restrictions_eau')
        .select(','.join(COLS))
        .order('departement')
        .order('niveau_actuel')
        .execute()
    )
    return resp.data or []


def build_csv(rows: list) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=COLS, delimiter=';', extrasaction='ignore')
    writer.writeheader()
    writer.writerows(rows)
    # UTF-8 BOM for Excel compatibility
    return ('\ufeff' + buf.getvalue()).encode('utf-8')


def build_html_summary(rows: list) -> str:
    from collections import Counter
    counts = Counter(r.get('niveau_actuel', '') for r in rows)
    ordre = ['crise', 'alerte renforcée', 'alerte', 'vigilance']
    lines = ''.join(
        f"<tr><td style='padding:4px 12px 4px 0'>{NIVEAU_LABEL.get(n, n)}</td>"
        f"<td style='padding:4px 0;font-weight:600'>{counts[n]}</td></tr>"
        for n in ordre if counts.get(n)
    )
    date_str = datetime.now(timezone.utc).strftime('%d/%m/%Y')
    return f"""
    <p>Bonjour,</p>
    <p>Voici l'export quotidien des restrictions d'eau en vigueur au <strong>{date_str}</strong>
    ({len(rows)} zones actives).</p>
    <table style='border-collapse:collapse;font-family:sans-serif;font-size:14px'>
      <tr><th style='text-align:left;padding:4px 12px 4px 0;color:#666'>Niveau</th>
          <th style='text-align:left;padding:4px 0;color:#666'>Zones</th></tr>
      {lines}
    </table>
    <p>Le détail complet est en pièce jointe (CSV, compatible Excel).</p>
    <p style='color:#999;font-size:12px'>— ABC Veille Environnementale</p>
    """


def main():
    if not all([SUPABASE_URL, SUPABASE_KEY, RESEND_API, MAIL_TO]):
        log.warning("send_eau_csv: variables manquantes — skip")
        sys.exit(0)

    try:
        rows = fetch_zones()
    except Exception as e:
        log.warning(f"send_eau_csv: fetch Supabase échoué — {e}")
        sys.exit(0)

    if not rows:
        log.info("send_eau_csv: table vide — pas d'envoi")
        sys.exit(0)

    csv_bytes = build_csv(rows)
    html = build_html_summary(rows)
    date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    filename = f"restrictions_eau_{date_str}.csv"

    import resend
    resend.api_key = RESEND_API

    import base64
    r = resend.Emails.send({
        "from":    "onboarding@resend.dev",
        "to":      MAIL_TO,
        "subject": f"[ABC Veille] Restrictions eau — {date_str}",
        "html":    html,
        "attachments": [{
            "filename": filename,
            "content":  list(csv_bytes),
        }],
    })
    log.info(f"send_eau_csv: mail envoyé → {MAIL_TO} (id: {r.get('id', '?')})")


if __name__ == '__main__':
    main()
