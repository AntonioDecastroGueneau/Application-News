"""
Static configuration for the GSF pipeline.

Dynamic values like paths and "today" are handled in `gsf_pipeline.main`.
"""

# ─────────────────────────────────────────────
# External URLs
# ─────────────────────────────────────────────

JORF_BASE_URL = 'https://echanges.dila.gouv.fr/OPENDATA/JORF/'
VIGIEAU_DEPTS_URL = 'https://api.vigieau.gouv.fr/api/departements'


# ─────────────────────────────────────────────
# LLM — providers and models
# ─────────────────────────────────────────────

MISTRAL_MODEL = 'mistral-small-latest'
MISTRAL_MIN_INTERVAL = 1.2  # 1 req/s max -> margin

GROQ_MODEL_FILTER = 'llama-3.1-8b-instant'
GROQ_MODEL_ENRICH = 'llama-3.3-70b-versatile'
GROQ_MODEL = GROQ_MODEL_FILTER  # alias compat

GROQ_MAX_RETRY = 3
GROQ_RETRY_WAIT = 62
GROQ_MIN_INTERVAL = 10.0  # seconds between Groq calls


# ─────────────────────────────────────────────
# Timeouts
# ─────────────────────────────────────────────

TIMEOUT = 30


# ─────────────────────────────────────────────
# GSF context injected into prompts
# ─────────────────────────────────────────────

GSF_CONTEXT = """
GSF est un prestataire multiservices (propreté, maintenance, FM) qui intervient sur les
sites de ses clients — il n'est PAS exploitant ICPE.

Enjeux environnement / climat / carbone :
- Flotte de véhicules (scope 1 significatif) : électrification en cours
- Consommation énergétique locaux et prestations (eau, électricité, chauffage)
- Déchets produits lors des prestations (tri, traçabilité, éco-organismes)
- Obligation BGES et plan climat / trajectoire de réduction des émissions
- CSRD / reporting extra-financier (grande entreprise de services)
- Certifications ISO 14001, démarches RSE et bas-carbone

Risques indirects et chaîne de valeur :
- 3 000+ sites clients exposés aux aléas climatiques (inondations, sécheresse, canicules)
  → impacts sur volumes de prestations, contrats, résilience opérationnelle
- Exigences carbone croissantes des clients (nucléaire, santé, agroalimentaire, industrie)
  sur empreinte, réduction déchets, éco-efficacité des prestations
""".strip()

GSF_CONTEXT_SHORT = (
    "GSF : prestataire propreté/FM, intervient chez ses clients (non exploitant ICPE). "
    "Enjeux : BGES/plan climat, flotte véhicules (scope 1), énergie, déchets prestations, "
    "CSRD/reporting, ISO 14001. "
    "Risques indirects : aléas climatiques sur 3 000+ sites clients, "
    "exigences carbone des clients (nucléaire, santé, industrie)."
)


# ─────────────────────────────────────────────
# Keywords — first-pass relevance filter
# ─────────────────────────────────────────────

KEYWORDS = [
    # Climat & décarbonation
    'climat', 'climatique', 'réchauffement', 'décarbonation', 'carbone',
    'GES', 'gaz à effet de serre', 'neutralité carbone', 'net zéro',
    'SNBC', 'PNACC', 'adaptation', 'atténuation', 'trajectoire',
    'transition énergétique', 'transition écologique',
    'Accord de Paris', 'COP', 'GIEC', 'IPCC',
    'décarboner', 'bas-carbone', 'scope 1', 'scope 2', 'scope 3',
    'bilan carbone', 'empreinte carbone', 'plan climat',
    'loi énergie', 'loi climat', 'France 2030',

    # Reporting & obligations ESG
    'CSRD', 'reporting extra-financier', 'taxonomie verte',
    'reporting environnemental', 'reporting durabilité',
    'bilan GES', 'BGES',

    # Énergie
    'énergie', 'efficacité énergétique', 'renouvelable', 'DPE',
    'bâtiment tertiaire', 'rénovation énergétique',

    # Déchets (angle prestataire de services)
    'déchet', 'déchets', 'REP', 'responsabilité élargie',
    'tri', 'recyclage', 'éco-organisme', 'traçabilité déchets',

    # Risques climatiques physiques
    'inondation', 'sécheresse', 'canicule', 'événement extrême',
    'submersion', 'feu de forêt', 'catastrophe naturelle', 'PPR',

    # Mobilité & flotte
    'véhicule électrique', 'électrification', 'flotte',
    'mobilité durable', 'ZFE',
]


# ─────────────────────────────────────────────
# RSS sources
# ─────────────────────────────────────────────

RSS_SOURCES = [
    {
        'name': 'Actu-Environnement',
        'url': 'https://www.actu-environnement.com/flux/rss/environnement/',
        'categorie': 'Presse',
        'fallback_crawl': 'https://www.actu-environnement.com/ae/news/',
    },
    {
        'name': 'Reporterre',
        'url': 'https://reporterre.net/spip.php?page=backend',
        'categorie': 'Presse',
        'fallback_crawl': 'https://reporterre.net/',
    },
    {
        'name': 'Novethic',
        'url': 'https://www.novethic.fr/feed',
        'categorie': 'Presse',
        'fallback_crawl': 'https://www.novethic.fr/',
    },
    {
        'name': 'Min. Transition Écologique',
        'url': 'https://www.ecologie.gouv.fr/rss-actualites.xml',
        'categorie': 'Réglementation',
        'fallback_crawl': 'https://www.ecologie.gouv.fr/actualites',
    },
    {
        'name': 'ADEME',
        'url': 'https://www.ademe.fr/feed/',
        'categorie': 'Climat',
        'fallback_crawl': 'https://www.ademe.fr/actualites/',
    },
    {
        'name': 'Haut Conseil pour le Climat',
        'url': 'https://www.hautconseilclimat.fr/feed/',
        'categorie': 'Climat',
        'fallback_crawl': 'https://www.hautconseilclimat.fr/actualites/',
    },
    {
        'name': 'France Stratégie',
        'url': 'https://www.strategie.gouv.fr/rss.xml',
        'categorie': 'Climat',
        'fallback_crawl': 'https://www.strategie.gouv.fr/publications',
        'require_keywords': [
            'climat', 'carbone', 'transition', 'énergie', 'décarbonation',
            'environnement', 'adaptation', 'neutralité', 'SNBC', 'empreinte',
            'biodiversité', 'trajectoire', 'bas-carbone', 'GES',
        ],
    },
    {
        'name': 'Vie-publique.fr',
        'url': 'https://www.vie-publique.fr/rss/actualites.xml',
        'categorie': 'Réglementation',
        'fallback_crawl': 'https://www.vie-publique.fr/loi',
        'require_keywords': [
            'climat', 'énergie', 'transition', 'environnement', 'carbone',
            'décarbonation', 'renouvelable', 'biodiversité', 'CSRD',
            'adaptation', 'neutralité', 'trajectoire', 'émissions',
        ],
    },
    {
        'name': 'The Shift Project',
        'url': 'https://theshiftproject.org/feed/',
        'categorie': 'Climat',
        'fallback_crawl': 'https://theshiftproject.org/articles/',
    },
    {
        'name': 'I4CE',
        'url': 'https://www.i4ce.org/feed/',
        'categorie': 'Climat',
        'fallback_crawl': 'https://www.i4ce.org/publications/',
        'require_keywords': [
            'climat', 'carbone', 'transition', 'financement', 'investissement',
            'décarbonation', 'politique climatique', 'trajectoire', 'adaptation',
        ],
    },
    {
        'name': 'Carbone 4',
        'url': 'https://www.carbone4.com/feed',
        'categorie': 'Climat',
        'fallback_crawl': 'https://www.carbone4.com/publications',
    },
    {
        'name': 'Politico Energy EU',
        'url': 'https://www.politico.eu/section/energy-fr/feed/',
        'categorie': 'Réglementation',
        'fallback_crawl': 'https://www.politico.eu/section/energy-fr/',
        'require_keywords': [
            'climat', 'énergie', 'carbone', 'taxonomie', 'CSRD', 'Green Deal',
            'transition', 'renouvelable', 'émissions', 'règlement', 'directive',
        ],
    },
    {
        'name': 'Contexte Environnement',
        'url': 'https://www.contexte.com/articles/rss/edition/environnement',
        'categorie': 'Réglementation',
        'fallback_crawl': 'https://www.contexte.com/environnement/',
    },
]


# ─────────────────────────────────────────────
# VigiEau
# ─────────────────────────────────────────────

NIVEAUX_ORDRE = {
    'vigilance': 1,
    'alerte': 2,
    'alerte renforcée': 3,
    'crise': 4,
}

DATAGOUV_DATASET_ID = 'donnee-secheresse-vigieau'
DATAGOUV_API = 'https://www.data.gouv.fr/api/1/datasets/'

NIVEAUX_GRAVITE = ['vigilance', 'alerte', 'alerte renforcée', 'crise']

VIGIEAU_HISTORY_YEARS = None  # computed in vigieau module using current year

VIGIEAU_FALLBACK_URLS = {
    2026: 'https://www.data.gouv.fr/api/1/datasets/r/0732e970-c12c-4e6a-adca-5ac9dbc3fdfa',
}

DEPT_NOMS = {
    '01': 'Ain', '02': 'Aisne', '03': 'Allier', '04': 'Alpes-de-Haute-Provence', '05': 'Hautes-Alpes',
    '06': 'Alpes-Maritimes', '07': 'Ardèche', '08': 'Ardennes', '09': 'Ariège', '10': 'Aube',
    '11': 'Aude', '12': 'Aveyron', '13': 'Bouches-du-Rhône', '14': 'Calvados', '15': 'Cantal',
    '16': 'Charente', '17': 'Charente-Maritime', '18': 'Cher', '19': 'Corrèze', '2A': 'Corse-du-Sud',
    '2B': 'Haute-Corse', '21': "Côte-d'Or", '22': "Côtes-d'Armor", '23': 'Creuse', '24': 'Dordogne',
    '25': 'Doubs', '26': 'Drôme', '27': 'Eure', '28': 'Eure-et-Loir', '29': 'Finistère',
    '30': 'Gard', '31': 'Haute-Garonne', '32': 'Gers', '33': 'Gironde', '34': 'Hérault',
    '35': 'Ille-et-Vilaine', '36': 'Indre', '37': 'Indre-et-Loire', '38': 'Isère', '39': 'Jura',
    '40': 'Landes', '41': 'Loir-et-Cher', '42': 'Loire', '43': 'Haute-Loire', '44': 'Loire-Atlantique',
    '45': 'Loiret', '46': 'Lot', '47': 'Lot-et-Garonne', '48': 'Lozère', '49': 'Maine-et-Loire',
    '50': 'Manche', '51': 'Marne', '52': 'Haute-Marne', '53': 'Mayenne', '54': 'Meurthe-et-Moselle',
    '55': 'Meuse', '56': 'Morbihan', '57': 'Moselle', '58': 'Nièvre', '59': 'Nord',
    '60': 'Oise', '61': 'Orne', '62': 'Pas-de-Calais', '63': 'Puy-de-Dôme', '64': 'Pyrénées-Atlantiques',
    '65': 'Hautes-Pyrénées', '66': 'Pyrénées-Orientales', '67': 'Bas-Rhin', '68': 'Haut-Rhin', '69': 'Rhône',
    '70': 'Haute-Saône', '71': 'Saône-et-Loire', '72': 'Sarthe', '73': 'Savoie', '74': 'Haute-Savoie',
    '75': 'Paris', '76': 'Seine-Maritime', '77': 'Seine-et-Marne', '78': 'Yvelines', '79': 'Deux-Sèvres',
    '80': 'Somme', '81': 'Tarn', '82': 'Tarn-et-Garonne', '83': 'Var', '84': 'Vaucluse',
    '85': 'Vendée', '86': 'Vienne', '87': 'Haute-Vienne', '88': 'Vosges', '89': 'Yonne',
    '90': 'Territoire de Belfort', '91': 'Essonne', '92': 'Hauts-de-Seine', '93': 'Seine-Saint-Denis',
    '94': 'Val-de-Marne', '95': "Val-d'Oise", '971': 'Guadeloupe', '972': 'Martinique',
    '973': 'Guyane', '974': 'La Réunion', '976': 'Mayotte',
}


# ─────────────────────────────────────────────
# Parlement (AN)
# ─────────────────────────────────────────────

PARLEMENT_MAX_GROQ = 10

STADES_ORDRE = [
    'Dépôt',
    'Commission',
    'Séance publique AN',
    'Sénat 1ère lecture',
    'Commission mixte paritaire',
    'Sénat 2ème lecture',
    'AN 2ème lecture',
    'Adopté',
    'Promulgué',
]

AN_BASE = 'https://www2.assemblee-nationale.fr'
AN_DOSSIERS_BASE = 'https://www.assemblee-nationale.fr/dyn/17/dossiers'

AN_SCRAPER_SOURCES = [
    {
        'name': 'AN Projets de loi',
        'url': (f'{AN_BASE}/documents/liste'
                '?limit=30&type=projets-loi&legis=17&type_tri=DATE_MISE_LIGNE'),
    },
    {
        'name': 'AN Textes adoptés',
        'url': (f'{AN_BASE}/documents/liste'
                '?limit=30&type=ta&legis=17&type_tri=DATE_MISE_LIGNE'),
    },
]

STADE_PATTERNS = [
    ('Promulgué', ['promulgu', 'loi n°', 'parue au jo']),
    ('Adopté', ['adopté définitivement', 'adoption définitive', 'texte adopté']),
    ('Commission mixte paritaire', ['commission mixte paritaire', 'cmp']),
    ('AN 2ème lecture', ['2e lecture', 'deuxième lecture', 'nouvelle lecture']),
    ('Sénat 1ère lecture', ['sénat', 'haute assemblée']),
    ('Séance publique AN', ['séance publique', 'hémicycle', 'discussion générale']),
    ('Commission', [
        'en commission',
        'examiné par la commission',
        'rapporteur désigné',
        'renvoyé en commission',
    ]),
]

