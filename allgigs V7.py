import os
import pandas as pd
from datetime import datetime
import hashlib
from supabase import create_client, Client
from dotenv import load_dotenv
import logging
from pathlib import Path
import time
import re

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('allgigs.log')
    ]
)

# Suppress HTTP and verbose logs
import logging as _logging
_logging.getLogger("httpx").setLevel(_logging.WARNING)
_logging.getLogger("requests").setLevel(_logging.WARNING)
_logging.getLogger("supabase_py").setLevel(_logging.WARNING)

# Load environment variables
load_dotenv('dotenv')

# Supabase configuration
SUPABASE_URL = "https://lfwgzoltxrfutexrjahr.supabase.co"
SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
if not SUPABASE_SERVICE_ROLE_KEY:
    raise ValueError("SUPABASE_SERVICE_ROLE_KEY environment variable is not set")

# Create Supabase client with service role key to bypass RLS
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# Constants
BATCH_SIZE = 500  # Number of records to upload in each batch
NEW_TABLE = "Allgigs_All_vacancies_NEW"
HISTORICAL_TABLE = "Allgigs_All_vacancies"

# Directory structure
BASE_DIR = Path('/Users/jaapjanlammers/Desktop/Freelancedirectory')
FREELANCE_DIR = BASE_DIR / 'Freelance Directory'
IMPORTANT_DIR = BASE_DIR / 'Important_allGigs'

# ==================================================
# REGIONAL CATEGORIZATION SYSTEM
# ==================================================
# Uses "Dutch by default" approach - assume Dutch unless clear evidence otherwise

def categorize_location(location: str, rate: str = None, company: str = None, source: str = None, title: str = None, summary: str = None) -> dict:
    """
    Categorize a location into Dutch, EU, and Rest of World categories.
    Enhanced with remote/hybrid detection and contextual analysis.

    Args:
        location (str): The location string to categorize
        rate (str, optional): The rate/salary string to check for currency indicators
        company (str, optional): Company name for contextual analysis
        source (str, optional): Job source for regional bias detection
        title (str, optional): Job title for additional context
        summary (str, optional): Job description for remote/hybrid clues

    Returns:
        Dict[str, bool]: Dictionary with 'Dutch', 'EU', 'Rest_of_World' as keys
    """
    if pd.isna(location) or location == '':
        location_clean = ''
    else:
        location_clean = str(location).lower().strip()

    # Initialize remote/hybrid detection
    is_remote = False
    is_hybrid = False
    remote_region = None

    # 1. DETECT REMOTE/HYBRID PATTERNS FIRST
    remote_patterns = ['remote', 'remote work', 'work from home', 'wfh', 'telecommute',
                      'home office', 'work remotely', 'remote position', 'virtual']
    hybrid_patterns = ['hybrid', 'hybrid work', 'office + remote', 'mixed', 'flexible location']

    # Check for remote patterns
    for pattern in remote_patterns:
        if pattern in location_clean:
            is_remote = True
            break

    # Check for hybrid patterns
    for pattern in hybrid_patterns:
        if pattern in location_clean:
            is_hybrid = True
            break

    # Extract remote region specifications
    if is_remote:
        if 'remote (eu)' in location_clean or 'eu remote' in location_clean:
            remote_region = 'eu'
        elif 'remote (netherlands)' in location_clean or 'remote nl' in location_clean:
            remote_region = 'dutch'
        elif 'remote (germany)' in location_clean or 'germany remote' in location_clean:
            remote_region = 'eu_germany'

    # 2. ANALYZE CONTEXT FOR REMOTE JOBS WITHOUT REGION SPECIFICATION
    if is_remote and not remote_region:
        # Check company context for Dutch companies
        if company and not pd.isna(company):
            company_clean = str(company).lower().strip()
            dutch_companies = ['ing', 'rabobank', 'abn amro', 'philips', 'shell', 'unilever']
            if any(dutch_company in company_clean for dutch_company in dutch_companies):
                remote_region = 'dutch'

        # Check source context (Dutch job boards suggest Dutch remote)
        if not remote_region and source:
            dutch_sources = ['freelance.nl', 'interimnetwerk']
            if any(dutch_source in str(source).lower() for dutch_source in dutch_sources):
                remote_region = 'dutch'

    # 3. APPLY REMOTE REGIONAL LOGIC
    if is_remote and remote_region:
        if remote_region == 'dutch':
            return {'Dutch': True, 'EU': False, 'Rest_of_World': False}
        elif remote_region.startswith('eu'):
            return {'Dutch': False, 'EU': True, 'Rest_of_World': False}

    # For unspecified remote, use company context
    if is_remote and not remote_region:
        if company and not pd.isna(company):
            company_clean = str(company).lower().strip()
            dutch_companies = ['ing', 'rabobank', 'abn amro', 'philips', 'shell', 'unilever']
            if any(dutch_company in company_clean for dutch_company in dutch_companies):
                return {'Dutch': True, 'EU': False, 'Rest_of_World': False}

    # 4. REGULAR LOCATION ANALYSIS (for non-remote or hybrid office locations)
    eu_countries_excluding_nl = [
        'germany', 'france', 'italy', 'spain', 'poland', 'belgium', 'austria',
        'sweden', 'denmark', 'finland', 'portugal', 'greece', 'czech republic',
        'czechia', 'hungary', 'romania', 'bulgaria', 'croatia', 'slovakia',
        'slovenia', 'lithuania', 'latvia', 'estonia', 'ireland', 'luxembourg',
        'malta', 'cyprus'
    ]

    major_eu_cities = [
        'berlin', 'munich', 'hamburg', 'cologne', 'frankfurt', 'paris', 'marseille',
        'lyon', 'toulouse', 'nice', 'nantes', 'strasbourg', 'montpellier', 'bordeaux',
        'lille', 'rennes', 'reims', 'saint-étienne', 'toulon', 'grenoble', 'dijon',
        'angers', 'nîmes', 'villeurbanne', 'saint-denis', 'le havre', 'tours',
        'limoges', 'amiens', 'perpignan', 'metz', 'boulogne-billancourt', 'orléans',
        'mulhouse', 'rouen', 'caen', 'nancy', 'saint-pierre', 'argenteuil',
        'rome', 'milan', 'madrid', 'barcelona', 'warsaw', 'krakow',
        'brussels', 'vienna', 'stockholm', 'copenhagen', 'helsinki', 'lisbon'
    ]

    if location_clean:
        for country in eu_countries_excluding_nl:
            if country in location_clean:
                return {'Dutch': False, 'EU': True, 'Rest_of_World': False}

        for city in major_eu_cities:
            if city in location_clean:
                return {'Dutch': False, 'EU': True, 'Rest_of_World': False}

    # 5. Check for "European Union" mentions
    if location_clean and 'european union' in location_clean:
        return {'Dutch': False, 'EU': True, 'Rest_of_World': False}

    # 6. Check for non-EU countries
    rest_of_world_countries = [
        'united states', 'usa', 'america', 'canada', 'australia', 'new zealand',
        'india', 'china', 'japan', 'singapore', 'hong kong', 'south korea',
        'brazil', 'mexico', 'argentina', 'chile', 'south africa', 'israel',
        'turkey', 'russia', 'ukraine', 'belarus', 'switzerland', 'norway',
        'united kingdom', 'uk', 'britain', 'england', 'scotland', 'wales'
    ]

    if location_clean:
        for country in rest_of_world_countries:
            if country in location_clean:
                return {'Dutch': False, 'EU': False, 'Rest_of_World': True}

    # 7. Check for USD currency
    if rate and not pd.isna(rate):
        rate_str = str(rate).lower().strip()
        usd_indicators = ['$', 'usd', 'dollar', 'dollars', '$/hr', '$/hour', '$/day', '$/month', '$/week', '$/year']
        if any(indicator in rate_str for indicator in usd_indicators):
            return {'Dutch': False, 'EU': False, 'Rest_of_World': True}

    # 7.5. LANGUAGE DETECTION - Check for French language indicators
    def detect_french_language(text_fields):
        """Detect French language in text fields."""
        if not text_fields:
            return False
        
        # French language indicators
        french_indicators = [
            # Common French words
            'développeur', 'développeuse', 'ingénieur', 'consultant', 'consultante',
            'freelance', 'indépendant', 'indépendante', 'télétravail', 'travail',
            'entreprise', 'société', 'projet', 'mission', 'compétences', 'expérience',
            'années', 'ans', 'niveau', 'maîtrise', 'connaissance', 'technologies',
            'développement', 'conception', 'analyse', 'gestion', 'équipe', 'client',
            'cahier', 'charges', 'spécifications', 'fonctionnelles', 'techniques',
            'architecture', 'système', 'application', 'logiciel', 'programmation',
            'code', 'base', 'données', 'interface', 'utilisateur', 'web', 'mobile',
            'desktop', 'cloud', 'sécurité', 'performance', 'qualité', 'tests',
            'déploiement', 'maintenance', 'support', 'formation', 'documentation',
            # French articles and prepositions
            'le', 'la', 'les', 'un', 'une', 'des', 'du', 'de', 'dans', 'sur', 'avec',
            'pour', 'par', 'sans', 'sous', 'vers', 'chez', 'entre', 'pendant',
            # French job-related terms
            'poste', 'emploi', 'offre', 'recrutement', 'candidature', 'cv',
            'entretien', 'salaire', 'rémunération', 'avantages', 'congés',
            'télétravail', 'bureau', 'lieu', 'adresse', 'téléphone', 'email',
            'contact', 'référence', 'urgent', 'immédiat', 'début', 'fin',
            'durée', 'période', 'contrat', 'cdi', 'cdd', 'stage', 'alternance'
        ]
        
        # Combine all text fields
        combined_text = ' '.join([str(field).lower() for field in text_fields if field and not pd.isna(field)])
        
        # Count French indicators
        french_count = sum(1 for indicator in french_indicators if indicator in combined_text)
        
        # If we find 3 or more French indicators, consider it French
        return french_count >= 3
    
    # Check for French language in available text fields
    text_fields = [location, title, summary, company]
    if detect_french_language(text_fields):
        return {'Dutch': False, 'EU': True, 'Rest_of_World': False}

    # 8. Default to Dutch
    return {'Dutch': True, 'EU': False, 'Rest_of_World': False}

def detect_work_arrangement(location: str = None, title: str = None, summary: str = None,
                          company: str = None, source: str = None) -> str:
    """Detect Remote, Hybrid, Onsite, or Not Specified from multiple text sources."""
    text_sources = []
    if location and not pd.isna(location):
        text_sources.append(str(location))
    if title and not pd.isna(title):
        text_sources.append(str(title))
    if summary and not pd.isna(summary):
        text_sources.append(str(summary))
    if company and not pd.isna(company):
        text_sources.append(str(company))

    combined_text = ' '.join(text_sources).lower().strip()
    if not combined_text:
        return 'Not Specified'

    # Remote patterns
    remote_patterns = ['remote', 'remote work', 'work from home', 'wfh', 'telecommute',
                      'home office', 'work remotely', 'remote position', 'virtual']
    if any(pattern in combined_text for pattern in remote_patterns):
        # First check for explicit region mentions
        if 'remote (eu)' in combined_text or 'eu remote' in combined_text:
            return 'Remote (EU)'
        elif 'remote (netherlands)' in combined_text or 'remote nl' in combined_text:
            return 'Remote (Netherlands)'
        elif any(region in combined_text for region in ['remote (germany)', 'remote (france)', 'remote (uk)']):
            return 'Remote (Specified Country)'
        else:
            # For unspecified remote, determine region from location classification
            region_classification = categorize_location(location, None, company, source, title, summary)
            if region_classification['Dutch']:
                return 'Remote (Netherlands)'
            elif region_classification['EU']:
                return 'Remote (EU)'
            elif region_classification['Rest_of_World']:
                return 'Remote (Rest of World)'
            else:
                return 'Remote'

    # Hybrid patterns
    hybrid_patterns = ['hybrid', 'hybrid work', 'office + remote', 'remote + office',
                      'mixed work', 'flexible location', 'blended']
    if any(pattern in combined_text for pattern in hybrid_patterns):
        return 'Hybrid'

    # Onsite patterns
    onsite_patterns = ['onsite', 'on-site', 'office based', 'office-based', 'in office',
                      'at office', 'office location', 'physical office']
    if any(pattern in combined_text for pattern in onsite_patterns):
        return 'Onsite'

    return 'Not Specified'

def add_regional_columns(df: pd.DataFrame, location_column: str = 'Location') -> pd.DataFrame:
    """
    Add regional categorization and work arrangement columns to a DataFrame.
    Enhanced with remote/hybrid detection across multiple columns.

    Args:
        df (pd.DataFrame): The DataFrame to add columns to
        location_column (str): The name of the location column to analyze

    Returns:
        pd.DataFrame: DataFrame with new regional and work arrangement columns
    """
    if location_column not in df.columns:
        logging.warning(f"Column '{location_column}' not found in DataFrame")
        return df

    df_copy = df.copy()

    # Add WORK ARRANGEMENT column first
    work_arrangements = []
    for idx, row in df_copy.iterrows():
        arrangement = detect_work_arrangement(
            location=row.get(location_column),
            title=row.get('Title'),
            summary=row.get('Summary'),
            company=row.get('Company'),
            source=row.get('Source')
        )
        work_arrangements.append(arrangement)
    df_copy['Work_Arrangement'] = work_arrangements

    # Apply regional categorization with enhanced remote/hybrid logic
    def categorize_row(row):
        return categorize_location(
            location=row.get(location_column),
            rate=row.get('rate'),
            company=row.get('Company'),
            source=row.get('Source'),
            title=row.get('Title'),
            summary=row.get('Summary')
        )

    categorizations = df_copy.apply(categorize_row, axis=1)

    # Extract regional boolean values
    df_copy['Dutch'] = categorizations.apply(lambda x: x['Dutch'])
    df_copy['EU'] = categorizations.apply(lambda x: x['EU'])
    df_copy['Rest_of_World'] = categorizations.apply(lambda x: x['Rest_of_World'])

    return df_copy

def analyze_regional_distribution(df: pd.DataFrame) -> dict:
    """
    Analyze the distribution of jobs across regions.
    
    Args:
        df (pd.DataFrame): DataFrame with regional columns
        
    Returns:
        Dict[str, int]: Dictionary with counts for each region
    """
    if not all(col in df.columns for col in ['Dutch', 'EU', 'Rest_of_World']):
        logging.warning("Regional columns not found. Run add_regional_columns first.")
        return {}
    
    distribution = {
        'Dutch': df['Dutch'].sum(),
        'EU': df['EU'].sum(),
        'Rest_of_World': df['Rest_of_World'].sum(),
        'Total': len(df)
    }
    
    return distribution

def print_regional_summary(df: pd.DataFrame) -> None:
    """
    Print a summary of regional job distribution.
    
    Args:
        df (pd.DataFrame): DataFrame with regional columns
    """
    distribution = analyze_regional_distribution(df)
    
    if not distribution:
        return
    
    logging.info("=" * 50)
    logging.info("REGIONAL JOB DISTRIBUTION")
    logging.info("=" * 50)
    logging.info(f"Dutch jobs: {distribution['Dutch']} ({distribution['Dutch']/distribution['Total']*100:.1f}%)")
    logging.info(f"EU jobs: {distribution['EU']} ({distribution['EU']/distribution['Total']*100:.1f}%)")
    logging.info(f"Rest of World jobs: {distribution['Rest_of_World']} ({distribution['Rest_of_World']/distribution['Total']*100:.1f}%)")
    logging.info(f"Total jobs: {distribution['Total']}")
    logging.info("=" * 50)

# Company mappings dictionary
COMPANY_MAPPINGS = {
    # 1. TWINE
    'twine': {
        'Title': 'Title',
        'URL': 'Title_URL',
        'Company': '_17qbdj78',
        'Location': '_3cvhfnlp',
        'rate': '_3cvhfnlp1',
        'Summary': '_1cgbvbmx',
        'start': 'ASAP',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'twine'
    },
    # 2. LINKIT
    'LinkIT': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'Field3',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Field4',
        'Duration': 'Field5',  # Will be processed to check for "months" marker
        'Company': 'Field1',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'LinkIT'
    },
    # 3. FREELANCE.NL
    'freelance.nl': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'Field2',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Field2',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'freelance.nl'
    },
    # 4. YACHT
    'Yacht': {
        'Title': 'Field1',
        'Location': 'Field2',
        'Summary': 'Text1',
        'URL': 'URL',
        'start': 'Not mentioned',
        'rate': 'Field4',
        'Hours': 'Field3',
        'Duration': 'Text',
        'Company': 'Not mentioned',  # Probably mentioned in summary
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'Yacht'
    },
    # 5. FLEXTENDER
    'Flextender': {
        'Title': 'Field2',
        'Location': 'Field3',
        'Summary': 'See Vacancy',
        'URL': 'URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Field5',
        'Duration': 'Field4',
        'Company': 'Field1',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'Flextender'
    },
    # 6. KVK
    'KVK': {
        'Title': 'Field1',
        'Location': 'Amsterdam',
        'Summary': 'Text',
        'URL': 'Page_URL',
        'start': 'Not mentioned',
        'rate': 'Field3',
        'Hours': 'Not mentioned',
        'Duration': 'Field2',
        'Company': 'KVK',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'KVK'
    },
    # 7. CIRCLE8
    'Circle8': {
        'Title': 'Title',
        'Location': 'cvacancygridcard_usp',
        'Summary': 'Text',  # Will be processed to provide fallback if empty
        'URL': 'Title_URL',
        'start': 'Date',
        'rate': 'Not mentioned',
        'Hours': 'cvacancygridcard_usp2',
        'Duration': 'cvacancygridcard_usp1',
        'Company': 'Not mentioned',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'Circle8'
    },
    # 8. BEBEE
    'Bebee': {
        'Title': 'Field1_text',
        'Location': 'Info1',
        'Summary': 'Text',
        'URL': 'Field2_links',
        'start': 'ASAP',
        'rate': 'Info2',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'Bebee'
    },
    # 9. LINKEDIN
    'LinkedIn': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'About_the_job',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Field3',
        'Duration': 'Not mentioned',
        'Company': 'Company',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'LinkedIn'
    },
    # 10. LINKEDINZZP
    'LinkedInZZP': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'About_the_job',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Field3',
        'Duration': 'Not mentioned',
        'Company': 'Company',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'LinkedIn'
    },
    # 11. LINKEDININTERIM
    'LinkedInInterim': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'About_the_job',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Field3',
        'Duration': 'Not mentioned',
        'Company': 'Company',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'LinkedIn'
    },
    # 12. POLITIE
    'politie': {
        'Title': 'Field1',
        'Location': 'Text2',
        'Summary': 'Text5,Text6',  # Will be processed to merge Text5 and Text6
        'URL': 'URL',
        'start': 'Text',
        'rate': 'Not mentioned',
        'Hours': 'Text3',
        'Duration': 'Not mentioned',
        'Company': 'politie',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'politie'
    },
    # 13. GELDERLAND
    'gelderland': {
        'Title': 'Title',
        'Location': 'Gelderland',
        'Summary': 'Text1',
        'URL': 'URL',
        'start': 'vacancy_details1',
        'rate': 'Not mentioned',
        'Hours': 'Text',
        'Duration': 'vacancy_details3',
        'Company': 'Not mentioned',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'gelderland'
    },
    # 14. WERK.NL
    'werk.nl': {
        'Title': 'Title',
        'Location': 'Description1',
        'Summary': 'Text',
        'URL': 'Page_URL1',
        'start': 'ASAP',
        'rate': 'Text',  # Will search for currency/number patterns in Text column
        'Hours': 'Title3',
        'Duration': 'Not mentioned',
        'Company': 'Description',  # Will be processed to remove everything after "-"
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'werk.nl'
    },
    # 15. INDEED
    'indeed': {
        'Title': 'Title',
        'Location': 'Field2',
        'Summary': 'Field2',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'css18z4q2i',
        'Hours': 'Field3',
        'Duration': 'Not mentioned',
        'Company': 'css1h7lukg',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'indeed'
    },
    # 16. PLANET INTERIM
    'Planet Interim': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'Text',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Price',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'Planet Interim'
    },
    # 17. NS
    'NS': {
        'Title': 'Field1',
        'Location': 'Field2',
        'Summary': 'See Vacancy',
        'URL': 'https://www.werkenbijns.nl/vacatures?keywords=Inhuur',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Field3',
        'Duration': 'Not mentioned',
        'Company': 'NS',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'NS'
    },
    # 18. HOOFDKRAAN
    'hoofdkraan': {
        'Title': 'Title',
        'Location': 'colmd4',
        'Summary': 'Text',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'fontweightbold',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'hoofdkraan'
        },
    
    # 'Zoekklus': {
    #     'Title': 'Field',
    #     'URL': 'Field3_links',
    #     'Location': 'Field2',
    #     'Summary': 'Not mentioned',
    #     'rate': 'Not mentioned',
    #     'Hours': 'Field3',
    #     'Duration': 'Not mentioned',
    #     'Company': 'Not mentioned',
    #     'Source': 'Zoekklus'
    # },
    
    # 19. OVERHEID
    'Overheid': {
        'Title': 'Title',
        'Location': 'Field1',
        'Summary': 'Field2',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Content',
        'Hours': 'Content2',
        'Duration': 'Not mentioned',
        'Company': 'Description',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'Overheid'
    },
    # 20. RIJKSWATERSTAAT
    'rijkswaterstaat': {
        'Title': 'widgetheader',
        'Location': 'feature1',
        'Summary': 'Text1',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'feature2',
        'Hours': 'feature',
        'Duration': 'Not mentioned',
        'Company': 'Rijkswaterstaat',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'rijkswaterstaat'
    },
    # 21. ZOP OPDACHTEN
    'zzp opdrachten': {
        'Title': 'Title',
        'Location': 'jobdetails6',
        'Summary': '_Text',
        'URL': '_Link',
        'start': 'ASAP',
        'rate': 'jobdetails',
        'Hours': 'jobdetails4',
        'Duration': 'jobdetails4',
        'Company': 'Title2',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'zzp opdrachten'
    },
    # 22. HARVEY NASH
    'Harvey Nash': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'Text',
        'URL': 'Title_URL',
        'start': 'Not mentioned',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'Harvey Nash'
    },
    # 23. BEHANCE
    'Behance': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'Description',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Field3',
        'Duration': 'Not mentioned',
        'Company': 'Company',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'Behance'
    },
    # 24. SCHIPHOL
    'Schiphol': {
        'Title': 'Field2',
        'Location': 'Text2',
        'Summary': 'Text6',
        'URL': 'Field1_links',
        'start': 'ASAP',
        'rate': 'Text4',  # Will be processed to check Text2 and Text4
        'Hours': 'Text3',
        'Duration': 'Not mentioned',
        'Company': 'Schiphol',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'Schiphol'
    },
    # 25. JOOBLE
    'Jooble': {
        'Title': 'Keywords',
        'Location': 'caption',
        'Summary': 'geyos4,geyos41,geyos42,geyos43',
        'URL': 'Time_links',
        'start': 'ASAP',
        'rate': 'not mentioned',
        'Hours': 'Field3',
        'Duration': 'Not mentioned',
        'Company': 'z6wlhx',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'Jooble'
    },
    # 26. WERKZOEKEN.NL
    'werkzoeken.nl': {
        'Title': 'Title1',
        'Location': 'Company_name1',
        'Summary': 'Field3',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'requestedwrapper5',  # Will be processed to remove entries with "p/m"
        'Hours': 'requestedwrapper',
        'Duration': 'Not mentioned',
        'Company': 'Field1',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'werkzoeken.nl'
    },
    # 27. CODEUR.COM
    'Codeur.com': {
        'Title': 'nounderline',
        'URL': 'nounderline_URL',
        'Location': 'Not mentioned',
        'Summary': 'mt2',
        'start': 'ASAP',
        'rate': 'whitespacenowrap1',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Views': 'whitespacenowrap3',
        'Likes': '',
        'Keywords': 'Keywords',
        'Offers': 'Text2',
        'Source': 'Codeur.com'
    },
    # 28. UMC
    'UMC': {
        'Title': 'Text',
        'Location': 'Text2',
        'Summary': 'See Vacancy',
        'URL': 'Field2_links',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Text3',  # Will be processed to remove "p.w."
        'Duration': 'Not mentioned',
        'Company': 'Text1',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'UMC'
    },
    # 29. FLEXVALUE_B.V.
    'FlexValue_B.V.': {
        'Title': 'Title',
        'Location': 'scjnlklf',
        'Summary': 'Text',  # Will be processed to remove everything before "opdrachtbeschrijving"
        'URL': 'Title_URL',
        'start': 'Date1',
        'rate': 'Text',  # Will be processed to extract text between "Tarief" and "all-in"
        'Hours': 'Text',  # Will be processed to extract number after "Uren per week"
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'FlexValue_B.V.'
    },
    # 30. CENTRIC
    'Centric': {
        'Title': 'Field1',
        'Location': 'Field2',
        'Summary': 'Text',
        'URL': 'URL',
        'start': 'ASAP',
        'rate': 'Text',
        'Hours': 'Text',
        'Duration': 'Not mentioned',
        'Company': 'Centric',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'Centric'
    },
    # 31. FREELANCER.COM
    'freelancer.com': {
        'Title': 'Like',
        'Location': 'Remote',
        'Summary': 'Field4',
        'URL': 'Like_URL',
        'start': 'ASAP',
        'rate': 'Price',
        'Hours': 'Field3',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'freelancer.com'
    },
    # 32. FREELANCER.NL
    'Freelancer.nl': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'Text',
        'URL': 'btn_URL',
        'start': 'ASAP',
        'rate': 'budget',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'Freelancer.nl'
    },
    # 33. SALTA GROUP
    'Salta Group': {
        'Title': 'Keywords',
        'URL': 'Title_URL',
        'Location': 'Not mentioned',
        'Summary': 'Text',
        'Company': 'Company',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'start': 'ASAP',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'Salta Group'
    },
    # 34. PROLINKER.COM
    'ProLinker.com': {
        'Title': 'Field4_text',
        'Location': 'Field11',
        'Summary': 'Field4',
        'URL': 'Field4_links',
        'start': 'ASAP',
        'rate': 'Field15',
        'Hours': 'Field3',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'ProLinker.com'
    },
    # 35. FLEX WEST-BRABANT
    'Flex West-Brabant': {
        'Title': 'Field5',
        'Location': 'Not mentioned',
        'Summary': 'Text',
        'URL': 'Field4',
        'start': 'Not mentioned',
        'rate': 'Not mentioned',
        'Hours': 'Field3',
        'Duration': 'Not mentioned',
        'Company': 'org',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'Flex West-Brabant'
    },
    # 36. AMSTELVEENHUURTIN
    'Amstelveenhuurtin': {
        'Title': 'Field1',
        'Location': 'Text',  # Will be processed to extract text between "standplaats:" and "|"
        'Summary': 'See Vacancy',
        'URL': 'Page_URL',
        'start': 'Field11',  # Will be processed to extract everything before "t/m"
        'rate': 'Field8',
        'Hours': 'Text',  # Will be processed to extract text between "Uren:" and "|"
        'Duration': 'Not mentioned',
        'Company': 'Field2',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'Amstelveenhuurtin'
    },
    # 37. NOORDOOSTBRABANT
    'noordoostbrabant': {
        'Title': 'Field1',
        'Location': 'Text',
        'Summary': 'Text',
        'URL': 'Page_URL',
        'start': 'Text',
        'rate': 'Field2',
        'Hours': 'See Summary',
        'Duration': 'Text',
        'Company': 'Field3',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'noordoostbrabant'
    },
    # 38. FLEVOLAND
    'flevoland': {
        'Title': 'Field1',
        'Location': 'Field2',
        'Summary': 'Field7,Field8',  # Will be processed to merge Field7 and Field8
        'URL': 'Field9_links',
        'start': 'Field3',
        'rate': 'Not mentioned',
        'Hours': 'Field5',
        'Company': 'Field6',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'flevoland'
    },
    # 39. NOORD-HOLLAND
    'Noord-Holland': {
        'Title': 'Text',
        'Location': 'Field2',
        'Summary': 'Text2',
        'URL': 'Page_URL',
        'start': 'See Summary',
        'rate': 'Text2',  # Will be processed to extract text between "€" and "|"
        'Hours': 'See Summary',
        'Duration': 'See Summary',
        'Company': 'Text2',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'Noord-Holland'
    },
    # 40. GRONINGENHUURTIN
    'groningenhuurtin': {
        'Title': 'Text2',
        'Location': 'Text',  # Will be processed to extract text between "Standplaats:" and "|"
        'Summary': 'Text',
        'URL': 'Page_URL',
        'start': 'Text',
        'rate': 'Text',  # Will be processed to extract number after "€"
        'Hours': 'Text',  # Will be processed to extract number between "uren" and "|"
        'Duration': 'Text',
        'Company': 'Text1',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'groningenhuurtin'
    },
    # 41. TALENTENREGIO
    'TalentenRegio': {
        'Title': 'Field1',
        'Location': 'Not mentioned',
        'Summary': 'Text4,Text5',  # Will be processed to merge Text4 and Text5
        'URL': 'Page_URL',
        'start': 'Not mentioned',
        'rate': 'Not mentioned',
        'Hours': 'csscolumnflexer8',
        'Duration': 'Not mentioned',
        'Company': 'Field2',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'TalentenRegio'
    },
    # 42. HINTTECH
    'HintTech': {
        'Title': 'Title',
        'URL': 'Title_URL',
        'Company': 'vc_colmd6',
        'Hours': 'vc_colmd62',
        'rate': 'vc_colmd63',
        'Duration': 'vc_colmd64',
        'Location': 'vc_colmd61',
        'Summary': 'Text7',
        'start': 'ASAP',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'HintTech'
    },

    # 43. SELECT
    'Select': {
        'Title': 'Title',
        'Location': 'Field2', 
        'Summary': 'See Vacancy',
        'URL': 'Title_URL',
        'start': 'Date',
        'rate': 'Not mentioned',
        'Hours': 'hfp_cardiconsblockitem',
        'Duration': 'Not mentioned',
        'Company': 'Company',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'Select'
    },
    # 44. OVERHEIDZZP
    'overheidzzp': {
        'Title': 'Field7_text',
        'Location': 'Field3',
        'Summary': 'Field4',
        'URL': 'Field1_links',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Field4',
        'Duration': 'Field5',
        'Company': 'Field2',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'overheidzzp'
    },
    # 45. POOQ
    'POOQ': {
        'Title': 'Title',
        'Location': 'ml5',
        'Summary': 'Field1',
        'URL': 'Title_URL',
        'start': 'hidden',
        'rate': 'Field2',
        'Hours': 'Field3',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'POOQ'
    },
    # 46. GEMEENTE-PROJECTEN
    'gemeente-projecten': {
        'Title': 'Field1',  # Will be processed to remove text in () and () themselves
        'Location': 'Field2',  # Will be processed to remove text in () and () themselves
        'Summary': 'Text',
        'URL': 'URL',
        'start': 'ASAP',
        'rate': 'Field4',  # Will be processed to extract currency from Field4
        'Hours': 'Field3',
        'Duration': 'Field4',
        'Company': 'Not mentioned',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'gemeente-projecten'
    },
    # 47. GGDZWHUURTIN.NL
    'ggdzwhuurtin.nl': {
        'Title': 'Text',
        'Location': 'Field2',
        'Summary': 'Text11',
        'URL': 'Page_URL',
        'start': 'Text11',
        'rate': 'Text11',
        'Hours': 'Field3',
        'Duration': 'Text11',  # Will be processed to calculate date difference in months
        'Company': 'GGD Zaanstreek-Waterland',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'ggdzwhuurtin.nl'
    },
    # 48. FREEP
    'freep': {
        'Title': 'Title',
        'Location': 'flex3',
        'Summary': 'Field4',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'flex2',
        'Hours': 'flex4',
        'Duration': 'Not mentioned',
        'Company': 'mdcolspan2',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'freep'
    },
    # 49. ONLYHUMAN
    'onlyhuman': {
        'Title': 'Title',
        'Location': 'flex3',
        'Summary': 'See Vacancy',
        'URL': 'URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Field3',
        'Duration': 'Not mentioned',
        'Company': 'vacancycard',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'onlyhuman'
    },
    # 50. STAFFINGMS
    'StaffingMS': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'See Vacancy',
        'URL': 'Title_URL',
        'start': 'Date',
        'rate': 'Not mentioned',
        'Hours': 'hfp_cardiconsblockitem',
        'Duration': 'Not mentioned',
        'Company': 'Company',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'StaffingMS'
    },
    # 51. 4-FREELANCERS.NL
    '4-Freelancers.nl': {
        'Title': 'Functietitel',
        'URL': 'Functietitel_URL',
        'Location': 'Plaats',
        'Summary': 'Text6',
        'rate': 'Not mentioned',
        'Hours': 'Field3',
        'Duration': 'Text2',
        'Company': 'Not mentioned',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': '4-Freelancers.nl',
        'start': 'ASAP'
    },
    # 51. FLEXSPOT.IO
    'flexSpot.io': {
        'Title': 'Title',
        'Location': 'reset',
        'Summary': 'Field4',
        'URL': 'button_URL',
        'start': 'ASAP',
        'rate': 'Text1',
        'Hours': 'Field3',
        'Duration': 'Text3',
        'Company': 'Company',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'flexSpot.io'
    },
    # 52. ASNBANK
    'ASNBank': {
        'Title': 'Field2',
        'Location': 'Text',  # Will be processed to extract first part after splitting on space
        'Summary': 'Text1',
        'URL': 'Field1_links',
        'start': 'ASAP',
        'rate': 'Text',  # Will be processed to extract rate after "€"
        'Hours': 'Text',  # Will be processed to extract hours and remove "uur"
        'Duration': 'Not mentioned',
        'Company': 'ASNBank',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'ASNBank'
    },
    # 53. TENNET
    'tennet': {
        'Title': 'widgetheader',
        'Location': 'feature1',  # Will be processed to remove everything after "/"
        'Summary': 'Field4',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'feature2',
        'Hours': 'feature',
        'Duration': 'Not mentioned',
        'Company': 'TenneT',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'tennet'
    },
    # 54. INTERIMNETWERK
    'InterimNetwerk': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'Summary',
        'URL': 'URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Field3',
        'Duration': 'Duration',
        'Company': 'InterimNetwerk',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'InterimNetwerk'
    },
    # 55. FRIESLAND
    'Friesland': {
        'Title': 'Title',
        'URL': 'Title_URL',
        'Location': 'Field2',
        'Summary': 'Field4',
        'start': 'Not mentioned',
        'rate': 'Not mentioned',
        'Hours': 'Field3',
        'Duration': 'Not mentioned',
        'Company': 'caption',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'Friesland'
    },
    # 56. ZUID-HOLLAND
    'Zuid-Holland': {
        'Title': 'Title',
        'URL': 'Title_URL',
        'Company': 'Company',
        'Location': 'Location',
        'Hours': 'hfp_cardiconsblockitem',
        'Summary': 'Field4',
        'start': 'Date',
        'rate': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'Zuid-Holland'
    },
    # 57. COMET
    'comet': {
        'Title': 'Mission',
        'URL': 'Page_URL',
        'Location': 'Localisation',
        'Summary': 'Not mentioned',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Durée',
        'Company': 'Not mentioned',
        'Views': '',
        'Likes': '',
        'Keywords': 'Compétences_requises',
        'Offers': '',
        'Source': 'comet'
    },
    # 58. FREELANCE-INFORMATIQUE
    'Freelance-Informatique': {
        'Title': 'stretchedlink',
        'URL': 'Page_URL',
        'Location': 'colmd101',  # Will be processed to remove everything before "-" and the marker itself
        'Summary': 'lineclamp2',
        'start': 'Date',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'colmd102',
        'Company': 'Not mentioned',
        'Views': '',
        'Likes': '',
        'Keywords': 'Keywords',
        'Offers': '',
        'Source': 'Freelance-Informatique'
    },
    # 60. 404WORKS
    '404Works': {
        'Title': 'nomargin',
        'URL': 'nomargin_URL',
        'Location': 'pin',
        'Summary': 'Field1',
        'start': 'Field4',
        'rate': 'cash',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Views': '',
        'Likes': '',
        'Keywords': 'Keywords',
        'Offers': '',
        'Source': '404Works'
    },
    # 59. HAARLEMMERMEERHUURTIN
    'haarlemmermeerhuurtin': {
        'Title': 'Title',
        'Location': 'Pub_Time',  # Will be processed to extract text between "Standplaats:" and "|"
        'Summary': 'Pub_Time',
        'URL': 'Page_URL',
        'start': 'Pub_Time',
        'rate': 'Pub_Time',  # Will be processed to extract currency marker "€"
        'Hours': 'Pub_Time',
        'Duration': 'Pub_Time',
        'Company': 'Not mentioned',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'haarlemmermeerhuurtin'
    },
    # 60. HAYS
    'hays': {
        'Title': 'Description',
        'Location': 'Location',
        'Summary': 'Text',
        'URL': 'Description_URL',
        'start': '',
        'rate': 'Salary',
        'Hours': '',
        'Duration': '',
        'Company': 'Not mentioned',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': 'hays'
    },
    # 61. WELCOMETOTHEJUNGLE
    'welcometothejungle': {
        'Title': 'scbrzpdj',
        'URL': 'scjcbmet_URL',
        'Company': 'scizxthl',
        'Location': 'scgfgbys',
        'Summary': 'scizxthl3',
        'rate': 'Not mentioned',
        'start': 'ASAP',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Views': '',
        'Likes': '',
        'Keywords': 'scfibhhp5, scfibhhp4',
        'Offers': '',
        'Source': 'welcometothejungle'
    },
    # 62. 404WORKS
    '404Works': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'Text',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': '404works'
    },
    # 63. 4-FREELANCERS.NL
    '4-Freelancers.nl': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'Text',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Views': '',
        'Likes': '',
        'Keywords': '',
        'Offers': '',
        'Source': '4_freelancers_nl'
    },
}

def timestamp():
    """Get current timestamp in YYYY-MM-DD format."""
    return datetime.now().strftime("%Y-%m-%d")

def generate_unique_id(title, url, company):
    """Generate a unique ID based on the combination of title, URL, and company."""
    combined = f"{title}|{url}|{company}".encode('utf-8')
    return hashlib.md5(combined).hexdigest()

def generate_group_id(title):
    """Generate a group ID based on a cleaned-up title for grouping similar jobs."""
    # Normalize the title
    # 1. Convert to lowercase
    # 2. Remove punctuation and special characters
    # 3. Standardize whitespace
    try:
        cleaned_title = title.lower()
        cleaned_title = re.sub(r'[^\w\s]', '', cleaned_title) # Remove punctuation
        cleaned_title = re.sub(r'\s+', ' ', cleaned_title).strip() # Standardize whitespace
        
        # Log the transformation for debugging purposes (commented out for cleaner logs)
        # if title != cleaned_title:
        #     logging.info(f"group_id generation: Converted '{title}' to '{cleaned_title}'")
            
        combined = cleaned_title.encode('utf-8')
        return hashlib.md5(combined).hexdigest()
    except Exception as e:
        logging.error(f"Could not generate group_id for title: {title}. Error: {e}")
        # Fallback to using the raw title if cleaning fails
        return hashlib.md5(title.encode('utf-8')).hexdigest()

def generate_location_id(location, is_from_input=True):
    """Generate a location ID based on normalized location terms."""
    try:
        # If this is a default value from mapping, return empty ID
        if not is_from_input or pd.isna(location) or location == '':
            return hashlib.md5(''.encode('utf-8')).hexdigest()
        
        # List of common default values that should be treated as empty
        default_values = {'not mentioned', 'see vacancy', 'asap', 'remote', 'hybrid', 'on-site', 'onsite'}
        if str(location).lower().strip() in default_values:
            return hashlib.md5(''.encode('utf-8')).hexdigest()
        
        # Normalize location for ID generation
        cleaned_location = str(location).lower()
        # Remove common location prefixes/suffixes
        cleaned_location = re.sub(r'\b(remote|hybrid|on-site|onsite|work from home|wfh|locatie:|location:)\b', '', cleaned_location)
        # Remove punctuation and special characters
        cleaned_location = re.sub(r'[^\w\s]', '', cleaned_location)
        # Standardize whitespace
        cleaned_location = re.sub(r'\s+', ' ', cleaned_location).strip()
        
        if not cleaned_location:
            return hashlib.md5(''.encode('utf-8')).hexdigest()
        
        return hashlib.md5(cleaned_location.encode('utf-8')).hexdigest()
    except Exception as e:
        logging.error(f"Could not generate location_id for location: {location}. Error: {e}")
        return hashlib.md5(''.encode('utf-8')).hexdigest()

def generate_hours_id(hours, is_from_input=True):
    """Generate an hours ID based on the last number in ranges."""
    try:
        # If this is a default value from mapping, return empty ID
        if not is_from_input or pd.isna(hours) or hours == '':
            return hashlib.md5(''.encode('utf-8')).hexdigest()
        
        # List of common default values that should be treated as empty
        default_values = {'not mentioned', 'see vacancy', 'asap'}
        if str(hours).lower().strip() in default_values:
            return hashlib.md5(''.encode('utf-8')).hexdigest()
        
        hours_str = str(hours).strip()
        
        # Extract all numbers from the string
        numbers = re.findall(r'\d+', hours_str)
        if not numbers:
            return hashlib.md5(''.encode('utf-8')).hexdigest()
        
        # For ranges like "3-6", use the last number (6)
        last_number = numbers[-1]
        
        return hashlib.md5(last_number.encode('utf-8')).hexdigest()
    except Exception as e:
        logging.error(f"Could not generate hours_id for hours: {hours}. Error: {e}")
        return hashlib.md5(''.encode('utf-8')).hexdigest()

def generate_duration_id(duration, is_from_input=True):
    """Generate a duration ID based on numbers or calculated months from date ranges."""
    try:
        # If this is a default value from mapping, return empty ID
        if not is_from_input or pd.isna(duration) or duration == '':
            return hashlib.md5(''.encode('utf-8')).hexdigest()
        
        # List of common default values that should be treated as empty
        default_values = {'not mentioned', 'see vacancy', 'asap'}
        if str(duration).lower().strip() in default_values:
            return hashlib.md5(''.encode('utf-8')).hexdigest()
        
        duration_str = str(duration).strip()
        
        # Check for date ranges like "2024-01-01 to 2024-06-30"
        date_pattern = r'(\d{4}-\d{2}-\d{2})\s*(?:to|until|-)\s*(\d{4}-\d{2}-\d{2})'
        date_match = re.search(date_pattern, duration_str)
        
        if date_match:
            try:
                from datetime import datetime
                start_date = datetime.strptime(date_match.group(1), '%Y-%m-%d')
                end_date = datetime.strptime(date_match.group(2), '%Y-%m-%d')
                
                # Calculate months between dates
                months_diff = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)
                
                # Round to nearest month if there are partial months
                if end_date.day >= start_date.day:
                    months_diff += 1
                
                return hashlib.md5(str(months_diff).encode('utf-8')).hexdigest()
            except ValueError:
                # If date parsing fails, fall back to number extraction
                pass
        
        # Extract all numbers from the string
        numbers = re.findall(r'\d+', duration_str)
        if not numbers:
            return hashlib.md5(''.encode('utf-8')).hexdigest()
        
        # For ranges like "3-6", use the last number (6)
        last_number = numbers[-1]
        
        return hashlib.md5(last_number.encode('utf-8')).hexdigest()
    except Exception as e:
        logging.error(f"Could not generate duration_id for duration: {duration}. Error: {e}")
        return hashlib.md5(''.encode('utf-8')).hexdigest()

def get_generic_job_terms():
    """Return generic job terms covering ALL industries, excluding seniority levels."""
    return [
        # Core Job Roles
        'manager', 'director', 'coordinator', 'specialist', 'analyst', 'consultant', 
        'advisor', 'assistant', 'associate', 'executive', 'officer', 'representative',
        'administrator', 'supervisor', 'lead', 'head', 'chief', 'principal',
        
        # IT & Development
        'developer', 'programmer', 'coder', 'engineer', 'architect', 'designer',
        'technician', 'software', 'web', 'mobile', 'frontend', 'backend', 'fullstack',
        'devops', 'database', 'system', 'network', 'security', 'cloud', 'data',
        'qa', 'testing', 'scrum', 'agile', 'product owner', 'tech lead',
        
        # Job Functions
        'sales', 'marketing', 'finance', 'accounting', 'human resources', 'operations',
        'project', 'product', 'business', 'strategy', 'planning', 'development',
        'research', 'design', 'creative', 'communication', 'customer service',
        'support', 'maintenance', 'quality', 'safety', 'compliance', 'legal',
        
        # Industries
        'healthcare', 'education', 'retail', 'hospitality', 'manufacturing', 
        'construction', 'transport', 'logistics', 'energy', 'government',
        'nonprofit', 'insurance', 'banking', 'real estate', 'media', 'technology',
        
        # Skills & Competencies
        'leadership', 'management', 'communication', 'teamwork', 'problem solving',
        'analytical', 'creative', 'organizational', 'interpersonal', 'technical',
        'administrative', 'operational', 'strategic', 'financial', 'commercial',
        
        # Work Types
        'full time', 'part time', 'contract', 'temporary', 'permanent', 'freelance',
        'remote', 'hybrid', 'onsite', 'flexible', 'shift', 'weekend',
        
        # Common Job Titles
        'mechanic', 'driver', 'operator', 'worker', 'clerk', 'secretary', 
        'receptionist', 'cashier', 'server', 'cook', 'nurse', 'teacher', 
        'trainer', 'instructor', 'counselor', 'therapist', 'writer', 'editor', 
        'photographer', 'artist', 'lawyer', 'accountant', 'auditor', 'inspector',
        
        # Action Words
        'manage', 'coordinate', 'supervise', 'lead', 'develop', 'implement',
        'analyze', 'evaluate', 'monitor', 'maintain', 'operate', 'support',
        'assist', 'serve', 'deliver', 'provide', 'ensure', 'improve'
    ]

def generate_summary_id(summary, is_from_input=True):
    """Generate a summary ID based on generic job terms."""
    try:
        # If this is a default value from mapping, return empty ID
        if not is_from_input or pd.isna(summary) or summary == '':
            return hashlib.md5(''.encode('utf-8')).hexdigest()
        
        # List of common default values that should be treated as empty
        default_values = {'not mentioned', 'see vacancy', 'asap'}
        if str(summary).lower().strip() in default_values:
            return hashlib.md5(''.encode('utf-8')).hexdigest()
        
        # Get generic job terms
        job_terms = get_generic_job_terms()
        
        # Extract matching terms from summary
        summary_lower = str(summary).lower()
        found_terms = []
        
        for term in job_terms:
            # Check for exact word matches (not partial)
            if re.search(r'\b' + re.escape(term) + r'\b', summary_lower):
                found_terms.append(term)
        
        # If no terms found, return empty ID
        if not found_terms:
            return hashlib.md5(''.encode('utf-8')).hexdigest()
        
        # Sort terms for consistent ID generation
        found_terms.sort()
        
        # Create ID from found terms
        terms_string = '|'.join(found_terms)
        return hashlib.md5(terms_string.encode('utf-8')).hexdigest()
        
    except Exception as e:
        logging.error(f"Could not generate summary_id for summary: {summary}. Error: {e}")
        return hashlib.md5(''.encode('utf-8')).hexdigest()

def generate_source_id(source, is_from_input=True):
    """Generate a source ID for grouping jobs by their source/platform."""
    try:
        # If this is a default value from mapping, return empty ID
        if not is_from_input or pd.isna(source) or source == '':
            return hashlib.md5(''.encode('utf-8')).hexdigest()
        
        # Normalize the source name
        source_lower = str(source).lower().strip()
        
        # Remove common suffixes and prefixes
        source_normalized = source_lower
        
        # Remove common company suffixes
        suffixes_to_remove = ['.com', '.nl', '.org', '.eu', ' b.v.', ' bv', ' b.v', ' ltd', ' inc', ' corp', ' gmbh']
        for suffix in suffixes_to_remove:
            if source_normalized.endswith(suffix):
                source_normalized = source_normalized[:-len(suffix)].strip()
        
        # Remove common prefixes
        prefixes_to_remove = ['www.', 'http://', 'https://']
        for prefix in prefixes_to_remove:
            if source_normalized.startswith(prefix):
                source_normalized = source_normalized[len(prefix):].strip()
        
        # Handle special cases for known sources
        source_mappings = {
            # 1. TWINE
            'twine': 'twine',
            # 2. LINKIT
            'LinkIT': 'linkit',
            # 3. FREELANCE.NL
            'freelance.nl': 'freelance_nl',
            # 4. YACHT
            'Yacht': 'yacht',
            # 5. FLEXTENDER
            'Flextender': 'flextender',
            # 6. KVK
            'KVK': 'kvk',
            # 7. CIRCLE8
            'Circle8': 'circle8',
            # 8. BEBEE
            'Bebee': 'bebee',
            # 9. LINKEDIN
            'linkedin': 'linkedin',
            # 10. LINKEDINZZP
            'linkedinzzp': 'LinkedIn',
            # 11. LINKEDININTERIM
            'linkedininterim': 'LinkedIn',
            # 12. POLITIE
            'politie': 'politie',
            # 13. GELDERLAND
            'gelderland': 'gelderland',
            # 14. WERK.NL
            'werk.nl': 'werk_nl',
            # 15. INDEED
            'indeed': 'indeed',
            # 16. PLANET INTERIM
            'Planet Interim': 'planet_interim',
            # 17. HARVEY NASH
            'Harvey Nash': 'harvey_nash',
            # 18. BEHANCE
            'Behance': 'behance',
            # 19. JOOBLE
            'Jooble': 'jooble',
            # 20. CENTRIC
            'Centric': 'centric',
            # 21. CODEUR.COM
            'Codeur.com': 'codeur_com',
            # 22. SCHIPHOL
            'Schiphol': 'schiphol',
            # 23. RIJKSWATERSTAAT
            'rijkswaterstaat': 'rijkswaterstaat',
            # 24. NS
            'ns': 'ns',
            # 25. UMC
            'UMC': 'umc',
            # 26. HINTTECH
            'HintTech': 'hinttech',
            # 27. SELECT
            'Select': 'select',
            # 28. HOOFDKRAAN
            'hoofdkraan': 'hoofdkraan',
            # 29. ZOP OPDACHTEN
            'zzp opdrachten': 'zzp_opdrachten',
            # 30. FLEXVALUE_B.V.
            'FlexValue_B.V.': 'flexvalue',
            # 31. TALENTENREGIO
            'TalentenRegio': 'talentenregio',
            # 32. NOORD-HOLLAND
            'Noord-Holland': 'noord_holland',
            # 33. FLEVOLAND
            'flevoland': 'flevoland',
            # 34. GRONINGEN
            'groningen': 'groningenhuurtin',
            # 35. NOORDOOSTBRABANT
            'noordoostbrabant': 'noordoostbrabant',
            # 36. FLEX WEST-BRABANT
            'Flex West-Brabant': 'flex_west_brabant',
            # 37. AMSTELVEEN
            'amstelveen': 'amstelveenhuurtin',
            # 38. AMSTELVEENHUURTIN
            'Amstelveenhuurtin': 'amstelveenhuurtin',
            # 39. AMSTELVEENHUURTIN (ALT)
            'amstelveenhuurtin': 'amstelveenhuurtin',
            # 40. HAARLEMMERMEERHUURTIN
            'haarlemmermeerhuurtin': 'haarlemmermeer',
            # 41. HAYS
            'hays': 'hays',
            # 42. GRONINGENHUURTIN
            'groningenhuurtin': 'groningenhuurtin',
            # 43. GEMEENTE-PROJECTEN
            'gemeente-projecten': 'gemeente_projecten',
            # 44. GGDZWHUURTIN
            'ggdzwhuurtin': 'ggdz_whuurtin',
            # 45. GGDZWHUURTIN.NL
            'ggdzwhuurtin.nl': 'ggdz_whuurtin_nl',
            # 46. FREEP
            'freep': 'freep',
            # 47. ONLYHUMAN
            'onlyhuman': 'onlyhuman',
            # 48. STAFFINGMS
            'StaffingMS': 'staffingms',
            # 49. 4-FREELANCERS.NL
            '4-Freelancers.nl': '4_freelancers_nl',
            # 50. 4 FREELANCERS
            '4 freelancers': '4_freelancers',
            # 51. FLEXSPOT.IO
            'flexSpot.io': 'flexspot',
            # 52. ASNBANK
            'ASNBank': 'asnbank',
            # 53. FRIESLAND
            'Friesland': 'friesland',
            # 54. TENNET
            'tennet': 'tennet',
            # 55. INTERIMNETWERK
            'InterimNetwerk': 'interim_netwerk',
            # 56. ZUID-HOLLAND
            'Zuid-Holland': 'zuid_holland',
            # 57. OVERHEIDZZP
            'overheidzzp': 'overheidzzp',
            # 58. POOQ
            'POOQ': 'POOQ',
            # 59. COMET
            'comet': 'comet',
            # 60. FREELANCE-INFORMATIQUE
            'Freelance-Informatique': 'freelance_informatique',
            # 61. 404WORKS
            '404Works': '404works',
            # 62. WELCOMETOTHEJUNGLE
            'welcometothejungle': 'welcome_to_the_jungle',
            # 63. 404WORKS
            '404Works': '404works',
            # 64. 4-FREELANCERS.NL
            '4-Freelancers.nl': '4_freelancers_nl',
            # 'zoekklus': 'zoekklus'
        }
        
        # Check if normalized source matches any known mapping
        for key, mapped_value in source_mappings.items():
            if key in source_normalized:
                source_normalized = mapped_value
                break
        
        # Replace spaces and special characters with underscores
        source_normalized = re.sub(r'[^a-z0-9]', '_', source_normalized)
        
        # Remove multiple underscores
        source_normalized = re.sub(r'_+', '_', source_normalized).strip('_')
        
        # If empty after normalization, use original
        if not source_normalized:
            source_normalized = str(source).lower().strip()
        
        return hashlib.md5(source_normalized.encode('utf-8')).hexdigest()
        
    except Exception as e:
        logging.error(f"Could not generate source_id for source: {source}. Error: {e}")
        return hashlib.md5(''.encode('utf-8')).hexdigest()

def is_from_input_value(value):
    """Check if a value is from actual input or a default mapping."""
    if pd.isna(value) or value == '':
        return False
    
    # Known default values that should be treated as "not from input"
    default_values = {
        'not mentioned', 'see vacancy', 'asap', 'remote', 'hybrid', 'on-site', 'onsite',
        'amsterdam', 'hilversum', 'gelderland', '36', 'price'
    }
    
    return str(value).lower().strip() not in default_values

def validate_dataframe(df, required_columns):
    """Validate that DataFrame has required columns and data."""
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logging.warning(f"Missing required columns: {missing_columns}")
    
    if df.empty:
        logging.warning("DataFrame is empty")
    
    return True

def validate_data_quality(df, required_columns):
    """
    Simplified data validation without URL, special character, or data quality checks.
    Returns a boolean indicating if the DataFrame is valid.
    """
    try:
        # Basic validation first
        validate_dataframe(df, required_columns)
        
        # Data type validation (log issues but do not cause errors)
        expected_types = {
            'Title': str,
            'Location': str,
            'Summary': str,
            'URL': str,
            'Company': str,
            'date': str,
            'UNIQUE_ID': str
        }
        
        for col, expected_type in expected_types.items():
            if col in df.columns:
                mismatched_types = [val for val in df[col].dropna() if not isinstance(val, expected_type)]
                if mismatched_types:
                    logging.warning(f"Data type mismatch in column {col}: {mismatched_types}")
        
        return True
    except Exception as e:
        logging.error(f"Data validation error: {str(e)}")
        return False

def freelance_directory(files_read, company_name):
    """Process and standardize job listings from different sources."""
    try:
        # Get the mapping for this company
        mapping = COMPANY_MAPPINGS.get(company_name)
        if not mapping:
            logging.warning(f"No mapping found for company {company_name}")
            return pd.DataFrame()
        
        # First replace all NaN/None values with empty strings in the input DataFrame
        files_read = files_read.fillna('')
        
        # COMPANY-SPECIFIC PRE-MAPPING FILTERS
        if company_name == 'ProLinker.com':
            # No pre-mapping filter - removed text column check for "Open"
            pass
        elif company_name == 'werkzoeken.nl':
            if 'requestedwrapper2' in files_read.columns:
                initial_count = len(files_read)
                # Filter for rows where 'requestedwrapper2' column contains "Freelance" (case-insensitive)
                files_read = files_read[files_read['requestedwrapper2'].astype(str).str.contains("Freelance", case=False, na=False)]
                filtered_count = len(files_read)
                if initial_count > filtered_count:
                    logging.info(f"Applied werkzoeken.nl pre-mapping filter on 'requestedwrapper2' column for 'Freelance', rows changed from {initial_count} to {filtered_count}")
                elif initial_count == filtered_count and initial_count > 0:
                    logging.info(f"werkzoeken.nl pre-mapping filter: 'requestedwrapper2' column checked for 'Freelance', no rows removed from {initial_count} rows.")
            else:
                logging.warning(f"werkzoeken.nl pre-mapping filter: 'requestedwrapper2' column not found in the input CSV for {company_name}. Skipping filter.")
        elif company_name == 'HintTech':
            if 'Title' in files_read.columns:
                initial_count = len(files_read)
                # Filter out rows where 'Title' column contains "gesloten" (case-insensitive)
                files_read = files_read[~files_read['Title'].astype(str).str.contains("gesloten", case=False, na=False)]
                filtered_count = len(files_read)
                if initial_count > filtered_count:
                    logging.info(f"Applied HintTech pre-mapping filter on 'Title' column to remove 'gesloten', rows changed from {initial_count} to {filtered_count}")
                elif initial_count == filtered_count and initial_count > 0:
                    logging.info(f"HintTech pre-mapping filter: 'Title' column checked for 'gesloten', no rows removed from {initial_count} rows.")
            else:
                logging.warning(f"HintTech pre-mapping filter: 'Title' column not found in the input CSV for {company_name}. Skipping filter.")
        elif company_name == 'Behance':
            if 'Text4' in files_read.columns:
                initial_count = len(files_read)
                # Filter out rows where 'Text4' column is empty or contains only whitespace
                files_read = files_read[files_read['Text4'].astype(str).str.strip() != '']
                # Also filter out rows where Text4 is NaN or 'nan' string
                files_read = files_read[~files_read['Text4'].astype(str).str.lower().isin(['nan', 'none', ''])]
                filtered_count = len(files_read)
                if initial_count > filtered_count:
                    logging.info(f"Applied Behance pre-mapping filter on 'Text4' column to remove empty rows, rows changed from {initial_count} to {filtered_count}")
                elif initial_count == filtered_count and initial_count > 0:
                    logging.info(f"Behance pre-mapping filter: 'Text4' column checked for empty content, no rows removed from {initial_count} rows.")
            else:
                logging.warning(f"Behance pre-mapping filter: 'Text4' column not found in the input CSV for {company_name}. Skipping filter.")
        elif company_name == 'Bebee':
            # Step 1: Keep rows that mention freelance-related terms in either Field1_text or Text (case-insensitive, substring allowed)
            freelance_keywords = ['freelance', 'interim', 'zzp', 'flexibele', 'remote']
            if 'Field1_text' in files_read.columns or 'Text' in files_read.columns:
                initial_count = len(files_read)

                def contains_freelance_keywords(value):
                    try:
                        text = str(value).lower()
                        return any(keyword in text for keyword in freelance_keywords)
                    except Exception:
                        return False

                mask_field1 = files_read['Field1_text'].apply(contains_freelance_keywords) if 'Field1_text' in files_read.columns else False
                mask_text = files_read['Text'].apply(contains_freelance_keywords) if 'Text' in files_read.columns else False
                # If only one column exists, the other mask will be a scalar False, bitwise OR will still work
                freelance_mask = mask_field1 | mask_text
                files_read = files_read[freelance_mask]
                post_freelance_count = len(files_read)

                # Step 2: Remove rows that contain permanent job markers (salary, baan, etc.)
                permanent_job_markers = ['salaris', 'base salary', 'startersbaan', 'salary', 'salarisrange', 'baan', 'maandsalaris', 'monthly salary', 'loon', 'wage']

                def contains_permanent_markers(value):
                    try:
                        text = str(value).lower()
                        return any(marker in text for marker in permanent_job_markers)
                    except Exception:
                        return False

                # Check both Text and Summary columns for permanent job markers
                permanent_mask = pd.Series([False] * len(files_read), index=files_read.index)
                
                if 'Text' in files_read.columns:
                    text_mask = files_read['Text'].apply(contains_permanent_markers)
                    permanent_mask = permanent_mask | text_mask
                
                if 'Summary' in files_read.columns:
                    summary_mask = files_read['Summary'].apply(contains_permanent_markers)
                    permanent_mask = permanent_mask | summary_mask
                
                permanent_count = permanent_mask.sum()
                files_read = files_read[~permanent_mask]  # Remove rows with permanent markers
                final_count = len(files_read)

                # Log results
                if initial_count > post_freelance_count:
                    logging.info(f"Applied Bebee freelance filter: rows {initial_count} → {post_freelance_count} (kept freelance terms)")

                if permanent_count > 0:
                    logging.info(f"Applied Bebee permanent job filter: removed {permanent_count} rows with markers {permanent_job_markers} from Text/Summary columns, final count: {final_count}")
                    logging.info(f"Bebee filtering complete: {initial_count} → {final_count} rows (reason: not freelance jobs)")
                else:
                    logging.info(f"Bebee permanent job filter: No rows removed, final count: {final_count}")
            else:
                logging.warning(f"Bebee filtering: Neither 'Field1_text' nor 'Text' column found in the input CSV for {company_name}. Skipping all filters.")
        elif company_name == 'Codeur.com':
            if 'Text' in files_read.columns:
                initial_count = len(files_read)
                # Drop rows where 'Text' column contains "Fermé" (case-insensitive)
                files_read = files_read[~files_read['Text'].astype(str).str.contains("Fermé", case=False, na=False)]
                filtered_count = len(files_read)
                if initial_count > filtered_count:
                    logging.info(f"Applied Codeur.com pre-mapping filter on 'Text' column to remove 'Fermé' rows, rows changed from {initial_count} to {filtered_count}")
                elif initial_count == filtered_count and initial_count > 0:
                    logging.info(f"Codeur.com pre-mapping filter: 'Text' column checked for 'Fermé' content, no rows removed from {initial_count} rows.")
            else:
                logging.warning(f"Codeur.com pre-mapping filter: 'Text' column not found in the input CSV for {company_name}. Skipping filter.")
        elif company_name == 'welcometothejungle':
            if 'scfibhhp' in files_read.columns:
                initial_count = len(files_read)
                # Keep only rows where 'scfibhhp' column contains "freelance" (case-insensitive)
                files_read = files_read[files_read['scfibhhp'].astype(str).str.contains("freelance", case=False, na=False)]
                filtered_count = len(files_read)
                if initial_count > filtered_count:
                    logging.info(f"Applied Welcome to the Jungle pre-mapping filter on 'scfibhhp' column to keep only 'freelance' rows, rows changed from {initial_count} to {filtered_count}")
                elif initial_count == filtered_count and initial_count > 0:
                    logging.info(f"Welcome to the Jungle pre-mapping filter: 'scfibhhp' column checked for 'freelance' content, no rows removed from {initial_count} rows.")
            else:
                logging.warning(f"Welcome to the Jungle pre-mapping filter: 'scfibhhp' column not found in the input CSV for {company_name}. Skipping filter.")
        elif company_name == 'Overheid':
            if 'Field3' in files_read.columns:
                initial_count = len(files_read)
                # Filter out rows where 'Field3' column contains "Loondienst" (case-insensitive)
                files_read = files_read[~files_read['Field3'].astype(str).str.contains("Loondienst", case=False, na=False)]
                filtered_count = len(files_read)
                if initial_count > filtered_count:
                    logging.info(f"Applied Overheid pre-mapping filter on 'Field3' column to remove 'Loondienst', rows changed from {initial_count} to {filtered_count}")
                elif initial_count == filtered_count and initial_count > 0:
                    logging.info(f"Overheid pre-mapping filter: 'Field3' column checked for 'Loondienst', no rows removed from {initial_count} rows.")
            else:
                logging.warning(f"Overheid pre-mapping filter: 'Field3' column not found in the input CSV for {company_name}. Skipping filter.")
        elif company_name == 'overheidzzp':
            # No pre-mapping filter anymore as per new spec
            pass
        elif company_name == 'freep':
            if 'flex1' in files_read.columns:
                initial_count = len(files_read)
                # Filter for rows where 'flex1' column contains "freelance" (case-insensitive)
                files_read = files_read[files_read['flex1'].astype(str).str.contains("freelance", case=False, na=False)]
                filtered_count = len(files_read)
                if initial_count > filtered_count:
                    logging.info(f"Applied freep pre-mapping filter on 'flex1' column for 'freelance', rows changed from {initial_count} to {filtered_count}")
                elif initial_count == filtered_count and initial_count > 0:
                    logging.info(f"freep pre-mapping filter: 'flex1' column checked for 'freelance', no rows removed from {initial_count} rows.")
            else:
                logging.warning(f"freep pre-mapping filter: 'flex1' column not found in the input CSV for {company_name}. Skipping filter.")
        elif company_name == 'onlyhuman':
            if 'px2' in files_read.columns:
                initial_count = len(files_read)
                # Filter for rows where 'px2' column contains "Actueel" (case-insensitive)
                files_read = files_read[files_read['px2'].astype(str).str.contains("Actueel", case=False, na=False)]
                filtered_count = len(files_read)
                if initial_count > filtered_count:
                    logging.info(f"Applied onlyhuman pre-mapping filter on 'px2' column for 'Actueel', rows changed from {initial_count} to {filtered_count}")
                elif initial_count == filtered_count and initial_count > 0:
                    logging.info(f"onlyhuman pre-mapping filter: 'px2' column checked for 'Actueel', no rows removed from {initial_count} rows.")
            else:
                logging.warning(f"onlyhuman pre-mapping filter: 'px2' column not found in the input CSV for {company_name}. Skipping filter.")
        elif company_name == 'flexSpot.io':
            if 'Text2' in files_read.columns:
                initial_count = len(files_read)
                # Filter for rows where 'Text2' column contains "Freelance" (case-insensitive)
                files_read = files_read[files_read['Text2'].astype(str).str.contains("Freelance", case=False, na=False)]
                filtered_count = len(files_read)
                if initial_count > filtered_count:
                    logging.info(f"Applied flexSpot.io pre-mapping filter on 'Text2' column for 'Freelance', rows changed from {initial_count} to {filtered_count}")
                elif initial_count == filtered_count and initial_count > 0:
                    logging.info(f"flexSpot.io pre-mapping filter: 'Text2' column checked for 'Freelance', no rows removed from {initial_count} rows.")
            else:
                logging.warning(f"flexSpot.io pre-mapping filter: 'Text2' column not found in the input CSV for {company_name}. Skipping filter.")
        elif company_name == '404Works':
            if 'Field6' in files_read.columns:
                initial_count = len(files_read)
                # Filter out rows where 'Field6' column contains "Status Closed" (case-insensitive, handles extra whitespace)
                files_read = files_read[~files_read['Field6'].astype(str).str.contains(r"\s*Status\s+Closed\s*", case=False, na=False)]
                filtered_count = len(files_read)
                if initial_count > filtered_count:
                    logging.info(f"Applied 404Works pre-mapping filter on 'Field6' column to remove 'Status Closed' rows, rows changed from {initial_count} to {filtered_count}")
                elif initial_count == filtered_count and initial_count > 0:
                    logging.info(f"404Works pre-mapping filter: 'Field6' column checked for 'Status Closed', no rows removed from {initial_count} rows.")
            else:
                logging.warning(f"404Works pre-mapping filter: 'Field6' column not found in the input CSV for {company_name}. Skipping filter.")
        elif company_name == 'InterimNetwerk':
            # Special processing for InterimNetwerk: extract data from Text column
            if 'Text' in files_read.columns and 'Field1_links' in files_read.columns:
                # Take only the first row of Text column but keep all Field1_links
                if len(files_read) > 0:
                    text_content = files_read['Text'].iloc[0]
                    # Combine all Field1_links from all rows to get complete URL list
                    all_field1_links = []
                    for idx, row in files_read.iterrows():
                        if pd.notna(row['Field1_links']) and row['Field1_links']:
                            all_field1_links.append(str(row['Field1_links']))
                    field1_links = " ".join(all_field1_links)
                    
                    # Split text into individual job blocks using the 5-digit number pattern
                    job_blocks = re.split(r'(\d{5}[A-Za-z])', str(text_content))
                    
                    # Filter out empty blocks and reconstruct job blocks
                    jobs = []
                    for i in range(1, len(job_blocks), 2):
                        if i + 1 < len(job_blocks):
                            job_header = job_blocks[i]  # e.g., "88959Interim"
                            job_content = job_blocks[i + 1]  # rest of the job content
                            full_job = job_header + job_content
                            jobs.append(full_job)
                    
                    processed_data = []
                    for job_block in jobs:
                        # Extract 5-digit number from start of block
                        number_match = re.match(r'(\d{5})', job_block)
                        if not number_match:
                            continue
                            
                        number = number_match.group(1)
                        
                        # Extract title: text after number until we hit duration info or location
                        title_pattern = rf'{number}([^|]*?)(?=\d+ maanden|\d+ jaar|Half jaar|Verwachte opdrachtduur|Plaats/regio|\|)'
                        title_match = re.search(title_pattern, job_block)
                        title = title_match.group(1).strip() if title_match else "Not found"
                        title = re.sub(r'\s+', ' ', title).strip()  # Clean up whitespace
                        
                        # Extract duration first to remove it from title later
                        duration_pattern = r'Verwachte opdrachtduur:\s*([^\n\r]*?)(?=\n|\r|Plaats/regio|$)'
                        duration_match = re.search(duration_pattern, job_block, re.DOTALL)
                        duration = duration_match.group(1).strip() if duration_match else "Not mentioned"
                        
                        # Clean title by removing duration text that appears in it
                        if duration != "Not mentioned" and duration:
                            # Remove the exact duration text from title
                            title = title.replace(duration, "").strip()
                            # Remove common duration patterns that might appear in title
                            duration_patterns_to_remove = [
                                r'\d+\s*maanden?',
                                r'\d+\s*jaar',
                                r'Half\s*jaar',
                                r'\d+\s*-\s*\d+\s*maanden?',
                                r'\d+\s*uur\s*per\s*week',
                                r'\d+\s*dagen\s*per\s*week',
                                r'gemiddeld\s*\d+\s*uur',
                                r'fulltime',
                                r'start\s*asap',
                                r'start:\s*\d+',
                                r'optie\s*tot\s*verlenging'
                            ]
                            for pattern in duration_patterns_to_remove:
                                title = re.sub(pattern, '', title, flags=re.IGNORECASE).strip()
                        
                        # Final cleanup of title
                        title = re.sub(r'\s+', ' ', title).strip()
                        title = re.sub(r'^[,\-\s]+|[,\-\s]+$', '', title).strip()  # Remove leading/trailing punctuation
                        

                        
                        # Extract location: text after "Plaats/regio:"
                        location_pattern = r'Plaats/regio:\s*([^\n\r]*?)(?=\n|\r|Profiel|$)'
                        location_match = re.search(location_pattern, job_block, re.DOTALL)
                        location = location_match.group(1).strip() if location_match else "Not mentioned"
                        
                        # Extract summary: combine "Profiel van het bedrijf:" and "Profiel van de opdracht:"
                        summary_parts = []
                        
                        # Find "Profiel van het bedrijf:"
                        bedrijf_pattern = r'Profiel van het bedrijf:\s*(.*?)(?=Profiel van de opdracht|Profiel van de manager|Opmerkingen|Nu reageren|$)'
                        bedrijf_match = re.search(bedrijf_pattern, job_block, re.DOTALL)
                        if bedrijf_match:
                            bedrijf_text = re.sub(r'\s+', ' ', bedrijf_match.group(1).strip())
                            summary_parts.append(f"Bedrijf: {bedrijf_text}")
                        
                        # Find "Profiel van de opdracht:"
                        opdracht_pattern = r'Profiel van de opdracht:\s*(.*?)(?=Profiel van de manager|Opmerkingen|Nu reageren|$)'
                        opdracht_match = re.search(opdracht_pattern, job_block, re.DOTALL)
                        if opdracht_match:
                            opdracht_text = re.sub(r'\s+', ' ', opdracht_match.group(1).strip())
                            summary_parts.append(f"Opdracht: {opdracht_text}")
                        
                        summary = " | ".join(summary_parts) if summary_parts else "Not mentioned"
                        
                        # Find matching URL for this number in Field1_links
                        url = "Not found"
                        if field1_links and number in str(field1_links):
                            url_pattern = rf'(https?://[^\s,]*{number}[^\s,]*)'
                            url_match = re.search(url_pattern, str(field1_links))
                            if url_match:
                                url = url_match.group(1)
                        
                        processed_data.append({
                            'Title': title,
                            'Location': location,
                            'Summary': summary,
                            'URL': url,
                            'Duration': duration,
                            'start': 'ASAP',
                            'rate': 'Not mentioned',
                            'Hours': 'Field3',
                                                    'Company': 'InterimNetwerk',
                        'Source': 'InterimNetwerk'
                        })
                    
                    # Convert to DataFrame
                    files_read = pd.DataFrame(processed_data)
                    logging.info(f"InterimNetwerk special processing: Created {len(files_read)} rows from {len(jobs)} job blocks")
                else:
                    logging.warning("InterimNetwerk: No data found in CSV file")
                    files_read = pd.DataFrame()
            else:
                logging.warning("InterimNetwerk: Required columns 'Text' and 'Field1_links' not found")
                files_read = pd.DataFrame()
        
        # Create a new DataFrame with standardized columns
        result = pd.DataFrame()
        
        # Map the columns according to the mapping
        for std_col, src_col_mapping_value in mapping.items():
            if src_col_mapping_value in files_read.columns:
                if company_name == 'werk.nl' and std_col == 'Company' and src_col_mapping_value == 'Description':
                    # Special handling for werk.nl: split Description on "-" and use first part as Company
                    result[std_col] = files_read['Description'].str.split('-').str[0].str.strip()
                else:
                    result[std_col] = files_read[src_col_mapping_value]
            elif '+' in src_col_mapping_value or ',' in src_col_mapping_value:
                # Handle column merging (e.g., 'col1+col2+col3' or 'col1, col2, col3')
                if '+' in src_col_mapping_value:
                    columns_to_merge = src_col_mapping_value.split('+')
                else:  # comma-separated
                    columns_to_merge = [col.strip() for col in src_col_mapping_value.split(',')]
                
                existing_columns = [col for col in columns_to_merge if col in files_read.columns]
                
                if existing_columns:
                    # Merge the existing columns with space separator
                    merged_data = files_read[existing_columns[0]].astype(str)
                    for col in existing_columns[1:]:
                        merged_data = merged_data + ' ' + files_read[col].astype(str)
                    result[std_col] = merged_data
                else:
                    # No columns found, assign empty string
                    result[std_col] = pd.Series([''] * len(files_read), index=files_read.index)
            else:
                # If the mapping value is not a column name, assign the mapping value itself.
                # This handles literal defaults like 'Company': 'LinkIT' or 'start': 'ASAP'.
                # If files_read is empty (e.g. due to pre-mapping filter), ensure Series is not created with wrong length.
                if files_read.empty:
                    # Create an empty series of appropriate type if result is also going to be empty for this column
                    result[std_col] = pd.Series(dtype='object') 
                else:
                    result[std_col] = src_col_mapping_value

        # NEW SAFEGUARD: If data matches the mapping key string, blank it, unless it's an intentional literal.
        for std_col, src_col_mapping_value in mapping.items():
            if std_col in result.columns and isinstance(src_col_mapping_value, str) and not result[std_col].empty:
                # Determine if src_col_mapping_value should be preserved (not blanked out)
                # Check if it's a column name in the original CSV
                is_column_name = src_col_mapping_value in files_read.columns
                
                # Also check for common column names that should be preserved
                # Note: Field4 is removed from this list to prevent showing "Field4" as literal text
                # Note: Field1 is removed from this list to prevent blanking out legitimate Field1 data
                common_column_names = {'Text2', 'Location', 'searchresultorganisation', 'Keywords'}
                is_common_column = src_col_mapping_value in common_column_names
                
                # Check if it's a common literal value
                is_literal_value = src_col_mapping_value in {'ASAP', 'Not mentioned', 'See Vacancy', '36', 'Hybrid', 'Remote', 'Hilversum', 'Gelderland', 'Amsterdam', 'not mentioned'}
                
                # Check if it's a special case
                is_special_case = (
                    (std_col == 'URL' and (src_col_mapping_value.startswith('http://') or src_col_mapping_value.startswith('https://'))) or \
                    std_col == 'Company' or \
                    std_col == 'Source' or \
                    std_col == 'Hours'  # Hours field values are always intentional literals
                )
                
                # Don't blank out if it's a column name, literal value, or special case
                is_intentional_literal = is_column_name or is_common_column or is_literal_value or is_special_case
                # Special case: For freelancer.com, treat 'Price' as intentional literal
                if company_name == 'freelancer.com' and src_col_mapping_value == 'Price':
                    is_intentional_literal = True
                
                # If the current column IS 'URL', we give it special treatment: 
                # it should NOT be blanked if its mapping value is a placeholder like 'Title_URL' or 'URL_Column_Name'
                # UNLESS that placeholder is ALSO a generic http/https link (which is handled above)
                # This effectively means: only blank URL if the mapping value was a generic placeholder AND the data matches it.
                # However, if the mapping value itself was a specific URL (like a base URL for a site), and data matches, it should be KEPT.
                # The original logic before the last user request was to always keep URLs if their mapping value started with http/https.
                # The request before this one was to blank them out even if they were http/https.
                # This request is to NOT blank them out if std_col is URL, regardless of what src_col_mapping_value is, as long as data matches.
                # Actually, the most straightforward way to implement "in the URL section it does not matter if it is placeholder" 
                # is to simply NOT apply the blanking logic IF std_col == 'URL'.

                if std_col == 'URL': # If it's the URL column, we don't apply the placeholder blanking based on this user request.
                    pass # Do nothing, leave URL as is from the mapping.
                elif not is_intentional_literal and not (std_col == 'Location' and src_col_mapping_value == 'Remote'):
                    # If it's not an intentional literal (and not 'URL'), check if data matches the mapping value (placeholder scenario)
                    condition = result[std_col].astype(str) == src_col_mapping_value
                    if condition.any():
                        actual_matches = result.loc[condition, std_col].unique()
                        logging.info(f"INFO: For {company_name}, blanking out {condition.sum()} instance(s) in '{std_col}' because data {list(actual_matches)} matched mapping value '{src_col_mapping_value}' (which is treated as a placeholder).")
                        result.loc[condition, std_col] = ''
        # End of new safeguard block
        
        # Clean the data
        standard_columns = ['Title', 'Location', 'Summary', 'URL', 'start', 'rate', 'Company', 'Views', 'Likes', 'Keywords', 'Offers']
        for col in standard_columns:
            if col in result.columns:
                # Convert to string and clean whitespace
                result[col] = result[col].astype(str).str.strip()
                # Replace empty strings and 'nan' strings with empty string
                result[col] = result[col].replace(['nan', 'None', 'NaN', 'none'], '')
                result[col] = result[col].fillna('')
        
        # Apply fallbacks for empty values after initial cleaning
        fallback_map = {
            'Location': "the Netherlands",
            'Summary': "See Vacancy",
            'start': "ASAP",
            'rate': "Not mentioned"
        }
        for col_name, fallback_value in fallback_map.items():
            if col_name in result.columns:
                result[col_name] = result[col_name].fillna(fallback_value)
        
        # Apply fallback for Company column using the company_name variable
        if 'Company' in result.columns:
            result.loc[result['Company'] == '', 'Company'] = company_name

        # WERK.NL POST-MAPPING PROCESSING
        if company_name == 'werk.nl':
            # Process rate field - extract only relevant numbers (rates/salaries) from Text column
            if 'rate' in result.columns:
                def extract_strict_rate(text_str):
                    if pd.isna(text_str) or text_str == '':
                        return 'Not mentioned'
                    
                    text_clean = str(text_str).strip()
                    
                    # Look for specific rate/salary patterns only
                    import re
                    
                    # Pattern for "€X/hour" or "€X per hour" or "€X/uur"
                    euro_per_hour = re.search(r'€\s*(\d+(?:\.\d+)?)\s*(?:per\s+hour|/hour|/uur)', text_clean, re.IGNORECASE)
                    if euro_per_hour:
                        amount = float(euro_per_hour.group(1))
                        return f'€{amount:.0f}/hour'
                    
                    # Pattern for "€X/day" or "€X per day" or "€X/dag"
                    euro_per_day = re.search(r'€\s*(\d+(?:\.\d+)?)\s*(?:per\s+day|/day|/dag)', text_clean, re.IGNORECASE)
                    if euro_per_day:
                        amount = float(euro_per_day.group(1))
                        return f'€{amount:.0f}/day'
                    
                    # Pattern for "€X/month" or "€X per month" or "€X/maand"
                    euro_per_month = re.search(r'€\s*(\d+(?:\.\d+)?)\s*(?:per\s+month|/month|/maand)', text_clean, re.IGNORECASE)
                    if euro_per_month:
                        amount = float(euro_per_month.group(1))
                        return f'€{amount:.0f}/month'
                    
                    # Pattern for "€X/year" or "€X per year" or "€X/jaar"
                    euro_per_year = re.search(r'€\s*(\d+(?:\.\d+)?)\s*(?:per\s+year|/year|/jaar)', text_clean, re.IGNORECASE)
                    if euro_per_year:
                        amount = float(euro_per_year.group(1))
                        return f'€{amount:.0f}/year'
                    
                    # Pattern for salary ranges "€X - €Y" or "€X tot €Y"
                    salary_range = re.search(r'€\s*(\d+(?:\.\d+)?)\s*(?:-|tot)\s*€\s*(\d+(?:\.\d+)?)', text_clean)
                    if salary_range:
                        min_amount = float(salary_range.group(1))
                        max_amount = float(salary_range.group(2))
                        return f'€{min_amount:.0f} - €{max_amount:.0f}'
                    
                    # Pattern for standalone "€X" (but avoid phone numbers, dates, etc.)
                    # Look for € followed by number but not in phone/date contexts
                    euro_standalone = re.search(r'(?<![\d\w])\€\s*(\d{2,5}(?:\.\d+)?)(?![\d\w])', text_clean)
                    if euro_standalone:
                        amount = float(euro_standalone.group(1))
                        # Only accept reasonable salary amounts (€20-€1000)
                        if 20 <= amount <= 1000:
                            return f'€{amount:.0f}'
                    
                    # Pattern for "X euro" or "X EUR" (but avoid phone numbers)
                    euro_text = re.search(r'(?<![\d\w])(\d{2,5}(?:\.\d+)?)\s*(?:euro|EUR)(?![\d\w])', text_clean, re.IGNORECASE)
                    if euro_text:
                        amount = float(euro_text.group(1))
                        # Only accept reasonable salary amounts (€20-€1000)
                        if 20 <= amount <= 1000:
                            return f'€{amount:.0f}'
                    
                    return 'Not mentioned'
                
                result['rate'] = result['rate'].apply(extract_strict_rate)
                logging.info(f"werk.nl post-mapping: Processed rate field with strict number filtering")
        
        # TWINE POST-MAPPING PROCESSING
        if company_name == 'twine':
            # Process Title field - remove "Easy Apply " text
            if 'Title' in result.columns:
                def process_title_twine(title_str):
                    if pd.isna(title_str) or title_str == '':
                        return 'Not mentioned'
                    
                    title_clean = str(title_str).strip()
                    
                    # Remove "Easy Apply " (case insensitive)
                    title_clean = title_clean.replace('Easy Apply ', '').replace('easy apply ', '')
                    
                    # Clean up extra spaces
                    title_clean = ' '.join(title_clean.split()).strip()
                    
                    return title_clean if title_clean else 'Not mentioned'
                
                result['Title'] = result['Title'].apply(process_title_twine)
                logging.info(f"twine post-mapping: Processed Title field to remove 'Easy Apply ' text")
        
        # GEMEENTE PROJECTEN POST-MAPPING PROCESSING
        if company_name == 'gemeente-projecten':
            # Process rate field - extract currency from Field4
            if 'rate' in result.columns:
                def extract_currency_from_field4(text_str):
                    if pd.isna(text_str) or text_str == '':
                        return 'Not mentioned'
                    
                    text_clean = str(text_str).strip()
                    
                    # Look for currency patterns
                    import re
                    
                    # Pattern for "€X" or "€ X"
                    euro_pattern = re.search(r'€\s*(\d+(?:\.\d+)?)', text_clean)
                    if euro_pattern:
                        amount = float(euro_pattern.group(1))
                        return f'€{amount:.0f}'
                    
                    # Pattern for "X euro" or "X EUR"
                    euro_text_pattern = re.search(r'(\d+(?:\.\d+)?)\s*(?:euro|EUR)', text_clean, re.IGNORECASE)
                    if euro_text_pattern:
                        amount = float(euro_text_pattern.group(1))
                        return f'€{amount:.0f}'
                    
                    # Pattern for "Tariefindicatie: €X" or similar
                    tarief_pattern = re.search(r'tariefindicatie[:\s]*€\s*(\d+(?:\.\d+)?)', text_clean, re.IGNORECASE)
                    if tarief_pattern:
                        amount = float(tarief_pattern.group(1))
                        return f'€{amount:.0f}'
                    
                    # If no currency found, return "Not mentioned"
                    return 'Not mentioned'
                
                result['rate'] = result['rate'].apply(extract_currency_from_field4)
                logging.info(f"gemeente-projecten post-mapping: Processed rate field to extract currency from Field4")
        
        # TENNET POST-MAPPING PROCESSING
        if company_name == 'tennet':
            # Process location field - remove everything after "/"
            if 'Location' in result.columns:
                def clean_location_tennet(location_str):
                    if pd.isna(location_str) or location_str == '':
                        return 'Not mentioned'
                    
                    location_clean = str(location_str).strip()
                    
                    # Remove everything after "/" (including the "/" itself)
                    if '/' in location_clean:
                        location_clean = location_clean.split('/')[0].strip()
                    
                    return location_clean if location_clean else 'Not mentioned'
                
                result['Location'] = result['Location'].apply(clean_location_tennet)
                logging.info(f"tennet post-mapping: Processed location field to remove content after '/'")
        
        # CIRCLE8 POST-MAPPING PROCESSING
        if company_name == 'Circle8':
            # Process Summary field - provide fallback text if no information is provided
            if 'Summary' in result.columns:
                def process_summary_circle8(summary_str):
                    if pd.isna(summary_str) or summary_str == '' or str(summary_str).strip() == '':
                        return 'We were not able to find description'
                    
                    summary_clean = str(summary_str).strip()
                    return summary_clean if summary_clean else 'We were not able to find description'
                
                result['Summary'] = result['Summary'].apply(process_summary_circle8)
                logging.info(f"Circle8 post-mapping: Processed Summary field to provide fallback text when empty")
        
        # FLEXVALUE POST-MAPPING PROCESSING
        if company_name == 'FlexValue_B.V.':
            # Process Summary field - remove everything before "opdrachtbeschrijving"
            if 'Summary' in result.columns:
                def process_summary_flexvalue(summary_str):
                    if pd.isna(summary_str) or summary_str == '':
                        return 'Not mentioned'
                    
                    summary_clean = str(summary_str).strip()
                    
                    # Find "opdrachtbeschrijving" and take everything after it
                    if 'opdrachtbeschrijving' in summary_clean.lower():
                        # Find the position of "opdrachtbeschrijving" (case insensitive)
                        import re
                        match = re.search(r'opdrachtbeschrijving', summary_clean, re.IGNORECASE)
                        if match:
                            # Take everything after the marker
                            summary_clean = summary_clean[match.end():].strip()
                    
                    return summary_clean if summary_clean else 'Not mentioned'
                
                result['Summary'] = result['Summary'].apply(process_summary_flexvalue)
                logging.info(f"FlexValue post-mapping: Processed Summary field to remove content before 'opdrachtbeschrijving'")
            
            # Process rate field - extract text between "Tarief" and "all-in"
            if 'rate' in result.columns:
                def process_rate_flexvalue(rate_str):
                    if pd.isna(rate_str) or rate_str == '':
                        return 'Not mentioned'
                    
                    rate_clean = str(rate_str).strip()
                    
                    # Find text between "Tarief" and "all-in"
                    import re
                    tarief_match = re.search(r'tarief', rate_clean, re.IGNORECASE)
                    allin_match = re.search(r'all-in', rate_clean, re.IGNORECASE)
                    
                    if tarief_match and allin_match and allin_match.start() > tarief_match.end():
                        # Extract text between the markers
                        extracted_text = rate_clean[tarief_match.end():allin_match.start()].strip()
                        return extracted_text if extracted_text else 'Not mentioned'
                    
                    return 'Not mentioned'
                
                result['rate'] = result['rate'].apply(process_rate_flexvalue)
                logging.info(f"FlexValue post-mapping: Processed rate field to extract text between 'Tarief' and 'all-in'")
            
            # Process Hours field - extract number after "Uren per week"
            if 'Hours' in result.columns:
                def process_hours_flexvalue(hours_str):
                    if pd.isna(hours_str) or hours_str == '':
                        return 'Not mentioned'
                    
                    hours_clean = str(hours_str).strip()
                    
                    # Find "Uren per week" and extract the number after it
                    import re
                    match = re.search(r'uren per week\s*(\d+)', hours_clean, re.IGNORECASE)
                    if match:
                        number = match.group(1)
                        return number
                    
                    return 'Not mentioned'
                
                result['Hours'] = result['Hours'].apply(process_hours_flexvalue)
                logging.info(f"FlexValue post-mapping: Processed Hours field to extract number after 'Uren per week'")
        
        # AMSTELVEENHUURTIN POST-MAPPING PROCESSING
        if company_name == 'Amstelveenhuurtin':
            # Process Title field - remove words in brackets and words starting with "SO" followed by a number
            if 'Title' in result.columns:
                def process_title_amstelveenhuurtin(title_str):
                    if pd.isna(title_str) or title_str == '':
                        return 'Not mentioned'
                    
                    # Convert to string and clean up
                    title_clean = str(title_str).strip()
                    
                    # Remove words in brackets (including nested brackets)
                    title_clean = re.sub(r'\([^)]*\)', '', title_clean)
                    
                    # Remove words that start with "SO" followed by a number
                    title_clean = re.sub(r'\bSO\d+\b', '', title_clean, flags=re.IGNORECASE)
                    
                    # Clean up extra spaces
                    title_clean = re.sub(r'\s+', ' ', title_clean).strip()
                    
                    # Return "Not mentioned" if empty after cleaning
                    if not title_clean:
                        return 'Not mentioned'
                    
                    return title_clean
                
                result['Title'] = result['Title'].apply(process_title_amstelveenhuurtin)
                logging.info(f"Amstelveenhuurtin post-mapping: Processed Title field to remove words in brackets and SO-number patterns")
            
            # Process Location field - extract text between "standplaats:" and "|"
            if 'Location' in result.columns:
                def process_location_amstelveenhuurtin(location_str):
                    if pd.isna(location_str) or location_str == '':
                        return 'Not mentioned'
                    
                    location_clean = str(location_str).strip()
                    
                    # Find text between "standplaats:" and "|"
                    import re
                    match = re.search(r'standplaats:\s*(.*?)\s*\|', location_clean, re.IGNORECASE)
                    if match:
                        extracted_text = match.group(1).strip()
                        return extracted_text if extracted_text else 'Not mentioned'
                    
                    return 'Not mentioned'
                
                result['Location'] = result['Location'].apply(process_location_amstelveenhuurtin)
                logging.info(f"Amstelveenhuurtin post-mapping: Processed Location field to extract text between 'standplaats:' and '|'")
            
            # Process Hours field - extract text between "Uren:" and "|"
            if 'Hours' in result.columns:
                def process_hours_amstelveenhuurtin(hours_str):
                    if pd.isna(hours_str) or hours_str == '':
                        return 'Not mentioned'
                    
                    hours_clean = str(hours_str).strip()
                    
                    # Find text between "Uren:" and "|"
                    import re
                    match = re.search(r'uren:\s*(.*?)\s*\|', hours_clean, re.IGNORECASE)
                    if match:
                        extracted_text = match.group(1).strip()
                        return extracted_text if extracted_text else 'Not mentioned'
                    
                    return 'Not mentioned'
                
                result['Hours'] = result['Hours'].apply(process_hours_amstelveenhuurtin)
                logging.info(f"Amstelveenhuurtin post-mapping: Processed Hours field to extract text between 'Uren:' and '|'")
            
            # Process start field - extract everything before "t/m"
            if 'start' in result.columns:
                def process_start_amstelveenhuurtin(start_str):
                    if pd.isna(start_str) or start_str == '':
                        return 'Not mentioned'
                    
                    start_clean = str(start_str).strip()
                    
                    # Find everything before "t/m" (case insensitive)
                    import re
                    match = re.search(r'^(.*?)\s*t/m', start_clean, re.IGNORECASE)
                    if match:
                        extracted_text = match.group(1).strip()
                        return extracted_text if extracted_text else 'Not mentioned'
                    
                    return start_clean if start_clean else 'Not mentioned'
                
                result['start'] = result['start'].apply(process_start_amstelveenhuurtin)
                logging.info(f"Amstelveenhuurtin post-mapping: Processed start field to extract everything before 't/m'")

        # Drop rows where Title or URL is empty
        before_drop = len(result)
        result = result[(result['Title'] != '') & (result['URL'] != '')]
        dropped = before_drop - len(result)
        if dropped > 0:
            logging.info(f"Dropped {dropped} rows with empty Title or URL for {company_name}")
        
        # Remove duplicates using standard columns
        initial_rows = len(result)
        result = result.drop_duplicates(subset=['Title', 'URL', 'Company'], keep='first')
        duplicates_removed = initial_rows - len(result)
        
        if duplicates_removed > 0:
            logging.info(f"Removed {duplicates_removed} duplicate entries from {company_name}")
        
        # Specific filtering for freelancer.com
        if company_name == 'freelancer.com':
            if 'Title' in result.columns:
                result = result[~result['Title'].str.contains('--', na=False)]
        
        # COMPANY-SPECIFIC POST-MAPPING PROCESSING
        if company_name == 'HintTech':
            # Process Hours field - remove "per week" and keep only numbers
            if 'Hours' in result.columns:
                def process_hours(hours_str):
                    if pd.isna(hours_str) or hours_str == '':
                        return 'Not mentioned'
                    # Remove "per week" (case insensitive) and extract numbers
                    hours_clean = str(hours_str).lower().replace('per week', '').replace('perweek', '').strip()
                    # Extract numbers using regex
                    import re
                    numbers = re.findall(r'\d+', hours_clean)
                    if numbers:
                        return numbers[0]  # Return the first number found
                    return 'Not mentioned'
                
                result['Hours'] = result['Hours'].apply(process_hours)
                logging.info(f"HintTech post-mapping: Processed Hours field to extract numbers and remove 'per week'")
            
            # Process Duration field - calculate difference between start and end dates
            if 'Duration' in result.columns:
                def process_duration(duration_str):
                    if pd.isna(duration_str) or duration_str == '':
                        return 'Not mentioned'
                    
                    try:
                        # Parse date range (assuming format like "2024-01-01 to 2024-06-30" or similar)
                        duration_clean = str(duration_str).strip()
                        
                        # Common separators for date ranges
                        separators = [' to ', ' - ', ' tot ', ' t/m ', ' until ', ' through ']
                        
                        for sep in separators:
                            if sep in duration_clean.lower():
                                parts = duration_clean.lower().split(sep)
                                if len(parts) == 2:
                                    start_date_str = parts[0].strip()
                                    end_date_str = parts[1].strip()
                                    
                                    # Try to parse dates with common formats
                                    from datetime import datetime
                                    date_formats = ['%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d']
                                    
                                    start_date = None
                                    end_date = None
                                    
                                    for fmt in date_formats:
                                        try:
                                            start_date = datetime.strptime(start_date_str, fmt)
                                            end_date = datetime.strptime(end_date_str, fmt)
                                            break
                                        except ValueError:
                                            continue
                                    
                                    if start_date and end_date:
                                        # Calculate difference in days
                                        diff_days = (end_date - start_date).days
                                        if diff_days > 0:
                                            # Convert to months/weeks if appropriate
                                            if diff_days >= 30:
                                                months = diff_days // 30
                                                return f"{months} months"
                                            elif diff_days >= 7:
                                                weeks = diff_days // 7
                                                return f"{weeks} weeks"
                                            else:
                                                return f"{diff_days} days"
                                    break
                        
                        return duration_clean  # Return original if can't parse
                    except Exception:
                        return 'Not mentioned'
                
                result['Duration'] = result['Duration'].apply(process_duration)
                logging.info(f"HintTech post-mapping: Processed Duration field to calculate date differences")
        
        # HAARLEMMERMEERHUURTIN POST-MAPPING PROCESSING
        if company_name == 'haarlemmermeerhuurtin':
            # Process rate field - remove "per uur" and keep only the amount
            if 'rate' in result.columns:
                def process_rate(rate_str):
                    if pd.isna(rate_str) or rate_str == '':
                        return 'Not mentioned'
                    # Remove "per uur" (case insensitive) and clean up
                    rate_clean = str(rate_str).lower().replace('per uur', '').replace('peruur', '').strip()
                    # Remove extra spaces and return
                    rate_clean = ' '.join(rate_clean.split())
                    return rate_clean if rate_clean else 'Not mentioned'
                
                result['rate'] = result['rate'].apply(process_rate)
                logging.info(f"haarlemmermeerhuurtin post-mapping: Processed rate field to remove 'per uur'")
            
            # Process Duration field - calculate difference between start and end dates
            if 'Duration' in result.columns:
                def process_duration_haarlemmermeer(duration_str):
                    if pd.isna(duration_str) or duration_str == '':
                        return 'Not mentioned'
                    
                    try:
                        # Parse date range (assuming format like "01-07-2025 t/m 01-01-2026" or similar)
                        duration_clean = str(duration_str).strip()
                        
                        # Common separators for date ranges (including Dutch separators)
                        separators = [' t/m ', ' to ', ' - ', ' tot ', ' until ', ' through ', ' tm ']
                        
                        for sep in separators:
                            if sep in duration_clean.lower():
                                parts = duration_clean.lower().split(sep)
                                if len(parts) == 2:
                                    start_date_str = parts[0].strip()
                                    end_date_str = parts[1].strip()
                                    
                                    # Try to parse dates with common formats
                                    from datetime import datetime
                                    date_formats = ['%d-%m-%Y', '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d']
                                    
                                    start_date = None
                                    end_date = None
                                    
                                    for fmt in date_formats:
                                        try:
                                            start_date = datetime.strptime(start_date_str, fmt)
                                            end_date = datetime.strptime(end_date_str, fmt)
                                            break
                                        except ValueError:
                                            continue
                                    
                                    if start_date and end_date:
                                        # Calculate difference in days
                                        diff_days = (end_date - start_date).days
                                        if diff_days > 0:
                                            # Convert to months/weeks if appropriate
                                            if diff_days >= 365:
                                                years = diff_days // 365
                                                months = (diff_days % 365) // 30
                                                if months > 0:
                                                    return f"{years} years {months} months"
                                                else:
                                                    return f"{years} years"
                                            elif diff_days >= 30:
                                                months = diff_days // 30
                                                return f"{months} months"
                                            elif diff_days >= 7:
                                                weeks = diff_days // 7
                                                return f"{weeks} weeks"
                                            else:
                                                return f"{diff_days} days"
                                    break
                        
                        return duration_clean  # Return original if can't parse
                    except Exception:
                        return 'Not mentioned'
                
                result['Duration'] = result['Duration'].apply(process_duration_haarlemmermeer)
                logging.info(f"haarlemmermeerhuurtin post-mapping: Processed Duration field to calculate date differences")
        
        # HOOFDKRAAN POST-MAPPING PROCESSING
        if company_name == 'hoofdkraan':
            # Process Location field - remove "Locatie:" prefix
            if 'Location' in result.columns:
                def process_location(location_str):
                    if pd.isna(location_str) or location_str == '':
                        return 'Not mentioned'
                    # Remove "Locatie:" (case insensitive) and clean up
                    location_clean = str(location_str).replace('Locatie:', '').replace('locatie:', '').strip()
                    # Remove extra spaces and return
                    location_clean = ' '.join(location_clean.split())
                    return location_clean if location_clean else 'Not mentioned'
                
                result['Location'] = result['Location'].apply(process_location)
                logging.info(f"hoofdkraan post-mapping: Processed Location field to remove 'Locatie:' prefix")
        
        # HARVEY NASH POST-MAPPING PROCESSING
        if company_name == 'Harvey Nash':
            # Process Title field - remove words in parentheses and words starting with "JP"
            if 'Title' in result.columns:
                def process_title(title_str):
                    if pd.isna(title_str) or title_str == '':
                        return 'Not mentioned'
                    
                    import re
                    title_clean = str(title_str)
                    
                    # Remove everything in parentheses (including the parentheses)
                    title_clean = re.sub(r'\([^)]*\)', '', title_clean)
                    
                    # Split into words and remove words starting with "JP" (case insensitive)
                    words = title_clean.split()
                    filtered_words = [word for word in words if not word.upper().startswith('JP')]
                    
                    # Join back and clean up extra spaces
                    title_clean = ' '.join(filtered_words).strip()
                    # Remove multiple spaces
                    title_clean = ' '.join(title_clean.split())
                    
                    return title_clean if title_clean else 'Not mentioned'
                
                result['Title'] = result['Title'].apply(process_title)
                logging.info(f"Harvey Nash post-mapping: Processed Title field to remove words in parentheses and words starting with 'JP'")
        
        # KVK POST-MAPPING PROCESSING
        if company_name == 'KVK':
            # Process Duration field - remove text and calculate date difference
            if 'Duration' in result.columns:
                def process_duration_kvk(duration_str):
                    if pd.isna(duration_str) or duration_str == '':
                        return 'Not mentioned'
                    
                    try:
                        # Remove the specific text
                        duration_clean = str(duration_str).replace('De looptijd van de opdracht is van', '').strip()
                        
                        # Extract dates using regex - look for date patterns
                        import re
                        from datetime import datetime
                        
                        # Common date patterns (DD-MM-YYYY, DD/MM/YYYY, YYYY-MM-DD, etc.)
                        date_patterns = [
                            r'\b(\d{1,2}[-/]\d{1,2}[-/]\d{4})\b',  # DD-MM-YYYY or DD/MM/YYYY
                            r'\b(\d{4}[-/]\d{1,2}[-/]\d{1,2})\b',  # YYYY-MM-DD or YYYY/MM/DD
                            r'\b(\d{1,2}\s+\w+\s+\d{4})\b',       # DD Month YYYY
                            r'\b(\w+\s+\d{1,2},?\s+\d{4})\b'      # Month DD, YYYY
                        ]
                        
                        found_dates = []
                        for pattern in date_patterns:
                            matches = re.findall(pattern, duration_clean)
                            found_dates.extend(matches)
                        
                        if len(found_dates) >= 2:
                            # Try to parse the first and last dates found
                            first_date_str = found_dates[0]
                            last_date_str = found_dates[-1]
                            
                            # Try different date formats
                            date_formats = [
                                '%d-%m-%Y', '%d/%m/%Y', '%Y-%m-%d', '%Y/%m/%d',
                                '%d %B %Y', '%d %b %Y', '%B %d, %Y', '%b %d, %Y',
                                '%B %d %Y', '%b %d %Y'
                            ]
                            
                            start_date = None
                            end_date = None
                            
                            for fmt in date_formats:
                                try:
                                    start_date = datetime.strptime(first_date_str, fmt)
                                    break
                                except ValueError:
                                    continue
                            
                            for fmt in date_formats:
                                try:
                                    end_date = datetime.strptime(last_date_str, fmt)
                                    break
                                except ValueError:
                                    continue
                            
                            if start_date and end_date:
                                # Calculate difference in days
                                diff_days = abs((end_date - start_date).days)
                                if diff_days > 0:
                                    # Convert to appropriate units
                                    if diff_days >= 365:
                                        years = diff_days // 365
                                        months = (diff_days % 365) // 30
                                        if months > 0:
                                            return f"{years} years {months} months"
                                        else:
                                            return f"{years} years"
                                    elif diff_days >= 30:
                                        months = diff_days // 30
                                        return f"{months} months"
                                    elif diff_days >= 7:
                                        weeks = diff_days // 7
                                        return f"{weeks} weeks"
                                    else:
                                        return f"{diff_days} days"
                        
                        return duration_clean  # Return cleaned text if can't parse dates
                    except Exception:
                        return 'Not mentioned'
                
                result['Duration'] = result['Duration'].apply(process_duration_kvk)
                logging.info(f"KVK post-mapping: Processed Duration field to remove text and calculate date differences")
            
            # Process rate field - remove "per uur"
            if 'rate' in result.columns:
                def process_rate_kvk(rate_str):
                    if pd.isna(rate_str) or rate_str == '':
                        return 'Not mentioned'
                    # Remove "per uur" (case insensitive) and clean up
                    rate_clean = str(rate_str).lower().replace('per uur', '').replace('peruur', '').strip()
                    # Remove extra spaces and return
                    rate_clean = ' '.join(rate_clean.split())
                    return rate_clean if rate_clean else 'Not mentioned'
                
                result['rate'] = result['rate'].apply(process_rate_kvk)
                logging.info(f"KVK post-mapping: Processed rate field to remove 'per uur'")
        
        # LINKEDIN POST-MAPPING PROCESSING
        if company_name == 'LinkedIn':
            # Process Summary field - remove all whitelines to make text more compact
            if 'Summary' in result.columns:
                def process_summary_linkedin(summary_str):
                    if pd.isna(summary_str) or summary_str == '':
                        return 'See Vacancy'
                    
                    # Convert to string and remove all types of line breaks and extra whitespace
                    summary_clean = str(summary_str)
                    
                    # Remove various types of line breaks and whitespace
                    summary_clean = summary_clean.replace('\n', ' ')  # Remove newlines
                    summary_clean = summary_clean.replace('\r', ' ')  # Remove carriage returns
                    summary_clean = summary_clean.replace('\t', ' ')  # Remove tabs
                    
                    # Remove multiple spaces and clean up
                    summary_clean = ' '.join(summary_clean.split())
                    
                    return summary_clean if summary_clean else 'See Vacancy'
                
                result['Summary'] = result['Summary'].apply(process_summary_linkedin)
                logging.info(f"LinkedIn post-mapping: Processed Summary field to remove whitelines and make text compact")
        
        # INDEED POST-MAPPING PROCESSING
        if company_name == 'indeed':
            # Process Location field - extract location from Field2 column
            if 'Location' in result.columns and 'Field2' in files_read.columns:
                def process_location_indeed(index):
                    import re
                    
                    # Get the Field2 value from the original input DataFrame
                    field2_val = files_read.iloc[index]['Field2'] if 'Field2' in files_read.columns else None
                    
                    if pd.isna(field2_val) or field2_val == '':
                        return 'Not mentioned'
                    
                    field2_str = str(field2_val)
                    
                    # Search for the exact word "Locatie" and extract everything after it until "&" marker
                    locatie_pattern = r'Locatie([^&]*)'
                    locatie_match = re.search(locatie_pattern, field2_str)
                    
                    if locatie_match:
                        location = locatie_match.group(1).strip()
                        return location if location else 'Not mentioned'
                    
                    return 'Not mentioned'
                
                # Apply the processing to each row
                result['Location'] = [process_location_indeed(i) for i in range(len(result))]
                logging.info(f"Indeed post-mapping: Processed Location field to extract location from Field2 column using 'Locatie' pattern")
            
            # Process Rate field - extract rate from Field2 column (only overwrite if a value is found)
            if 'rate' in result.columns and 'Field2' in files_read.columns:
                def process_rate_indeed(index):
                    import re
                    # Get the Field2 value from the original input DataFrame
                    field2_val = files_read.iloc[index]['Field2'] if 'Field2' in files_read.columns else None
                    if pd.isna(field2_val) or field2_val == '':
                        return None
                    field2_str = str(field2_val)
                    # Search for rate information in Field2 (look for patterns like "€", "EUR", "euro", etc.)
                    rate_patterns = [
                        r'€\s*(\d+(?:[.,]\d+)?)',        # € 50 or € 50,00
                        r'(\d+(?:[.,]\d+)?)\s*€',        # 50 € or 50,00 €
                        r'EUR\s*(\d+(?:[.,]\d+)?)',      # EUR 50
                        r'(\d+(?:[.,]\d+)?)\s*EUR',      # 50 EUR
                        r'euro\s*(\d+(?:[.,]\d+)?)',     # euro 50
                        r'(\d+(?:[.,]\d+)?)\s*euro',     # 50 euro
                        r'(\d+(?:[.,]\d+)?)\s*per\s*uur',   # 50 per uur
                        r'(\d+(?:[.,]\d+)?)\s*per\s*day',   # 50 per day
                        r'(\d+(?:[.,]\d+)?)\s*per\s*week',  # 50 per week
                        r'(\d+(?:[.,]\d+)?)\s*per\s*month'  # 50 per month
                    ]
                    for pattern in rate_patterns:
                        rate_match = re.search(pattern, field2_str, re.IGNORECASE)
                        if rate_match:
                            rate = rate_match.group(1).replace(',', '.')
                            return rate
                    return None
                # Compute extracted rates and only overwrite existing when found
                extracted_rates = [process_rate_indeed(i) for i in range(len(result))]
                for i, extracted in enumerate(extracted_rates):
                    if extracted is not None and extracted != '':
                        result.at[i, 'rate'] = extracted
                logging.info(f"Indeed post-mapping: Processed Rate field (conditional overwrite) from Field2 column")

            # Process Summary field - use Field2 but remove the first line
            if 'Summary' in result.columns:
                def process_summary_indeed(index):
                    try:
                        source_val = None
                        if 'Field2' in files_read.columns:
                            source_val = files_read.iloc[index]['Field2']
                        if pd.isna(source_val) or source_val == '':
                            source_val = result.iloc[index]['Summary'] if index < len(result) else ''
                        summary_str = str(source_val)
                        # Normalize line breaks and split
                        summary_str = summary_str.replace('\r\n', '\n').replace('\r', '\n')
                        lines = summary_str.split('\n')
                        if len(lines) <= 1:
                            cleaned = summary_str.strip()
                        else:
                            cleaned = ' '.join([ln.strip() for ln in lines[1:] if ln.strip()])
                        return cleaned if cleaned else 'See Vacancy'
                    except Exception:
                        return 'See Vacancy'

                result['Summary'] = [process_summary_indeed(i) for i in range(len(result))]
                logging.info(f"Indeed post-mapping: Processed Summary to drop first line from Field2")
        
        # SCHIPHOL POST-MAPPING PROCESSING
        if company_name == 'Schiphol':
            # Process rate field - check both Text2 and Text4, use whichever contains a number
            if 'rate' in result.columns and ('Text2' in files_read.columns or 'Text4' in files_read.columns):
                def process_rate_schiphol(index):
                    import re
                    
                    # Get values from both Text2 and Text4 columns if they exist
                    text2_val = files_read.iloc[index]['Text2'] if 'Text2' in files_read.columns else None
                    text4_val = files_read.iloc[index]['Text4'] if 'Text4' in files_read.columns else None
                    
                    # Function to extract numbers from text
                    def extract_number(text):
                        if pd.isna(text) or text == '':
                            return None
                        # Look for numbers (including decimals) in the text
                        number_match = re.search(r'\d+(?:\.\d+)?', str(text))
                        return number_match.group() if number_match else None
                    
                    # Check Text2 first
                    if text2_val is not None:
                        number_from_text2 = extract_number(text2_val)
                        if number_from_text2:
                            return str(text2_val)
                    
                    # Check Text4 if Text2 doesn't contain a number
                    if text4_val is not None:
                        number_from_text4 = extract_number(text4_val)
                        if number_from_text4:
                            return str(text4_val)
                    
                    return 'Not mentioned'
                
                # Apply the processing to each row
                result['rate'] = [process_rate_schiphol(i) for i in range(len(result))]
                logging.info(f"Schiphol post-mapping: Processed rate field to check both Text2 and Text4 columns")
        
        # WERKZOEKEN.NL POST-MAPPING PROCESSING
        if company_name == 'werkzoeken.nl':
            # Process Company field - remove everything before "• "
            if 'Company' in result.columns:
                def process_company_werkzoeken(company_str):
                    if pd.isna(company_str) or company_str == '':
                        return 'werkzoeken.nl'
                    
                    company_clean = str(company_str)
                    
                    # Find "• " and take everything after it
                    if '• ' in company_clean:
                        company_clean = company_clean.split('• ', 1)[1]  # Split on first occurrence and take second part
                    
                    # Clean up extra spaces
                    company_clean = company_clean.strip()
                    
                    return company_clean if company_clean else 'werkzoeken.nl'
                
                result['Company'] = result['Company'].apply(process_company_werkzoeken)
                logging.info(f"werkzoeken.nl post-mapping: Processed Company field to remove everything before '• '")
            
            # Remove entire rows where rate field contains "p/m" (indicates permanent employment, not freelance)
            if 'rate' in result.columns:
                initial_rows = len(result)
                # Filter out rows where rate contains "p/m" (case insensitive)
                result = result[~result['rate'].astype(str).str.contains('p/m', case=False, na=False)]
                removed_rows = initial_rows - len(result)
                if removed_rows > 0:
                    logging.info(f"werkzoeken.nl post-mapping: Removed {removed_rows} rows containing 'p/m' in rate field (permanent employment jobs)")
                else:
                    logging.info(f"werkzoeken.nl post-mapping: No rows contained 'p/m' in rate field")

        # OVERHEIDZZP POST-MAPPING PROCESSING
        if company_name == 'overheidzzp':
            # Hours: remove the words "per week"
            if 'Hours' in result.columns:
                def process_hours_overheidzzp(hours_str):
                    if pd.isna(hours_str) or hours_str == '':
                        return 'Not mentioned'
                    cleaned = str(hours_str).replace('per week', '').replace('Per week', '')
                    cleaned = ' '.join(cleaned.split())
                    return cleaned if cleaned else 'Not mentioned'
                result['Hours'] = result['Hours'].apply(process_hours_overheidzzp)
                logging.info(f"overheidzzp post-mapping: Processed Hours to remove 'per week'")

            # Duration: remove anything in brackets ()
            if 'Duration' in result.columns:
                def process_duration_overheidzzp(duration_str):
                    if pd.isna(duration_str) or duration_str == '':
                        return 'Not mentioned'
                    import re
                    cleaned = re.sub(r'\([^)]*\)', '', str(duration_str)).strip()
                    cleaned = ' '.join(cleaned.split())
                    return cleaned if cleaned else 'Not mentioned'
                result['Duration'] = result['Duration'].apply(process_duration_overheidzzp)
                logging.info(f"overheidzzp post-mapping: Processed Duration to remove bracketed text")

            # Summary: extract text between 'Over de opdracht\t' and 'Let op' (case-insensitive)
            if 'Summary' in result.columns:
                def process_summary_overheidzzp(index):
                    try:
                        source_val = None
                        if 'Text' in files_read.columns:
                            source_val = files_read.iloc[index]['Text']
                        if pd.isna(source_val) or source_val == '':
                            source_val = result.iloc[index]['Summary'] if index < len(result) else ''
                        s = str(source_val)
                        import re
                        # Normalize spaces and tabs
                        s_norm = s.replace('\r\n', '\n').replace('\r', '\n')
                        # Find markers case-insensitive
                        m1 = re.search(r'over de opdracht\t', s_norm, re.IGNORECASE)
                        m2 = re.search(r'document\.addEventListener', s_norm, re.IGNORECASE)
                        if m1 and m2 and m2.start() > m1.end():
                            mid = s_norm[m1.end():m2.start()].strip()
                            mid = ' '.join(mid.split())
                            return mid if mid else 'See Vacancy'
                        # Fallback: return original cleaned summary
                        fallback = ' '.join(s_norm.split())
                        return fallback if fallback else 'See Vacancy'
                    except Exception:
                        return 'See Vacancy'
                result['Summary'] = [process_summary_overheidzzp(i) for i in range(len(result))]
                logging.info(f"overheidzzp post-mapping: Extracted Summary between 'Over de opdracht' and 'document.addEventListener'")
        
        # UMC POST-MAPPING PROCESSING
        if company_name == 'UMC':
            # Process Hours field - remove "p.w." (per week)
            if 'Hours' in result.columns:
                def process_hours_umc(hours_str):
                    if pd.isna(hours_str) or hours_str == '':
                        return 'Not mentioned'
                    
                    hours_clean = str(hours_str)
                    
                    # Remove "p.w." (per week) - case insensitive
                    hours_clean = hours_clean.replace('p.w.', '').replace('P.W.', '').replace('P.w.', '').replace('p.W.', '')
                    
                    # Clean up extra spaces
                    hours_clean = hours_clean.strip()
                    
                    return hours_clean if hours_clean else 'Not mentioned'
                
                result['Hours'] = result['Hours'].apply(process_hours_umc)
                logging.info(f"UMC post-mapping: Processed Hours field to remove 'p.w.' (per week)")
        
        # YACHT POST-MAPPING PROCESSING
        if company_name == 'Yacht':
            # Process Duration field - remove "Voor" prefix
            if 'Duration' in result.columns:
                def process_duration_yacht(duration_str):
                    if pd.isna(duration_str) or duration_str == '':
                        return 'Not mentioned'
                    # Remove "Voor" (case insensitive) and clean up
                    duration_clean = str(duration_str).replace('Voor', '').replace('voor', '').strip()
                    # Remove extra spaces and return
                    duration_clean = ' '.join(duration_clean.split())
                    return duration_clean if duration_clean else 'Not mentioned'
                
                result['Duration'] = result['Duration'].apply(process_duration_yacht)
                logging.info(f"Yacht post-mapping: Processed Duration field to remove 'Voor' prefix")
        
        # ZZP OPDRACHTEN POST-MAPPING PROCESSING
        if company_name == 'zzp opdrachten':
            # Process rate field - remove "Max." prefix
            if 'rate' in result.columns:
                def process_rate_zzp(rate_str):
                    if pd.isna(rate_str) or rate_str == '':
                        return 'Not mentioned'
                    # Remove "Max." (case insensitive) and clean up
                    rate_clean = str(rate_str).replace('Max.', '').replace('max.', '').strip()
                    # Remove extra spaces and return
                    rate_clean = ' '.join(rate_clean.split())
                    return rate_clean if rate_clean else 'Not mentioned'
                
                result['rate'] = result['rate'].apply(process_rate_zzp)
                logging.info(f"zzp opdrachten post-mapping: Processed rate field to remove 'Max.' prefix")
            
            # Process Hours and Duration fields - split jobdetails4 by comma
            if 'Hours' in result.columns and 'Duration' in result.columns:
                def process_hours_duration_zzp(index):
                    # Get the original jobdetails4 value from the input DataFrame
                    jobdetails4_val = files_read.iloc[index]['jobdetails4'] if 'jobdetails4' in files_read.columns else None
                    
                    if pd.isna(jobdetails4_val) or jobdetails4_val == '':
                        return 'Not mentioned', 'Not mentioned'
                    
                    jobdetails4_str = str(jobdetails4_val).strip()
                    
                    # Split by comma
                    if ',' in jobdetails4_str:
                        parts = jobdetails4_str.split(',', 1)  # Split only on first comma
                        hours_part = parts[0].strip()
                        duration_part = parts[1].strip() if len(parts) > 1 else ''
                        
                        return hours_part if hours_part else 'Not mentioned', duration_part if duration_part else 'Not mentioned'
                    else:
                        # If no comma, use the whole value for hours
                        return jobdetails4_str if jobdetails4_str else 'Not mentioned', 'Not mentioned'
                
                # Apply the processing to each row
                processed_data = [process_hours_duration_zzp(i) for i in range(len(result))]
                hours_values = [item[0] for item in processed_data]
                duration_values = [item[1] for item in processed_data]
                
                result['Hours'] = hours_values
                result['Duration'] = duration_values
                logging.info(f"zzp opdrachten post-mapping: Processed Hours and Duration fields from jobdetails4 (split by comma)")
            
            # Process Summary field - remove white lines with no text
            if 'Summary' in result.columns:
                def process_summary_zzp(summary_str):
                    if pd.isna(summary_str) or summary_str == '':
                        return 'See Vacancy'
                    
                    # Convert to string and split by lines
                    summary_clean = str(summary_str)
                    
                    # Split by various line break types and filter out empty/whitespace-only lines
                    lines = summary_clean.replace('\r\n', '\n').replace('\r', '\n').split('\n')
                    filtered_lines = [line.strip() for line in lines if line.strip()]
                    
                    # Join the non-empty lines back together
                    summary_clean = ' '.join(filtered_lines)
                    
                    return summary_clean if summary_clean else 'See Vacancy'
                
                result['Summary'] = result['Summary'].apply(process_summary_zzp)
                logging.info(f"zzp opdrachten post-mapping: Processed Summary field to remove white lines with no text")
        
        # GEMEENTE PROJECTEN POST-MAPPING PROCESSING
        if company_name == 'gemeente-projecten':
            # Process Title field - remove text in () and the () themselves
            if 'Title' in result.columns:
                def process_title_gemeente(title_str):
                    if pd.isna(title_str) or title_str == '':
                        return 'Not mentioned'
                    
                    import re
                    title_clean = str(title_str)
                    
                    # Remove everything in parentheses (including the parentheses)
                    title_clean = re.sub(r'\([^)]*\)', '', title_clean)
                    
                    # Clean up extra spaces
                    title_clean = ' '.join(title_clean.split()).strip()
                    
                    return title_clean if title_clean else 'Not mentioned'
                
                result['Title'] = result['Title'].apply(process_title_gemeente)
                logging.info(f"gemeente-projecten post-mapping: Processed Title field to remove text in parentheses")
            
            # Process Location field - remove text in () and the () themselves
            if 'Location' in result.columns:
                def process_location_gemeente(location_str):
                    if pd.isna(location_str) or location_str == '':
                        return 'Not mentioned'
                    
                    import re
                    location_clean = str(location_str)
                    
                    # Remove everything in parentheses (including the parentheses)
                    location_clean = re.sub(r'\([^)]*\)', '', location_clean)
                    
                    # Clean up extra spaces
                    location_clean = ' '.join(location_clean.split()).strip()
                    
                    return location_clean if location_clean else 'Not mentioned'
                
                result['Location'] = result['Location'].apply(process_location_gemeente)
                logging.info(f"gemeente-projecten post-mapping: Processed Location field to remove text in parentheses")
            
            # Process rate field - extract euro amount from Field10 (e.g., "€50" or "€ 50,00").
            if 'rate' in result.columns:
                def process_rate_gemeente(rate_str):
                    if pd.isna(rate_str) or rate_str == '':
                        return 'Not mentioned'
                    import re
                    s = str(rate_str)
                    m = re.search(r'€\s*([0-9]+(?:[.,][0-9]{1,2})?)', s)
                    if m:
                        val = m.group(1).replace(',', '.')
                        return f"€ {val}"
                    return 'Not mentioned'
                result['rate'] = result['rate'].apply(process_rate_gemeente)
                logging.info(f"gemeente-projecten post-mapping: Extracted euro amount from Field10 into rate")

            # Process Duration field - remove the word "Voor" (case-insensitive) and clean up
            if 'Duration' in result.columns:
                def process_duration_gemeente(duration_str):
                    if pd.isna(duration_str) or duration_str == '':
                        return 'Not mentioned'
                    import re
                    cleaned = re.sub(r'\bvoor\b', '', str(duration_str), flags=re.IGNORECASE)
                    cleaned = ' '.join(cleaned.split()).strip()
                    return cleaned if cleaned else 'Not mentioned'
                result['Duration'] = result['Duration'].apply(process_duration_gemeente)
                logging.info(f"gemeente-projecten post-mapping: Removed 'Voor' from Duration")
        
        # GGDZWHUURTIN.NL POST-MAPPING PROCESSING
        if company_name == 'ggdzwhuurtin.nl':
            # Process Duration field - find 2 dates in DD-MM-YYYY format and calculate months between them
            if 'Duration' in result.columns:
                def process_duration_ggdz(duration_str):
                    if pd.isna(duration_str) or duration_str == '':
                        return 'Not mentioned'
                    
                    try:
                        import re
                        from datetime import datetime
                        
                        duration_clean = str(duration_str).strip()
                        
                        # Look for DD-MM-YYYY date patterns
                        date_pattern = r'(\d{1,2})-(\d{1,2})-(\d{4})'
                        dates_found = re.findall(date_pattern, duration_clean)
                        
                        if len(dates_found) >= 2:
                            # Get the first two dates
                            date1_parts = dates_found[0]
                            date2_parts = dates_found[1]
                            
                            try:
                                date1 = datetime(int(date1_parts[2]), int(date1_parts[1]), int(date1_parts[0]))
                                date2 = datetime(int(date2_parts[2]), int(date2_parts[1]), int(date2_parts[0]))
                                
                                # Calculate difference in months
                                months_diff = (date2.year - date1.year) * 12 + (date2.month - date1.month)
                                
                                if months_diff > 0:
                                    return f"{months_diff} months"
                                else:
                                    return "Less than 1 month"
                                    
                            except ValueError:
                                return duration_clean
                        else:
                            return duration_clean
                            
                    except Exception:
                        return 'Not mentioned'
                
                result['Duration'] = result['Duration'].apply(process_duration_ggdz)
                logging.info(f"ggdzwhuurtin.nl post-mapping: Processed Duration field to calculate months between DD-MM-YYYY dates")
            
            # Process start field - find first date in DD-MM-YYYY format
            if 'start' in result.columns:
                def process_start_ggdz(start_str):
                    if pd.isna(start_str) or start_str == '':
                        return 'ASAP'
                    
                    try:
                        import re
                        from datetime import datetime
                        
                        start_clean = str(start_str).strip()
                        
                        # Look for DD-MM-YYYY date pattern
                        date_pattern = r'(\d{1,2})-(\d{1,2})-(\d{4})'
                        date_match = re.search(date_pattern, start_clean)
                        
                        if date_match:
                            day, month, year = date_match.groups()
                            try:
                                date_obj = datetime(int(year), int(month), int(day))
                                return date_obj.strftime('%Y-%m-%d')
                            except ValueError:
                                return 'ASAP'
                        else:
                            return 'ASAP'
                            
                    except Exception:
                        return 'ASAP'
                
                result['start'] = result['start'].apply(process_start_ggdz)
                logging.info(f"ggdzwhuurtin.nl post-mapping: Processed start field to extract first DD-MM-YYYY date")
            
            # Process rate field - find currency symbol €
            if 'rate' in result.columns:
                def process_rate_ggdz(rate_str):
                    if pd.isna(rate_str) or rate_str == '':
                        return 'Not mentioned'
                    
                    try:
                        import re
                        rate_clean = str(rate_str).strip()
                        
                        # Look for currency pattern with € symbol
                        rate_match = re.search(r'€\s*(\d+(?:,\d+)?(?:\.\d+)?)', rate_clean)
                        if rate_match:
                            return rate_match.group(1)
                        
                        # Look for rate without € symbol but with numbers
                        number_match = re.search(r'(\d+(?:,\d+)?(?:\.\d+)?)', rate_clean)
                        if number_match:
                            return number_match.group(1)
                        
                        return 'Not mentioned'
                        
                    except Exception:
                        return 'Not mentioned'
                
                result['rate'] = result['rate'].apply(process_rate_ggdz)
                logging.info(f"ggdzwhuurtin.nl post-mapping: Processed rate field to extract currency from € symbol")
        
        # 4 FREELANCERS POST-MAPPING PROCESSING
        if company_name == '4-Freelancers.nl':
            # Process Title field - remove text in () and the () themselves
            if 'Title' in result.columns:
                def process_title_4freelancers(title_str):
                    if pd.isna(title_str) or title_str == '':
                        return 'Not mentioned'
                    
                    import re
                    title_clean = str(title_str)
                    
                    # Remove everything in parentheses (including the parentheses)
                    title_clean = re.sub(r'\([^)]*\)', '', title_clean)
                    
                    # Clean up extra spaces
                    title_clean = ' '.join(title_clean.split()).strip()
                    
                    return title_clean if title_clean else 'Not mentioned'
                
                result['Title'] = result['Title'].apply(process_title_4freelancers)
                logging.info(f"4 Freelancers post-mapping: Processed Title field to remove text in parentheses")

            # Pre-mapping-like filter: drop rows mentioning 'Detachering' in Text (case-insensitive)
            if 'Text' in files_read.columns and len(result) == len(files_read):
                try:
                    initial_count = len(result)
                    mask = ~files_read['Text'].astype(str).str.contains('Detachering', case=False, na=False)
                    result = result[mask.reset_index(drop=True)]
                    filtered = initial_count - len(result)
                    if filtered > 0:
                        logging.info(f"4-Freelancers.nl filter: removed {filtered} rows mentioning 'Detachering' in Text")
                except Exception:
                    pass
        
        # ASNBANK POST-MAPPING PROCESSING
        if company_name == 'ASNBank':
            # Process Text column to extract Location, Hours, and Rate
            if 'Location' in result.columns and 'Hours' in result.columns and 'rate' in result.columns:
                def process_text_asnbank(index):
                    # Get the original Text value from the input DataFrame
                    text_val = files_read.iloc[index]['Text'] if 'Text' in files_read.columns else None
                    
                    if pd.isna(text_val) or text_val == '':
                        return 'Not mentioned', 'Not mentioned', 'Not mentioned'
                    
                    text_str = str(text_val).strip()
                    
                    # Split on spaces
                    parts = text_str.split(' ')
                    
                    if len(parts) == 0:
                        return 'Not mentioned', 'Not mentioned', 'Not mentioned'
                    
                    # First part is Location
                    location = parts[0] if len(parts) > 0 else 'Not mentioned'
                    
                    # Find Hours (remove "uur")
                    hours = 'Not mentioned'
                    for part in parts:
                        if 'uur' in part.lower():
                            hours = part.lower().replace('uur', '').strip()
                            break
                    
                    # Find Rate (everything from "€" onwards)
                    rate = 'Not mentioned'
                    euro_found = False
                    rate_parts = []
                    for part in parts:
                        if '€' in part:
                            euro_found = True
                        if euro_found:
                            rate_parts.append(part)
                    
                    if rate_parts:
                        rate = ' '.join(rate_parts)
                    
                    return location, hours, rate
                
                # Apply the processing to each row
                processed_data = [process_text_asnbank(i) for i in range(len(result))]
                location_values = [item[0] for item in processed_data]
                hours_values = [item[1] for item in processed_data]
                rate_values = [item[2] for item in processed_data]
                
                result['Location'] = location_values
                result['Hours'] = hours_values
                result['rate'] = rate_values
                logging.info(f"ASNBank post-mapping: Processed Text field to extract Location, Hours, and Rate")
        
        # CENTRIC POST-MAPPING PROCESSING
        if company_name == 'Centric':
            # Process Title field - remove words starting with "OP-" and words in brackets
            if 'Title' in result.columns:
                def process_title_centric(title_str):
                    if pd.isna(title_str) or title_str == '':
                        return 'Not mentioned'
                    
                    import re
                    title_clean = str(title_str)
                    
                    # Remove words starting with "OP-"
                    title_clean = re.sub(r'\bOP-\w+\b', '', title_clean)
                    
                    # Remove words in brackets (including the brackets)
                    title_clean = re.sub(r'\([^)]*\)', '', title_clean)
                    
                    # Clean up extra spaces and return
                    title_clean = ' '.join(title_clean.split())
                    return title_clean if title_clean else 'Not mentioned'
                
                result['Title'] = result['Title'].apply(process_title_centric)
                logging.info(f"Centric post-mapping: Processed Title field to remove words starting with 'OP-' and words in brackets")
            
            # Process Hours field - extract text
            if 'Hours' in result.columns:
                def process_hours_centric(hours_str):
                    if pd.isna(hours_str) or hours_str == '':
                        return 'Not mentioned'
                    
                    hours_clean = str(hours_str).strip()
                    return hours_clean if hours_clean else 'Not mentioned'
                
                result['Hours'] = result['Hours'].apply(process_hours_centric)
                logging.info(f"Centric post-mapping: Processed Hours field to extract text")
            
            # Process Summary field - extract text
            if 'Summary' in result.columns:
                def process_summary_centric(summary_str):
                    if pd.isna(summary_str) or summary_str == '':
                        return 'Not mentioned'
                    
                    summary_clean = str(summary_str).strip()
                    return summary_clean if summary_clean else 'Not mentioned'
                
                result['Summary'] = result['Summary'].apply(process_summary_centric)
                logging.info(f"Centric post-mapping: Processed Summary field to extract text")
            
            # Process rate field - extract text between "maximum rate:" and "working distance" (case insensitive)
            if 'rate' in result.columns:
                def process_rate_centric(rate_str):
                    if pd.isna(rate_str) or rate_str == '':
                        return 'Not mentioned'
                    
                    rate_clean = str(rate_str).strip()
                    
                    # Find text between "maximum rate:" and "working distance" (case insensitive)
                    import re
                    max_rate_match = re.search(r'maximum rate:', rate_clean, re.IGNORECASE)
                    working_distance_match = re.search(r'working distance', rate_clean, re.IGNORECASE)
                    
                    if max_rate_match and working_distance_match and working_distance_match.start() > max_rate_match.end():
                        # Extract text between the markers
                        extracted_text = rate_clean[max_rate_match.end():working_distance_match.start()].strip()
                        return extracted_text if extracted_text else 'Not mentioned'
                    
                    return 'Not mentioned'
                
                result['rate'] = result['rate'].apply(process_rate_centric)
                logging.info(f"Centric post-mapping: Processed rate field to extract text between 'maximum rate:' and 'working distance'")
        
        # RIJKSWATERSTAAT POST-MAPPING PROCESSING
        if company_name == 'rijkswaterstaat':
            # Remove rows that contain specific markers in the Text column
            if 'Text' in result.columns:
                initial_count = len(result)
                
                # Create a mask to identify rows to keep (exclude rows with unwanted markers)
                mask = ~result['Text'].astype(str).str.contains(
                    r'ZZP niet toegestaan|geen ZZP|Detachering', 
                    case=False, 
                    na=False
                )
                
                # Apply the mask to filter out unwanted rows
                result = result[mask].reset_index(drop=True)
                
                removed_count = initial_count - len(result)
                if removed_count > 0:
                    logging.info(f"Rijkswaterstaat post-mapping: Removed {removed_count} rows containing 'ZZP niet toegestaan', 'geen ZZP', or 'Detachering' markers")
                else:
                    logging.info(f"Rijkswaterstaat post-mapping: No rows found with unwanted markers")
        
        # SALTA GROUP POST-MAPPING PROCESSING
        if company_name == 'Salta Group':
            # Process Title field - remove the first word "Freelance"
            if 'Title' in result.columns:
                def process_title_salta(title_str):
                    if pd.isna(title_str) or title_str == '':
                        return 'Not mentioned'
                    
                    title_clean = str(title_str).strip()
                    
                    # Remove only the first occurrence of "Freelance" (case insensitive)
                    words = title_clean.split()
                    if words and words[0].lower() == 'freelance':
                        title_clean = ' '.join(words[1:])
                    else:
                        # If "Freelance" is not the first word, look for it anywhere and remove only the first occurrence
                        title_lower = title_clean.lower()
                        freelance_index = title_lower.find('freelance')
                        if freelance_index != -1:
                            # Remove only the first occurrence of "Freelance"
                            before_freelance = title_clean[:freelance_index].strip()
                            after_freelance = title_clean[freelance_index + 9:].strip()  # 9 is length of "freelance"
                            title_clean = f"{before_freelance} {after_freelance}".strip()
                    
                    # Clean up extra spaces and return
                    title_clean = ' '.join(title_clean.split())
                    return title_clean if title_clean else 'Not mentioned'
                
                result['Title'] = result['Title'].apply(process_title_salta)
                logging.info(f"Salta Group post-mapping: Processed Title field to remove the first word 'Freelance'")
        
        # NOORDOOSTBRABANT POST-MAPPING PROCESSING
        if company_name == 'noordoostbrabant':
            # Process Duration field - calculate months between first and second date
            if 'Duration' in result.columns:
                def process_duration_noordoostbrabant(duration_str):
                    if pd.isna(duration_str) or duration_str == '':
                        return 'Not mentioned'
                    
                    try:
                        # Convert to string and clean up
                        duration_clean = str(duration_str).strip()
                        
                        # Look for date patterns (DD-MM-YYYY or DD/MM/YYYY)
                        date_patterns = [
                            r'(\d{1,2})[-/](\d{1,2})[-/](\d{4})',  # DD-MM-YYYY or DD/MM/YYYY
                            r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})',  # YYYY-MM-DD or YYYY/MM/DD
                        ]
                        
                        dates_found = []
                        for pattern in date_patterns:
                            matches = re.findall(pattern, duration_clean)
                            for match in matches:
                                if len(match) == 3:
                                    if len(match[0]) == 4:  # YYYY-MM-DD format
                                        year, month, day = match
                                    else:  # DD-MM-YYYY format
                                        day, month, year = match
                                    try:
                                        date_obj = datetime(int(year), int(month), int(day))
                                        dates_found.append(date_obj)
                                    except ValueError:
                                        continue
                        
                        # If we found at least 2 dates, calculate the difference in months
                        if len(dates_found) >= 2:
                            # Sort dates to ensure first date is earlier
                            dates_found.sort()
                            start_date = dates_found[0]
                            end_date = dates_found[1]
                            
                            # Calculate difference in months
                            months_diff = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)
                            
                            if months_diff > 0:
                                return f"{months_diff} months"
                            else:
                                return "Less than 1 month"
                        else:
                            return duration_clean
                            
                    except Exception as e:
                        logging.warning(f"Error processing noordoostbrabant duration '{duration_str}': {e}")
                        return duration_clean
                
                result['Duration'] = result['Duration'].apply(process_duration_noordoostbrabant)
                logging.info(f"noordoostbrabant post-mapping: Processed Duration field to calculate months between dates")
            
            # Process start field - extract the first date from Field11
            if 'start' in result.columns:
                def process_start_noordoostbrabant(start_str):
                    if pd.isna(start_str) or start_str == '':
                        return 'ASAP'
                    
                    try:
                        # Convert to string and clean up
                        start_clean = str(start_str).strip()
                        
                        # Look for date patterns (DD-MM-YYYY or DD/MM/YYYY)
                        date_patterns = [
                            r'(\d{1,2})[-/](\d{1,2})[-/](\d{4})',  # DD-MM-YYYY or DD/MM/YYYY
                            r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})',  # YYYY-MM-DD or YYYY/MM/DD
                        ]
                        
                        dates_found = []
                        for pattern in date_patterns:
                            matches = re.findall(pattern, start_clean)
                            for match in matches:
                                if len(match) == 3:
                                    if len(match[0]) == 4:  # YYYY-MM-DD format
                                        year, month, day = match
                                    else:  # DD-MM-YYYY format
                                        day, month, year = match
                                    try:
                                        date_obj = datetime(int(year), int(month), int(day))
                                        dates_found.append(date_obj)
                                    except ValueError:
                                        continue
                        
                        # Return the first date found, or ASAP if no dates
                        if dates_found:
                            # Sort dates and return the earliest one
                            dates_found.sort()
                            first_date = dates_found[0]
                            return first_date.strftime('%Y-%m-%d')
                        else:
                            return 'ASAP'
                            
                    except Exception as e:
                        logging.warning(f"Error processing noordoostbrabant start date '{start_str}': {e}")
                        return 'ASAP'
                
                result['start'] = result['start'].apply(process_start_noordoostbrabant)
                logging.info(f"noordoostbrabant post-mapping: Processed start field to extract first date from Text")
            
            # Process Hours field - extract number between "Uren:" and "|" from Location field
            if 'Hours' in result.columns and 'Location' in result.columns:
                def process_hours_noordoostbrabant(row):
                    location_str = row.get('Location', '')
                    if pd.isna(location_str) or location_str == '':
                        return 'Not mentioned'
                    
                    try:
                        import re
                        location_clean = str(location_str).strip()
                        
                        # Look for pattern "Uren:" followed by number and "|"
                        hours_match = re.search(r'uren:\s*(\d+)\s*\|', location_clean, re.IGNORECASE)
                        if hours_match:
                            return hours_match.group(1)
                        
                        return 'Not mentioned'
                        
                    except Exception:
                        return 'Not mentioned'
                
                result['Hours'] = result.apply(process_hours_noordoostbrabant, axis=1)
                logging.info(f"noordoostbrabant post-mapping: Processed Hours field to extract from Location using 'Uren:' marker")
        
        # GRONINGENHUURTIN POST-MAPPING PROCESSING
        if company_name == 'groningenhuurtin':
            # Process Duration field - calculate months between the two dates
            if 'Duration' in result.columns:
                def process_duration_groningenhuurtin(duration_str):
                    if pd.isna(duration_str) or duration_str == '':
                        return 'Not mentioned'
                    
                    try:
                        # Convert to string and clean up
                        duration_clean = str(duration_str).strip()
                        
                        # Look for date patterns (DD-MM-YYYY or DD/MM/YYYY)
                        date_patterns = [
                            r'(\d{1,2})[-/](\d{1,2})[-/](\d{4})',  # DD-MM-YYYY or DD/MM/YYYY
                            r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})',  # YYYY-MM-DD or YYYY/MM/DD
                        ]
                        
                        dates_found = []
                        for pattern in date_patterns:
                            matches = re.findall(pattern, duration_clean)
                            for match in matches:
                                if len(match) == 3:
                                    if len(match[0]) == 4:  # YYYY-MM-DD format
                                        year, month, day = match
                                    else:  # DD-MM-YYYY format
                                        day, month, year = match
                                    try:
                                        date_obj = datetime(int(year), int(month), int(day))
                                        dates_found.append(date_obj)
                                    except ValueError:
                                        continue
                        
                        # If we found at least 2 dates, calculate the difference in months
                        if len(dates_found) >= 2:
                            # Sort dates to ensure first date is earlier
                            dates_found.sort()
                            start_date = dates_found[0]
                            end_date = dates_found[1]
                            
                            # Calculate difference in months
                            months_diff = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)
                            
                            if months_diff > 0:
                                return f"{months_diff} months"
                            else:
                                return "Less than 1 month"
                        else:
                            return duration_clean
                            
                    except Exception as e:
                        logging.warning(f"Error processing groningenhuurtin duration '{duration_str}': {e}")
                        return duration_clean
                
                result['Duration'] = result['Duration'].apply(process_duration_groningenhuurtin)
                logging.info(f"groningenhuurtin post-mapping: Processed Duration field to calculate months between dates")
            
            # Process start field - extract the first date from Title5
            if 'start' in result.columns:
                def process_start_groningenhuurtin(start_str):
                    if pd.isna(start_str) or start_str == '':
                        return 'ASAP'
                    
                    try:
                        # Convert to string and clean up
                        start_clean = str(start_str).strip()
                        
                        # Look for date patterns (DD-MM-YYYY or DD/MM/YYYY)
                        date_patterns = [
                            r'(\d{1,2})[-/](\d{1,2})[-/](\d{4})',  # DD-MM-YYYY or DD/MM/YYYY
                            r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})',  # YYYY-MM-DD or YYYY/MM/DD
                        ]
                        
                        dates_found = []
                        for pattern in date_patterns:
                            matches = re.findall(pattern, start_clean)
                            for match in matches:
                                if len(match) == 3:
                                    if len(match[0]) == 4:  # YYYY-MM-DD format
                                        year, month, day = match
                                    else:  # DD-MM-YYYY format
                                        day, month, year = match
                                    try:
                                        date_obj = datetime(int(year), int(month), int(day))
                                        dates_found.append(date_obj)
                                    except ValueError:
                                        continue
                        
                        # Return the first date found, or ASAP if no dates
                        if dates_found:
                            # Sort dates and return the earliest one
                            dates_found.sort()
                            first_date = dates_found[0]
                            return first_date.strftime('%Y-%m-%d')
                        else:
                            return 'ASAP'
                            
                    except Exception as e:
                        logging.warning(f"Error processing groningenhuurtin start date '{start_str}': {e}")
                        return 'ASAP'
                
                result['start'] = result['start'].apply(process_start_groningenhuurtin)
                logging.info(f"groningenhuurtin post-mapping: Processed start field to extract first date from Title5")
            
            # Process Location field - extract text between "Standplaats:" and "|"
            if 'Location' in result.columns:
                def process_location_groningenhuurtin(location_str):
                    if pd.isna(location_str) or location_str == '':
                        return 'Not mentioned'
                    
                    location_clean = str(location_str).strip()
                    
                    # Find text between "Standplaats:" and "|"
                    import re
                    match = re.search(r'standplaats:\s*(.*?)\s*\|', location_clean, re.IGNORECASE)
                    if match:
                        extracted_text = match.group(1).strip()
                        return extracted_text if extracted_text else 'Not mentioned'
                    
                    return 'Not mentioned'
                
                result['Location'] = result['Location'].apply(process_location_groningenhuurtin)
                logging.info(f"groningenhuurtin post-mapping: Processed Location field to extract text between 'Standplaats:' and '|'")
            
            # Process Hours field - extract number between "uren" and "|"
            if 'Hours' in result.columns:
                def process_hours_groningenhuurtin(hours_str):
                    if pd.isna(hours_str) or hours_str == '':
                        return 'Not mentioned'
                    
                    hours_clean = str(hours_str).strip()
                    
                    # Find number between "uren" and "|"
                    import re
                    match = re.search(r'uren\s*(\d+)\s*\|', hours_clean, re.IGNORECASE)
                    if match:
                        number = match.group(1)
                        return number
                    
                    return 'Not mentioned'
                
                result['Hours'] = result['Hours'].apply(process_hours_groningenhuurtin)
                logging.info(f"groningenhuurtin post-mapping: Processed Hours field to extract number between 'uren' and '|'")
            
            # Process rate field - extract number after "€"
            if 'rate' in result.columns:
                def process_rate_groningenhuurtin(rate_str):
                    if pd.isna(rate_str) or rate_str == '':
                        return 'Not mentioned'
                    
                    rate_clean = str(rate_str).strip()
                    
                    # Find number after "€"
                    import re
                    match = re.search(r'€\s*(\d+(?:,\d+)?)', rate_clean, re.IGNORECASE)
                    if match:
                        number = match.group(1)
                        # Remove commas from numbers like "1,500"
                        number = number.replace(',', '')
                        return number
                    
                    return 'Not mentioned'
                
                result['rate'] = result['rate'].apply(process_rate_groningenhuurtin)
                logging.info(f"groningenhuurtin post-mapping: Processed rate field to extract number after '€'")
        
        # FLEVOLAND POST-MAPPING PROCESSING
        if company_name == 'flevoland':
            # Process Location field - remove the word "location" (case insensitive)
            if 'Location' in result.columns:
                def process_location_flevoland(location_str):
                    if pd.isna(location_str) or location_str == '':
                        return 'Not mentioned'
                    
                    # Convert to string and remove "location" (case insensitive)
                    location_clean = str(location_str)
                    location_clean = re.sub(r'\blocation\b', '', location_clean, flags=re.IGNORECASE)
                    
                    # Clean up extra spaces
                    location_clean = re.sub(r'\s+', ' ', location_clean).strip()
                    
                    # Return "Not mentioned" if empty after cleaning
                    if not location_clean:
                        return 'Not mentioned'
                    
                    return location_clean
                
                result['Location'] = result['Location'].apply(process_location_flevoland)
                logging.info(f"flevoland post-mapping: Processed Location field to remove 'location' word")
            
            # Process start field - remove the words "start date" (case insensitive)
            if 'start' in result.columns:
                def process_start_flevoland(start_str):
                    if pd.isna(start_str) or start_str == '':
                        return 'ASAP'
                    
                    # Convert to string and remove "start date" (case insensitive)
                    start_clean = str(start_str)
                    start_clean = re.sub(r'\bstart date\b', '', start_clean, flags=re.IGNORECASE)
                    
                    # Clean up extra spaces
                    start_clean = re.sub(r'\s+', ' ', start_clean).strip()
                    
                    # Return "ASAP" if empty after cleaning
                    if not start_clean:
                        return 'ASAP'
                    
                    return start_clean
                
                result['start'] = result['start'].apply(process_start_flevoland)
                logging.info(f"flevoland post-mapping: Processed start field to remove 'start date' words")
            
            # Process Hours field - remove the word "hours" (case insensitive)
            if 'Hours' in result.columns:
                def process_hours_flevoland(hours_str):
                    if pd.isna(hours_str) or hours_str == '':
                        return 'Not mentioned'
                    
                    # Convert to string and remove "hours" (case insensitive)
                    hours_clean = str(hours_str)
                    hours_clean = re.sub(r'\bhours\b', '', hours_clean, flags=re.IGNORECASE)
                    
                    # Clean up extra spaces
                    hours_clean = re.sub(r'\s+', ' ', hours_clean).strip()
                    
                    # Return "Not mentioned" if empty after cleaning
                    if not hours_clean:
                        return 'Not mentioned'
                    
                    return hours_clean
                
                result['Hours'] = result['Hours'].apply(process_hours_flevoland)
                logging.info(f"flevoland post-mapping: Processed Hours field to remove 'hours' word")
            
            # Process Summary field - merge Field7 and Field8
            if 'Summary' in result.columns:
                def process_summary_flevoland(row):
                    # Get Field7 and Field8 values from the original data
                    field7_value = row.get('Field7', '') if 'Field7' in row else ''
                    field8_value = row.get('Field8', '') if 'Field8' in row else ''
                    
                    # Convert to strings and clean up
                    field7_clean = str(field7_value).strip() if field7_value else ''
                    field8_clean = str(field8_value).strip() if field8_value else ''
                    
                    # Combine the fields with a space separator
                    if field7_clean and field8_clean:
                        combined_summary = f"{field7_clean} {field8_clean}"
                    elif field7_clean:
                        combined_summary = field7_clean
                    elif field8_clean:
                        combined_summary = field8_clean
                    else:
                        combined_summary = 'Not mentioned'
                    
                    # Clean up extra spaces
                    combined_summary = re.sub(r'\s+', ' ', combined_summary).strip()
                    
                    return combined_summary if combined_summary else 'Not mentioned'
                
                result['Summary'] = result.apply(process_summary_flevoland, axis=1)
                logging.info(f"flevoland post-mapping: Processed Summary field to merge Field7 and Field8")
        
        # TALENTENREGIO POST-MAPPING PROCESSING
        if company_name == 'TalentenRegio':
            # Process Summary field - merge Text4 and Text5
            if 'Summary' in result.columns:
                def process_summary_talentenregio(row):
                    # Get Text4 and Text5 values from the original data
                    text4_value = row.get('Text4', '') if 'Text4' in row else ''
                    text5_value = row.get('Text5', '') if 'Text5' in row else ''
                    
                    # Convert to strings and clean up
                    text4_clean = str(text4_value).strip() if text4_value else ''
                    text5_clean = str(text5_value).strip() if text5_value else ''
                    
                    # Combine the fields with a space separator
                    if text4_clean and text5_clean:
                        combined_summary = f"{text4_clean} {text5_clean}"
                    elif text4_clean:
                        combined_summary = text4_clean
                    elif text5_clean:
                        combined_summary = text5_clean
                    else:
                        combined_summary = 'Not mentioned'
                    
                    # Clean up extra spaces
                    combined_summary = re.sub(r'\s+', ' ', combined_summary).strip()
                    
                    return combined_summary if combined_summary else 'Not mentioned'
                
                result['Summary'] = result.apply(process_summary_talentenregio, axis=1)
                logging.info(f"TalentenRegio post-mapping: Processed Summary field to merge Text4 and Text5")
        
        # POLITIE POST-MAPPING PROCESSING
        if company_name == 'politie':
            # Process Summary field - merge Text5 and Text6
            if 'Summary' in result.columns:
                def process_summary_politie(row):
                    # Get Text5 and Text6 values from the original data
                    text5_value = row.get('Text5', '') if 'Text5' in row else ''
                    text6_value = row.get('Text6', '') if 'Text6' in row else ''
                    
                    # Convert to strings and clean up
                    text5_clean = str(text5_value).strip() if text5_value else ''
                    text6_clean = str(text6_value).strip() if text6_value else ''
                    
                    # Combine the fields with a space separator
                    if text5_clean and text6_clean:
                        combined_summary = f"{text5_clean} {text6_clean}"
                    elif text5_clean:
                        combined_summary = text5_clean
                    elif text6_clean:
                        combined_summary = text6_clean
                    else:
                        combined_summary = 'Not mentioned'
                    
                    # Clean up extra spaces
                    combined_summary = re.sub(r'\s+', ' ', combined_summary).strip()
                    
                    return combined_summary if combined_summary else 'Not mentioned'
                
                result['Summary'] = result.apply(process_summary_politie, axis=1)
                logging.info(f"Politie post-mapping: Processed Summary field to merge Text5 and Text6")
        

        
        # NOORD-HOLLAND POST-MAPPING PROCESSING
        if company_name == 'Noord-Holland':
            # Process Duration field - calculate months between the first and second date from Title5
            if 'Duration' in result.columns:
                def process_duration_noordholland(duration_str):
                    if pd.isna(duration_str) or duration_str == '':
                        return 'Not mentioned'
                    
                    try:
                        # Convert to string and clean up
                        duration_clean = str(duration_str).strip()
                        
                        # Look for date patterns (DD-MM-YYYY or DD/MM/YYYY)
                        date_patterns = [
                            r'(\d{1,2})[-/](\d{1,2})[-/](\d{4})',  # DD-MM-YYYY or DD/MM/YYYY
                            r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})',  # YYYY-MM-DD or YYYY/MM/DD
                        ]
                        
                        dates_found = []
                        for pattern in date_patterns:
                            matches = re.findall(pattern, duration_clean)
                            for match in matches:
                                if len(match) == 3:
                                    if len(match[0]) == 4:  # YYYY-MM-DD format
                                        year, month, day = match
                                    else:  # DD-MM-YYYY format
                                        day, month, year = match
                                    try:
                                        date_obj = datetime(int(year), int(month), int(day))
                                        dates_found.append(date_obj)
                                    except ValueError:
                                        continue
                        
                        # If we found at least 2 dates, calculate the difference in months
                        if len(dates_found) >= 2:
                            # Sort dates to ensure first date is earlier
                            dates_found.sort()
                            start_date = dates_found[0]
                            end_date = dates_found[1]
                            
                            # Calculate difference in months
                            months_diff = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)
                            
                            if months_diff > 0:
                                return f"{months_diff} months"
                            else:
                                return "Less than 1 month"
                        else:
                            return duration_clean
                            
                    except Exception as e:
                        logging.warning(f"Error processing Noord-Holland duration '{duration_str}': {e}")
                        return duration_clean
                
                result['Duration'] = result['Duration'].apply(process_duration_noordholland)
                logging.info(f"Noord-Holland post-mapping: Processed Duration field to calculate months between dates from Title5")
            
            # Process start field - extract the first date from Title5
            if 'start' in result.columns:
                def process_start_noordholland(start_str):
                    if pd.isna(start_str) or start_str == '':
                        return 'ASAP'
                    
                    try:
                        # Convert to string and clean up
                        start_clean = str(start_str).strip()
                        
                        # Look for date patterns (DD-MM-YYYY or DD/MM/YYYY)
                        date_patterns = [
                            r'(\d{1,2})[-/](\d{1,2})[-/](\d{4})',  # DD-MM-YYYY or DD/MM/YYYY
                            r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})',  # YYYY-MM-DD or YYYY/MM/DD
                        ]
                        
                        dates_found = []
                        for pattern in date_patterns:
                            matches = re.findall(pattern, start_clean)
                            for match in matches:
                                if len(match) == 3:
                                    if len(match[0]) == 4:  # YYYY-MM-DD format
                                        year, month, day = match
                                    else:  # DD-MM-YYYY format
                                        day, month, year = match
                                    try:
                                        date_obj = datetime(int(year), int(month), int(day))
                                        dates_found.append(date_obj)
                                    except ValueError:
                                        continue
                        
                        # Return the first date found, or ASAP if no dates
                        if dates_found:
                            # Sort dates and return the earliest one
                            dates_found.sort()
                            first_date = dates_found[0]
                            return first_date.strftime('%Y-%m-%d')
                        else:
                            return 'ASAP'
                            
                    except Exception as e:
                        logging.warning(f"Error processing Noord-Holland start date '{start_str}': {e}")
                        return 'ASAP'
                
                result['start'] = result['start'].apply(process_start_noordholland)
                logging.info(f"Noord-Holland post-mapping: Processed start field to extract first date from Title5")
        
        # HAARLEMMERMEERHUURTIN POST-MAPPING PROCESSING
        if company_name == 'haarlemmermeerhuurtin':
            # Process Duration field - extract from Summary field
            if 'Duration' in result.columns and 'Summary' in result.columns:
                def process_duration_from_summary(row):
                    summary_str = row.get('Summary', '')
                    if pd.isna(summary_str) or summary_str == '':
                        return 'Not mentioned'
                    
                    try:
                        # Convert to string and clean up
                        summary_clean = str(summary_str).strip()
                        
                        # Look for date patterns (DD-MM-YYYY or DD/MM/YYYY)
                        date_patterns = [
                            r'(\d{1,2})[-/](\d{1,2})[-/](\d{4})',  # DD-MM-YYYY or DD/MM/YYYY
                            r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})',  # YYYY-MM-DD or YYYY/MM/DD
                        ]
                        
                        dates_found = []
                        for pattern in date_patterns:
                            matches = re.findall(pattern, summary_clean)
                            for match in matches:
                                if len(match) == 3:
                                    if len(match[0]) == 4:  # YYYY-MM-DD format
                                        year, month, day = match
                                    else:  # DD-MM-YYYY format
                                        day, month, year = match
                                    try:
                                        date_obj = datetime(int(year), int(month), int(day))
                                        dates_found.append(date_obj)
                                    except ValueError:
                                        continue
                        
                        # If we found at least 2 dates, calculate the difference in months
                        if len(dates_found) >= 2:
                            # Sort dates to ensure first date is earlier
                            dates_found.sort()
                            start_date = dates_found[0]
                            end_date = dates_found[1]
                            
                            # Calculate difference in months
                            months_diff = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)
                            
                            if months_diff > 0:
                                return f"{months_diff} months"
                            else:
                                return "Less than 1 month"
                        else:
                            return 'Not mentioned'
                            
                    except Exception as e:
                        logging.warning(f"Error processing haarlemmermeerhuurtin duration from summary '{summary_str}': {e}")
                        return 'Not mentioned'
                
                result['Duration'] = result.apply(process_duration_from_summary, axis=1)
                logging.info(f"haarlemmermeerhuurtin post-mapping: Processed Duration field to extract from Summary")
            
            # Process fields that are set to "See Summary" - extract from Pub_Time field
            if 'rate' in result.columns and 'Summary' in result.columns:
                def process_rate_from_summary(row):
                    summary_str = row.get('Summary', '')
                    if pd.isna(summary_str) or summary_str == '':
                        return 'Not mentioned'
                    
                    summary_clean = str(summary_str).strip()
                    # Look for rate patterns in the summary
                    import re
                    # Look for currency patterns like €50, €50.00, 50€, etc.
                    rate_match = re.search(r'€\s*(\d+(?:,\d+)?(?:\.\d+)?)', summary_clean, re.IGNORECASE)
                    if rate_match:
                        return rate_match.group(1)
                    
                    # Look for "per uur" patterns
                    per_uur_match = re.search(r'(\d+(?:,\d+)?(?:\.\d+)?)\s*per\s*uur', summary_clean, re.IGNORECASE)
                    if per_uur_match:
                        return per_uur_match.group(1)
                    
                    return 'Not mentioned'
                
                result['rate'] = result.apply(process_rate_from_summary, axis=1)
                logging.info(f"haarlemmermeerhuurtin post-mapping: Processed rate field to extract from Summary")
            
            if 'Hours' in result.columns and 'Summary' in result.columns:
                def process_hours_from_summary(row):
                    summary_str = row.get('Summary', '')
                    if pd.isna(summary_str) or summary_str == '':
                        return 'Not mentioned'
                    
                    summary_clean = str(summary_str).strip()
                    # Look for hours patterns in the summary
                    import re
                    # Look for patterns like "40 uur", "40 uren", "40h", etc.
                    hours_match = re.search(r'(\d+)\s*(?:uur|uren|h|hours?)', summary_clean, re.IGNORECASE)
                    if hours_match:
                        return hours_match.group(1)
                    
                    return 'Not mentioned'
                
                result['Hours'] = result.apply(process_hours_from_summary, axis=1)
                logging.info(f"haarlemmermeerhuurtin post-mapping: Processed Hours field to extract from Summary")
            
            # Process start field - extract from Summary field
            if 'start' in result.columns and 'Summary' in result.columns:
                def process_start_from_summary(row):
                    summary_str = row.get('Summary', '')
                    if pd.isna(summary_str) or summary_str == '':
                        return 'ASAP'
                    
                    try:
                        # Convert to string and clean up
                        summary_clean = str(summary_str).strip()
                        
                        # Look for date patterns (DD-MM-YYYY or DD/MM/YYYY)
                        date_patterns = [
                            r'(\d{1,2})[-/](\d{1,2})[-/](\d{4})',  # DD-MM-YYYY or DD/MM/YYYY
                            r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})',  # YYYY-MM-DD or YYYY/MM/DD
                        ]
                        
                        dates_found = []
                        for pattern in date_patterns:
                            matches = re.findall(pattern, summary_clean)
                            for match in matches:
                                if len(match) == 3:
                                    if len(match[0]) == 4:  # YYYY-MM-DD format
                                        year, month, day = match
                                    else:  # DD-MM-YYYY format
                                        day, month, year = match
                                    try:
                                        date_obj = datetime(int(year), int(month), int(day))
                                        dates_found.append(date_obj)
                                    except ValueError:
                                        continue
                        
                        # Return the first date found, or ASAP if no dates
                        if dates_found:
                            # Sort dates and return the earliest one
                            dates_found.sort()
                            first_date = dates_found[0]
                            return first_date.strftime('%Y-%m-%d')
                        else:
                            return 'ASAP'
                            
                    except Exception as e:
                        logging.warning(f"Error processing Haarlemmermeerhuurtin start date from summary '{summary_str}': {e}")
                        return 'ASAP'
                
                result['start'] = result.apply(process_start_from_summary, axis=1)
                logging.info(f"haarlemmermeerhuurtin post-mapping: Processed start field to extract from Summary")
        

        

        
        # LINKIT POST-MAPPING PROCESSING
        elif company_name == 'LinkIT':
            if 'Duration' in result.columns:
                def process_duration_linkit(duration_value):
                    """Process LinkIT Duration field to check for 'months' marker"""
                    if pd.isna(duration_value) or duration_value == '':
                        return 'Not mentioned'
                    
                    duration_str = str(duration_value).lower()
                    if 'months' in duration_str:
                        return duration_str
                    else:
                        return 'Not mentioned'
                
                result['Duration'] = result['Duration'].apply(process_duration_linkit)
                logging.info(f"LinkIT post-mapping: Processed Duration field to check for 'months' marker")
        
        # Remove validation and cleaning logic - just return the processed data
        return result
    
    except Exception as e:
        logging.error(f"Error processing {company_name}: {e}")
        return pd.DataFrame()

def get_existing_records(table_name):
    """
    Get all existing records from Supabase table, handling pagination.
    Returns a DataFrame with the records.
    """
    all_records = []
    offset = 0
    page_size = 1000  # Supabase default limit, adjust if known to be different
    
    logging.info(f"Fetching existing records from {table_name} with pagination...")
    while True:
        try:
            # logging.debug(f"Fetching records from {table_name} with limit={page_size}, offset={offset}")
            response = supabase.table(table_name).select("*", count='exact').limit(page_size).offset(offset).execute()
            
            if hasattr(response, 'data') and response.data:
                all_records.extend(response.data)
                # logging.debug(f"Fetched {len(response.data)} records in this page. Total fetched so far: {len(all_records)}.")
                if len(response.data) < page_size:
                    # Last page fetched
                    break
                offset += len(response.data) # More robust than assuming page_size, in case less than page_size is returned before the end
            else:
                # No more data or error
                break
        except Exception as e:
            logging.error(f"Error fetching page of existing records from {table_name} (offset {offset}): {e}")
            # Depending on desired robustness, you might want to break or retry
            break 
            
    if not all_records:
        logging.info(f"No existing records found in {table_name}.")
        return pd.DataFrame()
    
    total_count_from_header = 0
    if hasattr(response, 'count') and response.count is not None:
        total_count_from_header = response.count
        logging.info(f"Successfully fetched {len(all_records)} records from {table_name}. Server reported total: {total_count_from_header}.")
        if len(all_records) != total_count_from_header:
            logging.warning(f"Mismatch between fetched records ({len(all_records)}) and server reported total ({total_count_from_header}) for {table_name}.")
    else:
        logging.info(f"Successfully fetched {len(all_records)} records from {table_name}. Server did not report a total count.")
        
    return pd.DataFrame(all_records)

def prepare_data_for_upload(df, historical_data=None):
    """
    Prepare DataFrame for upload by adding date, unique ID, and group ID.
    If historical_data is provided, preserves dates for existing records.
    """
    # Add UNIQUE_ID, group_id and date columns
    df['UNIQUE_ID'] = df.apply(
        lambda row: generate_unique_id(row['Title'], row['URL'], row['Company']),
        axis=1
    )
    df['group_id'] = df.apply(
        lambda row: generate_group_id(row['Title']),
        axis=1
    )
    
    # Add new ID columns for Location, Hours, and Duration
    logging.info("Generating additional ID columns...")
    df['location_id'] = df.apply(
        lambda row: generate_location_id(row['Location'], is_from_input_value(row['Location'])),
        axis=1
    )
    df['hours_id'] = df.apply(
        lambda row: generate_hours_id(row['Hours'], is_from_input_value(row['Hours'])),
        axis=1
    )
    df['duration_id'] = df.apply(
        lambda row: generate_duration_id(row['Duration'], is_from_input_value(row['Duration'])),
        axis=1
    )
    df['summary_id'] = df.apply(
        lambda row: generate_summary_id(row['Summary'], is_from_input_value(row['Summary'])),
        axis=1
    )
    df['source_id'] = df.apply(
        lambda row: generate_source_id(row['Source'] if 'Source' in row else row['Company'], is_from_input_value(row['Source'] if 'Source' in row else row['Company'])),
        axis=1
    )
    
    # Generate true_duplicates ID (source + group + summary + company)
    # This identifies jobs that are truly identical: same title, same skills, from same source and company
    logging.info("Generating true_duplicates ID...")
    df['true_duplicates'] = df.apply(
        lambda row: hashlib.md5(f"{row['source_id']}_{row['group_id']}_{row['summary_id']}_{row['Company']}".encode()).hexdigest(),
        axis=1
    )
    
    # Generate similarity matching IDs
    logging.info("Generating similarity matching IDs...")
    
    # Cross-platform duplicates: same title + same skills + same company (recruiters reposting same vacancy across platforms)
    df['cross_platform_duplicates'] = df.apply(
        lambda row: hashlib.md5(f"{row['group_id']}_{row['summary_id']}_{row['Company']}".encode()).hexdigest(),
        axis=1
    )
    
    # Location clusters: same title + same location (jobs in same area with same role)
    df['location_clusters'] = df.apply(
        lambda row: hashlib.md5(f"{row['group_id']}_{row['location_id']}".encode()).hexdigest(),
        axis=1
    )
    
    # Recommendations: same skills + same location (you might also be interested in this)
    df['recommendations'] = df.apply(
        lambda row: hashlib.md5(f"{row['summary_id']}_{row['location_id']}".encode()).hexdigest(),
        axis=1
    )
    
    # Company location roles: same title + same source + same location (distinguish between companies posting same job in same location)
    df['company_location_roles'] = df.apply(
        lambda row: hashlib.md5(f"{row['group_id']}_{row['source_id']}_{row['location_id']}".encode()).hexdigest(),
        axis=1
    )
    
    # Prepare ID generation results for table display
    id_results = [
        {
            'id_type': 'Location ID',
            'generated_count': len(df),
            'from_input_count': df['location_id'].apply(lambda x: is_from_input_value(x)).sum(),
            'from_historical_count': len(df) - df['location_id'].apply(lambda x: is_from_input_value(x)).sum(),
            'collision_count': len(df) - df['location_id'].nunique(),
            'success_pct': (df['location_id'].nunique() / len(df) * 100) if len(df) > 0 else 0
        },
        {
            'id_type': 'Hours ID',
            'generated_count': len(df),
            'from_input_count': df['hours_id'].apply(lambda x: is_from_input_value(x)).sum(),
            'from_historical_count': len(df) - df['hours_id'].apply(lambda x: is_from_input_value(x)).sum(),
            'collision_count': len(df) - df['hours_id'].nunique(),
            'success_pct': (df['hours_id'].nunique() / len(df) * 100) if len(df) > 0 else 0
        },
        {
            'id_type': 'Duration ID',
            'generated_count': len(df),
            'from_input_count': df['duration_id'].apply(lambda x: is_from_input_value(x)).sum(),
            'from_historical_count': len(df) - df['duration_id'].apply(lambda x: is_from_input_value(x)).sum(),
            'collision_count': len(df) - df['duration_id'].nunique(),
            'success_pct': (df['duration_id'].nunique() / len(df) * 100) if len(df) > 0 else 0
        },
        {
            'id_type': 'Summary ID',
            'generated_count': len(df),
            'from_input_count': df['summary_id'].apply(lambda x: is_from_input_value(x)).sum(),
            'from_historical_count': len(df) - df['summary_id'].apply(lambda x: is_from_input_value(x)).sum(),
            'collision_count': len(df) - df['summary_id'].nunique(),
            'success_pct': (df['summary_id'].nunique() / len(df) * 100) if len(df) > 0 else 0
        },
        {
            'id_type': 'Source ID',
            'generated_count': len(df),
            'from_input_count': df['source_id'].apply(lambda x: is_from_input_value(x)).sum(),
            'from_historical_count': len(df) - df['source_id'].apply(lambda x: is_from_input_value(x)).sum(),
            'collision_count': len(df) - df['source_id'].nunique(),
            'success_pct': (df['source_id'].nunique() / len(df) * 100) if len(df) > 0 else 0
        }
    ]
    
    # Prepare duplicate detection results for table display
    duplicate_results = [
        {
            'source': 'True Duplicates',
            'total_count': len(df),
            'duplicate_count': len(df) - df['true_duplicates'].nunique(),
            'unique_count': df['true_duplicates'].nunique(),
            'duplicate_pct': ((len(df) - df['true_duplicates'].nunique()) / len(df) * 100) if len(df) > 0 else 0,
            'detection_method': 'Source + Group + Summary + Company'
        },
        {
            'source': 'Cross-Platform',
            'total_count': len(df),
            'duplicate_count': len(df) - df['cross_platform_duplicates'].nunique(),
            'unique_count': df['cross_platform_duplicates'].nunique(),
            'duplicate_pct': ((len(df) - df['cross_platform_duplicates'].nunique()) / len(df) * 100) if len(df) > 0 else 0,
            'detection_method': 'Group + Summary + Company'
        },
        {
            'source': 'Location Clusters',
            'total_count': len(df),
            'duplicate_count': len(df) - df['location_clusters'].nunique(),
            'unique_count': df['location_clusters'].nunique(),
            'duplicate_pct': ((len(df) - df['location_clusters'].nunique()) / len(df) * 100) if len(df) > 0 else 0,
            'detection_method': 'Group + Location'
        }
    ]
    
    # Log summary of ID generation
    location_unique = df['location_id'].nunique()
    hours_unique = df['hours_id'].nunique()
    duration_unique = df['duration_id'].nunique()
    summary_unique = df['summary_id'].nunique()
    source_unique = df['source_id'].nunique()
    true_duplicates_unique = df['true_duplicates'].nunique()
    cross_platform_unique = df['cross_platform_duplicates'].nunique()
    location_clusters_unique = df['location_clusters'].nunique()
    recommendations_unique = df['recommendations'].nunique()
    company_location_roles_unique = df['company_location_roles'].nunique()
    logging.info(f"ID generation completed: {location_unique} unique locations, {hours_unique} unique hours, {duration_unique} unique durations, {summary_unique} unique summaries, {source_unique} unique sources, {true_duplicates_unique} unique true_duplicates")
    logging.info(f"Business matching: {cross_platform_unique} cross-platform groups, {location_clusters_unique} location clusters, {recommendations_unique} recommendation groups, {company_location_roles_unique} company-location-role groups")
    
    df['date'] = timestamp()
    
    # Log summary of duplicate analysis (replaced verbose logging with table format)
    duplicate_groups = df.groupby('group_id').size().reset_index(name='count')
    duplicate_groups = duplicate_groups[duplicate_groups['count'] > 1]
    
    true_dup_counts = df.groupby('true_duplicates').size().reset_index(name='count')
    true_duplicate_groups = true_dup_counts[true_dup_counts['count'] > 1]
    
    logging.info(f"Duplicate analysis: {len(duplicate_groups)} duplicate title groups, {len(true_duplicate_groups)} true duplicate groups")
    
    # If we have historical data, preserve dates for existing records
    if historical_data is not None and not historical_data.empty:
        for idx in df.index:
            if df.loc[idx, 'UNIQUE_ID'] in set(historical_data['UNIQUE_ID']):
                df.loc[idx, 'date'] = historical_data[
                    historical_data['UNIQUE_ID'] == df.loc[idx, 'UNIQUE_ID']
                ].iloc[0]['date']
    
    # Remove duplicates using UNIQUE_ID - COMMENTED OUT
    # duplicates_count = df.duplicated(subset=['UNIQUE_ID']).sum()
    # if duplicates_count > 0:
    #     logging.info(f"Removing {duplicates_count} duplicates from new data based on UNIQUE_ID")
    #     df = df.drop_duplicates(subset=['UNIQUE_ID'], keep='first')
    
    # Add regional categorization columns
    logging.info("Adding regional categorization columns...")
    df = add_regional_columns(df, location_column='Location')
    logging.info("Regional categorization completed.")
    
    # Return both the processed DataFrame and tracking results
    return df, id_results, duplicate_results

def merge_with_historical_data(new_data, historical_data):
    """
    Merge new data with historical data, preserving original dates for existing records.
    Uses UNIQUE_ID for matching to ensure consistency.
    """
    if historical_data.empty:
        return new_data
    
    # Find records that already exist using UNIQUE_ID
    existing_records = new_data['UNIQUE_ID'].isin(historical_data['UNIQUE_ID'])
    existing_count = existing_records.sum()
    
    if existing_count > 0:
        logging.info(f"Found {existing_count} records that already exist in historical data")
        
        # Update dates for existing records directly
        for idx in new_data[existing_records].index:
            matching_historical = historical_data[historical_data['UNIQUE_ID'] == new_data.loc[idx, 'UNIQUE_ID']]
            if not matching_historical.empty:
                new_data.loc[idx, 'date'] = matching_historical.iloc[0]['date']
        
        # Log some examples of preserved dates
        sample_size = min(3, existing_count)
        sample_records = new_data[existing_records].head(sample_size)
        for _, record in sample_records.iterrows():
            logging.info(f"Preserved date {record['date']} for job: {record['Title']} at {record['Company']}")
    
    return new_data

def supabase_upload(df, table_name, is_historical=False):
    """
    Upload data to Supabase.
    For Allgigs_All_vacancies_NEW:
        - Delete all records not present in today's data (by UNIQUE_ID)
        - Upsert new data, preserving older date for duplicates
        - Log a human-readable summary
    For historical table:
        - Upsert new data, preserving older date for duplicates
    """
    try:
        # Fetch existing records for date preservation and deletion
        existing_data = get_existing_records(table_name)
        existing_dates = {}
        existing_ids = set()
        if not existing_data.empty:
            existing_dates = dict(zip(existing_data['UNIQUE_ID'], existing_data['date']))
            existing_ids = set(existing_data['UNIQUE_ID'])
        num_existing_before = len(existing_ids)

        # For each record, if UNIQUE_ID exists, keep the older date
        updated_count = 0
        for idx in df.index:
            unique_id = df.loc[idx, 'UNIQUE_ID']
            if unique_id in existing_dates:
                old_date = existing_dates[unique_id]
                if old_date < df.loc[idx, 'date']:
                    df.loc[idx, 'date'] = old_date
                updated_count += 1

        # For NEW table: delete records not present in today's data
        deleted_count = 0
        actually_deleted_count = 0 # For diagnostics
        if table_name == 'Allgigs_All_vacancies_NEW':
            todays_ids = set(df['UNIQUE_ID'])
            ids_to_delete = list(existing_ids - todays_ids)
            deleted_count = len(ids_to_delete) # Total that should be deleted
            
            if ids_to_delete:
                logging.info(f"Attempting to batch delete {deleted_count} stale records from {table_name}.")
                BATCH_DELETE_SIZE_STALE = 250 # Local batch size for deleting stale records, reduced from 1000 to 250
                for i_stale in range(0, len(ids_to_delete), BATCH_DELETE_SIZE_STALE):
                    batch_ids_stale = ids_to_delete[i_stale:i_stale+BATCH_DELETE_SIZE_STALE]
                    try:
                        logging.info(f"Deleting batch of {len(batch_ids_stale)} stale IDs starting at index {i_stale}...")
                        supabase.table(table_name).delete().in_('UNIQUE_ID', batch_ids_stale).execute()
                        actually_deleted_count += len(batch_ids_stale) # Assuming success if no error
                    except Exception as e_batch_stale_delete:
                        logging.error(f"ERROR DURING BATCH STALE DELETE for IDs starting with {batch_ids_stale[0] if batch_ids_stale else 'N/A'} in table {table_name}.")
                        logging.error(f"Batch stale delete operation error details: {str(e_batch_stale_delete)}")
                        # Decide if you want to stop or continue. For now, let's log and attempt to continue.
                        # Consider re-raising if this is critical: raise e_batch_stale_delete
                
                if actually_deleted_count == deleted_count:
                    logging.info(f"Successfully batch deleted all {actually_deleted_count} stale records.")
                else:
                    logging.warning(f"Attempted to batch delete {deleted_count} stale records, but only {actually_deleted_count} were confirmed processed without error. Check logs.")
            else:
                logging.info(f"No stale records to delete from {table_name}.")

        # For historical table, remove group_id and ID columns if they exist
        if is_historical:
            columns_to_remove = []
            if 'group_id' in df.columns:
                columns_to_remove.append('group_id')
            if 'location_id' in df.columns:
                columns_to_remove.append('location_id')
            if 'hours_id' in df.columns:
                columns_to_remove.append('hours_id')
            if 'duration_id' in df.columns:
                columns_to_remove.append('duration_id')
            if 'summary_id' in df.columns:
                columns_to_remove.append('summary_id')
            if 'source_id' in df.columns:
                columns_to_remove.append('source_id')
            if 'true_duplicates' in df.columns:
                columns_to_remove.append('true_duplicates')
            if 'cross_platform_duplicates' in df.columns:
                columns_to_remove.append('cross_platform_duplicates')
            if 'location_clusters' in df.columns:
                columns_to_remove.append('location_clusters')
            if 'recommendations' in df.columns:
                columns_to_remove.append('recommendations')
            if 'company_location_roles' in df.columns:
                columns_to_remove.append('company_location_roles')
            if 'Views' in df.columns:
                columns_to_remove.append('Views')
            if 'Likes' in df.columns:
                columns_to_remove.append('Likes')
            if 'Keywords' in df.columns:
                columns_to_remove.append('Keywords')
            if 'Offers' in df.columns:
                columns_to_remove.append('Offers')
            
            if columns_to_remove:
                logging.info(f"Removing columns for historical table upload to {table_name}: {columns_to_remove}")
                df = df.drop(columns=columns_to_remove)
        
        # Handle NaN values before converting to records
        df = df.fillna('')
        # Replace any remaining NaN values with empty strings
        for col in df.columns:
            df[col] = df[col].astype(str).replace(['nan', 'NaN', 'None', 'none', 'NULL', 'null'], '')
        
        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')
        total_records = len(records)
        new_records_total = 0
        for i in range(0, total_records, BATCH_SIZE): # BATCH_SIZE is the main constant (500)
            batch_df = df[i:i + BATCH_SIZE]
            batch_data = batch_df.to_dict(orient='records')
            batch_ids = batch_df['UNIQUE_ID'].tolist()
            
            delete_successful = True # Flag to track delete success

            if table_name == NEW_TABLE: # Only perform pre-upsert batch delete for the NEW_TABLE
                if batch_ids:
                    logging.info(f"Attempting to batch delete {len(batch_ids)} records from {NEW_TABLE} for batch starting at index {i}")
                    try:
                        delete_response = supabase.table(table_name).delete().in_('UNIQUE_ID', batch_ids).execute()
                        # logging.info(f"Delete response for batch {i // BATCH_SIZE + 1}: {delete_response}")
                    except Exception as e_delete:
                        logging.error(f"ERROR DURING BATCH DELETE for batch {i // BATCH_SIZE + 1} of table {NEW_TABLE}.")
                        # logging.error(f"Problematic batch_ids for delete: {batch_ids[:10]}...") # Log some IDs
                        logging.error(f"Delete operation error details: {str(e_delete)}")
                        delete_successful = False 
                        raise # Re-raise to stop processing if batch delete fails
                    if delete_successful:
                        logging.info(f"Successfully batch deleted records for {NEW_TABLE} for batch starting at index {i} (if any were present).")
                else:
                    logging.info(f"Skipping batch delete for {NEW_TABLE} for batch starting at index {i} as batch_ids is empty.")
            # For HISTORICAL_TABLE, we don't do this pre-upsert delete.

            if delete_successful: # True if delete succeeded OR if table_name was HISTORICAL_TABLE (delete skipped)
                try:
                    logging.info(f"Attempting to upsert {len(batch_data)} records to {table_name} for batch starting at index {i}")
                    response = supabase.table(table_name).upsert(batch_data, on_conflict='UNIQUE_ID').execute()
                    if hasattr(response, 'data'):
                        new_records = len(response.data)
                        new_records_total += new_records
                    time.sleep(1)  # Rate limiting
                except Exception as e_upsert:
                    logging.error(f"Error during UPSERT for batch {i // BATCH_SIZE + 1} of table {table_name}.")
                    logging.error(f"Upsert operation error details: {str(e_upsert)}")
                    raise # Re-raising to see the error
            else:
                logging.warning(f"Skipping upsert for batch {i // BATCH_SIZE + 1} due to delete operation failure.")
        
        # Prepare upload results for table display
        upload_result = {
            'table_name': table_name,
            'status': 'Success',
            'before_count': num_existing_before,
            'deleted_count': deleted_count,
            'upserted_count': total_records,
            'new_count': new_records_total,
            'updated_count': updated_count - new_records_total,
            'final_count': total_records
        }
        
        # Add a clear header before the upload summary
        logging.info(f"\n========== SUPABASE UPLOAD RESULTS for {table_name} ==========")
        if table_name == 'Allgigs_All_vacancies_NEW':
            logging.info(f"Records before upload: {num_existing_before}")
            logging.info(f"Records deleted (not in today's data): {deleted_count}")
            logging.info(f"Records upserted (processed today): {total_records}")
            logging.info(f"New records added: {new_records_total}")
            logging.info(f"Records updated (date preserved): {updated_count - new_records_total}")
            logging.info(f"Records in table after upload: {total_records}")
        else:
            logging.info(f"Upserted {total_records} records to {table_name}, {new_records_total} were new.")
        
        return upload_result
    except Exception as e:
        logging.error(f"Failed to upload data to Supabase: {str(e)}")
        upload_result = {
            'table_name': table_name,
            'status': 'Failed',
            'error': str(e)
        }
        raise

def get_automation_details_from_supabase(supabase_client: Client, logger_param) -> pd.DataFrame:
    """Fetches automation details from the 'automation_details' table in Supabase."""
    try:
        logger_param.info("Fetching automation details from Supabase table 'automation_details'...")
        response = supabase_client.table('automation_details').select("*").execute()
        if response.data:
            df = pd.DataFrame(response.data)
            logger_param.info(f"Successfully fetched {len(df)} automation detail records from Supabase.")
            # Ensure 'Path' and 'Type' columns exist, similar to how octoparse_script expects them
            # This might need adjustment based on the actual column names in your Supabase table
            if 'Path' not in df.columns and 'URL' in df.columns: # Check if old 'URL' column exists and rename
                 df.rename(columns={'URL': 'Path'}, inplace=True)
            
            # Ensure critical columns are present (adjust as per your actual needs)
            required_cols_supabase = ['Company_name', 'Path', 'Type'] 
            missing_cols_supabase = [col for col in required_cols_supabase if col not in df.columns]
            if missing_cols_supabase:
                logger_param.error(f"Missing critical columns in data fetched from Supabase 'automation_details' table: {missing_cols_supabase}")
                logger_param.error(f"Available columns: {df.columns.tolist()}")
                return pd.DataFrame() # Return empty DataFrame on critical error
            return df
        else:
            logger_param.warning("No data found in Supabase 'automation_details' table.")
            return pd.DataFrame()
    except Exception as e:
        logger_param.error(f"Error fetching automation details from Supabase: {e}")
        return pd.DataFrame()

# Global tracking for table format
processing_results = []

def write_to_log_and_console(message):
    """Write message to both console and allgigs_v7.out.log file"""
    print(message)
    try:
        with open('allgigs_v7.out.log', 'a', encoding='utf-8') as log_file:
            log_file.write(message + '\n')
    except Exception as e:
        print(f"Warning: Could not write to log file: {e}")

def check_and_rotate_log_file():
    """Check if log file has 50+ runs and rotate if needed"""
    try:
        # Count the number of "NEW LOG SESSION STARTED" entries
        session_count = 0
        try:
            with open('allgigs_v7.out.log', 'r', encoding='utf-8') as log_file:
                for line in log_file:
                    if "🚀 NEW LOG SESSION STARTED" in line:
                        session_count += 1
        except FileNotFoundError:
            # Log file doesn't exist yet, create it
            return
        
        # Show current log status
        print(f"📊 Current log file has {session_count} sessions (max: 50)")
        
        # If we have 50 or more sessions, rotate the log file
        if session_count >= 50:
            from datetime import datetime
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            old_log_name = f'allgigs_v7.out.log.{timestamp}'
            
            # Rename current log file
            import os
            os.rename('allgigs_v7.out.log', old_log_name)
            
            # Create new log file with rotation notice
            rotation_notice = [
                "="*120,
                "🔄 LOG FILE ROTATED",
                "="*120,
                f"📅 DATE: {datetime.now().strftime('%Y-%m-%d')}",
                f"⏰ TIME: {datetime.now().strftime('%H:%M:%S')}",
                f"📊 REASON: Previous log file had {session_count} sessions (max: 50)",
                f"📁 OLD LOG: {old_log_name}",
                f"📁 NEW LOG: allgigs_v7.out.log",
                "="*120,
                ""
            ]
            
            with open('allgigs_v7.out.log', 'w', encoding='utf-8') as new_log:
                for line in rotation_notice:
                    new_log.write(line + '\n')
            
            print(f"🔄 Log file rotated: {old_log_name} -> allgigs_v7.out.log")
            
    except Exception as e:
        print(f"Warning: Could not rotate log file: {e}")

def print_simple_table(result_df=None):
    """Print processing results in enhanced table format with detailed reasons and field completion stats"""
    if not processing_results:
        return
    
    output = []
    output.append("\n" + "="*120)
    output.append("📊 DATA SOURCE PROCESSING RESULTS (Sorted by Success % - Lowest First)")
    output.append("="*120)
    
    # Enhanced header with status icons and field completion
    header = f"{'Source':<20} {'Status':<12} {'Initial':<8} {'Final':<8} {'Dropped':<8} {'Success %':<10} {'Title':<8} {'URL':<8} {'Location':<10} {'Summary':<10} {'Rate':<8} {'Hours':<8} {'Duration':<10}"
    output.append(header)
    output.append("-" * 120)
    
    # Calculate success percentages and create detailed reasons
    enhanced_results = []
    for result in processing_results:
        company = result['company']
        status = result['status']
        initial = result.get('read_data', 0)
        final = result.get('processed', 0)
        dropped = result.get('dropped', 0)
        success_pct = (final / initial * 100) if initial > 0 else 0
        
        # Calculate field completion statistics if result_df is available
        field_stats = {}
        if result_df is not None and status == "Success":
            source_data = result_df[result_df['Source'] == company]
            if len(source_data) > 0:
                fields_to_check = ['Title', 'URL', 'Location', 'Summary', 'rate', 'Hours', 'Duration']
                for field in fields_to_check:
                    if field in source_data.columns:
                        # Count non-empty, non-null values (excluding "Not mentioned")
                        completed = source_data[field].notna() & (source_data[field] != 'Not mentioned') & (source_data[field] != '')
                        completion_pct = (completed.sum() / len(source_data) * 100) if len(source_data) > 0 else 0
                        field_stats[field] = f"{completion_pct:.0f}%"
                    else:
                        field_stats[field] = "0%"
            else:
                field_stats = {field: "0%" for field in ['Title', 'URL', 'Location', 'Summary', 'rate', 'Hours', 'Duration']}
        else:
            field_stats = {field: "N/A" for field in ['Title', 'URL', 'Location', 'Summary', 'rate', 'Hours', 'Duration']}
        
        enhanced_results.append({
            'company': company,
            'status': status,
            'initial': initial,
            'final': final,
            'dropped': dropped,
            'success_pct': success_pct,
            'field_stats': field_stats
        })
    
    # Sort by success percentage (lowest first)
    enhanced_results.sort(key=lambda x: x['success_pct'])
    
    # Remove duplicates based on company name (keep the last occurrence)
    seen_companies = set()
    unique_results = []
    for result in enhanced_results:
        company = result['company']
        if company not in seen_companies:
            seen_companies.add(company)
            unique_results.append(result)
        else:
            # Replace the previous entry with the current one (keep the last occurrence)
            for i, existing_result in enumerate(unique_results):
                if existing_result['company'] == company:
                    unique_results[i] = result
                    break
    
    # Data rows with better formatting
    for result in unique_results:
        company = result['company'][:19]  # Truncate if too long
        status = result['status']
        initial = result['initial']
        final = result['final']
        dropped = result['dropped']
        success_pct = result['success_pct']
        field_stats = result['field_stats']
        
        # Status icons
        if status == "Success":
            status_icon = "✅"
        elif status == "Failed":
            status_icon = "❌"
        elif status == "Skipped":
            status_icon = "⏭️"
        else:
            status_icon = "❓"
        
        row = f"{company:<20} {status_icon} {status:<8} {initial:<8} {final:<8} {dropped:<8} {success_pct:>7.1f}% {field_stats['Title']:<8} {field_stats['URL']:<8} {field_stats['Location']:<10} {field_stats['Summary']:<10} {field_stats['rate']:<8} {field_stats['Hours']:<8} {field_stats['Duration']:<10}"
        output.append(row)
    
    output.append("-" * 120)
    
    # Add field completion legend
    output.append("\n📋 FIELD COMPLETION LEGEND:")
    output.append("  • Shows percentage of records with actual data (not 'Not mentioned' or empty)")
    output.append("  • N/A = Source was skipped or failed")
    output.append("  • Higher percentages indicate better data quality")
    
    # Write all lines to both console and log
    for line in output:
        write_to_log_and_console(line)

def print_summary_stats():
    """Print summary statistics"""
    if not processing_results:
        return
    
    output = []
    output.append("\n" + "="*120)
    output.append("📈 SUMMARY STATISTICS")
    output.append("="*120)
    
    total_sources = len(processing_results)
    successful_sources = sum(1 for r in processing_results if r['status'] == 'Success')
    failed_sources = sum(1 for r in processing_results if r['status'] == 'Failed')
    skipped_sources = sum(1 for r in processing_results if r['status'] == 'Skipped')
    
    total_initial_rows = sum(r.get('read_data', 0) for r in processing_results)
    total_final_rows = sum(r.get('processed', 0) for r in processing_results)
    total_dropped_rows = sum(r.get('dropped', 0) for r in processing_results)
    
    success_rate = (successful_sources / total_sources * 100) if total_sources > 0 else 0
    data_retention_rate = (total_final_rows / total_initial_rows * 100) if total_initial_rows > 0 else 0
    
    # Calculate performance tiers
    high_performers = sum(1 for r in processing_results if r['status'] == 'Success' and (r.get('processed', 0) / r.get('read_data', 1) * 100) >= 80)
    medium_performers = sum(1 for r in processing_results if r['status'] == 'Success' and 20 <= (r.get('processed', 0) / r.get('read_data', 1) * 100) < 80)
    low_performers = sum(1 for r in processing_results if r['status'] == 'Success' and (r.get('processed', 0) / r.get('read_data', 1) * 100) < 20)
    
    output.append(f"Total Sources: {total_sources}")
    output.append(f"Successful: {successful_sources} ({success_rate:.1f}%)")
    output.append(f"  ├─ High Performers (≥80%): {high_performers}")
    output.append(f"  ├─ Medium Performers (20-79%): {medium_performers}")
    output.append(f"  └─ Low Performers (<20%): {low_performers}")
    output.append(f"Failed: {failed_sources}")
    output.append(f"Skipped: {skipped_sources}")
    output.append("")
    output.append(f"Total Initial Rows: {total_initial_rows:,}")
    output.append(f"Total Final Rows: {total_final_rows:,}")
    output.append(f"Total Dropped Rows: {total_dropped_rows:,}")
    output.append(f"Data Retention Rate: {data_retention_rate:.1f}%")
    output.append("")
    output.append("📊 PERFORMANCE BREAKDOWN:")
    output.append(f"• Sources with 100% success: {sum(1 for r in processing_results if r['status'] == 'Success' and r.get('dropped', 0) == 0)}")
    output.append(f"• Sources with partial data loss: {sum(1 for r in processing_results if r['status'] == 'Success' and r.get('dropped', 0) > 0)}")
    output.append(f"• Sources completely filtered out: {skipped_sources}")
    output.append("="*120)
    
    # Write all lines to both console and log
    for line in output:
        write_to_log_and_console(line)

def upload_processing_results_to_supabase():
    """Upload processing results to Supabase table 'DATA SOURCE PROCESSING RESULTS' - overwrites entire table"""
    if not processing_results:
        return

    try:
        # Create results with the 6 columns you want
        csv_data = []
        current_date = datetime.now().strftime('%Y-%m-%d')
        current_time = datetime.now().strftime('%H:%M:%S')

        for result in processing_results:
            company = result['company']
            status = result['status']
            initial = result.get('read_data', 0)
            final = result.get('processed', 0)
            dropped = result.get('dropped', 0)
            success_pct = (final / initial * 100) if initial > 0 else 0

            csv_data.append({
                'Source': company,
                'Initial': initial,
                'Dropped': dropped,
                'Final': final,
                'Success_Percentage': success_pct,
                'Status': status,
                'run_date': current_date,
                'run_time': current_time
            })

        # Create DataFrame and sort by success percentage (lowest first)
        df = pd.DataFrame(csv_data)
        df = df.sort_values('Success_Percentage', ascending=True)

        # Handle NaN values before converting to records
        df = df.fillna('')
        for col in df.columns:
            df[col] = df[col].astype(str).replace(['nan', 'NaN', 'None', 'none', 'NULL', 'null'], '')

        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')
        total_records = len(records)

        # First, delete all existing records from the table
        logging.info(f"Deleting all existing records from 'DATA SOURCE PROCESSING RESULTS' table")
        supabase.table('DATA SOURCE PROCESSING RESULTS').delete().neq('id', 0).execute()

        # Then upload new records to Supabase in batches
        new_records_total = 0
        for i in range(0, total_records, BATCH_SIZE):
            batch_df = df[i:i + BATCH_SIZE]
            batch_data = batch_df.to_dict(orient='records')

            try:
                logging.info(f"Uploading {len(batch_data)} processing results to Supabase table 'DATA SOURCE PROCESSING RESULTS' for batch starting at index {i}")
                response = supabase.table('DATA SOURCE PROCESSING RESULTS').insert(batch_data).execute()
                if hasattr(response, 'data'):
                    new_records = len(response.data)
                    new_records_total += new_records
                time.sleep(1)  # Rate limiting
            except Exception as e_upsert:
                logging.error(f"Error during processing results upload for batch {i // BATCH_SIZE + 1}: {str(e_upsert)}")
                raise

        logging.info(f"Successfully uploaded {new_records_total} processing results to Supabase table 'DATA SOURCE PROCESSING RESULTS' (table overwritten)")

    except Exception as e:
        logging.error(f"Failed to upload processing results to Supabase: {str(e)}")
        raise

def print_supabase_upsert_table(upload_results):
    """Print Supabase upload results in table format"""
    if not upload_results:
        output = []
        output.append("\n" + "="*120)
        output.append("🗄️  SUPABASE UPLOAD RESULTS")
        output.append("="*120)
        output.append("No upload results to display.")
        output.append("-" * 120)
        
        for line in output:
            write_to_log_and_console(line)
        return
    
    output = []
    output.append("\n" + "="*120)
    output.append("🗄️  SUPABASE UPLOAD RESULTS")
    output.append("="*120)
    
    header = f"{'Table':<25} {'Status':<12} {'Before':<8} {'Deleted':<8} {'Upserted':<10} {'New':<8} {'Updated':<10} {'Final':<8}"
    output.append(header)
    output.append("-" * 120)
    
    for result in upload_results:
        table_name = result.get('table_name', 'Unknown')[:24]
        status = result.get('status', 'Unknown')
        before_count = result.get('before_count', 0)
        deleted_count = result.get('deleted_count', 0)
        upserted_count = result.get('upserted_count', 0)
        new_count = result.get('new_count', 0)
        updated_count = result.get('updated_count', 0)
        final_count = result.get('final_count', 0)
        
        # Status icons
        if status == "Success":
            status_icon = "✅"
        elif status == "Failed":
            status_icon = "❌"
        else:
            status_icon = "⚠️"
        
        row = f"{table_name:<25} {status_icon} {status:<8} {before_count:<8} {deleted_count:<8} {upserted_count:<10} {new_count:<8} {updated_count:<10} {final_count:<8}"
        output.append(row)
    
    output.append("-" * 120)
    
    # Write all lines to both console and log
    for line in output:
        write_to_log_and_console(line)

def print_duplicates_table(duplicate_results):
    """Print duplicate detection results in table format"""
    if not duplicate_results:
        return
    
    output = []
    output.append("\n" + "="*120)
    output.append("🔄 DUPLICATE DETECTION RESULTS")
    output.append("="*120)
    
    header = f"{'Source':<20} {'Total':<8} {'Duplicates':<12} {'Unique':<8} {'Duplicate %':<12} {'Detection Method':<20}"
    output.append(header)
    output.append("-" * 120)
    
    for result in duplicate_results:
        source = result.get('source', 'Unknown')[:19]
        total_count = result.get('total_count', 0)
        duplicate_count = result.get('duplicate_count', 0)
        unique_count = result.get('unique_count', 0)
        duplicate_pct = (duplicate_count / total_count * 100) if total_count > 0 else 0
        detection_method = result.get('detection_method', 'N/A')[:19]
        
        row = f"{source:<20} {total_count:<8} {duplicate_count:<12} {unique_count:<8} {duplicate_pct:>10.1f}% {detection_method:<20}"
        output.append(row)
    
    output.append("-" * 120)
    
    # Write all lines to both console and log
    for line in output:
        write_to_log_and_console(line)

def print_id_generation_table(id_results):
    """Print ID generation results in table format"""
    if not id_results:
        return
    
    output = []
    output.append("\n" + "="*120)
    output.append("🆔 ID GENERATION RESULTS")
    output.append("="*120)
    
    header = f"{'ID Type':<20} {'Generated':<10} {'From Input':<12} {'From Historical':<15} {'Collisions':<10} {'Success %':<10}"
    output.append(header)
    output.append("-" * 120)
    
    for result in id_results:
        id_type = result.get('id_type', 'Unknown')[:19]
        generated_count = result.get('generated_count', 0)
        from_input_count = result.get('from_input_count', 0)
        from_historical_count = result.get('from_historical_count', 0)
        collision_count = result.get('collision_count', 0)
        success_pct = ((generated_count - collision_count) / generated_count * 100) if generated_count > 0 else 0
        
        row = f"{id_type:<20} {generated_count:<10} {from_input_count:<12} {from_historical_count:<15} {collision_count:<10} {success_pct:>8.1f}%"
        output.append(row)
    
    output.append("-" * 120)
    
    # Write all lines to both console and log
    for line in output:
        write_to_log_and_console(line)

def main():
    global processing_results
    # Reset processing_results for new run
    processing_results = []
    error_messages = []  # Collect error messages for summary
    broken_urls = []     # Collect broken URLs for summary
    source_failures = [] # Collect (company, reason) for summary
    
    # Initialize tracking variables for enhanced tables
    upload_results = []
    duplicate_results = []
    id_results = []
    try:
        start_time = time.time()
        
        # Create session header in log file
        session_header = [
            "="*120,
            "🚀 NEW LOG SESSION STARTED",
            "="*120,
            f"📅 DATE: {datetime.now().strftime('%Y-%m-%d')}",
            f"⏰ TIME: {datetime.now().strftime('%H:%M:%S')}",
            "🔄 SESSION: AllGigs V7 Processing Started",
            "="*120,
            ""
        ]
        
        # Check if log file needs rotation before starting new session
        check_and_rotate_log_file()
        
        for line in session_header:
            write_to_log_and_console(line)
        
        logging.info("Script started.")

        # Ensure directories exist
        FREELANCE_DIR.mkdir(parents=True, exist_ok=True)
        IMPORTANT_DIR.mkdir(parents=True, exist_ok=True)
        
        # Fetch automation details from Supabase
        automation_details = get_automation_details_from_supabase(supabase, logging)

        if automation_details.empty:
            logging.error("Failed to load automation details from Supabase. Exiting.")
            return
        
        # Log the number of sources loaded
        logging.info(f"Loaded automation details with {len(automation_details)} sources from Supabase")
        
        # Fetch existing records from historical data
        try:
            historical_data = get_existing_records(HISTORICAL_TABLE)
        except Exception as e:
            msg = f"FAILED: Historical data - {e}"
            logging.warning(msg)
            error_messages.append(msg)
            source_failures.append(("Historical data", str(e)))
            historical_data = pd.DataFrame()
        
        result = pd.DataFrame()
        
        # Process each company from the automation details
        for index, row in automation_details.iterrows():
            try:
                company_name = row['Company_name']
                url_link = row['Path']

                # Initialize result tracking for table
                processing_result = {
                    'company': company_name,
                    'source': 'CSV',
                    'read_data': 0,
                    'dropped': 0,
                    'processed': 0,
                    'status': 'Processing',
                    'drop_reason': ''
                }

                # Add a minimal title before processing each company
                logging.info(f"Processing: {company_name}")
                
                files_read = None
                read_successful = False
                csv_is_empty_or_no_data = False # Flag for this specific condition

                for separator in [',', ';', '\t']:
                    try:
                        # Check file size first for large files
                        import os
                        file_size = os.path.getsize(url_link) if os.path.exists(url_link) else 0
                        file_size_mb = file_size / (1024 * 1024)
                        
                        if file_size_mb > 10:  # If file is larger than 10MB, use chunking
                            logging.info(f"INFO: {company_name} - Large CSV file detected ({file_size_mb:.1f}MB). Processing in chunks...")
                            
                            # Read CSV in chunks
                            chunk_size = 5000  # Process 5000 rows at a time
                            all_chunks = []
                            
                            for chunk in pd.read_csv(url_link, sep=separator, chunksize=chunk_size):
                                if chunk.empty:
                                    continue
                                
                                # Process each chunk
                                chunk_processed = chunk.fillna('')
                                for col in chunk_processed.columns:
                                    chunk_processed[col] = chunk_processed[col].astype(str).replace(['nan', 'NaN', 'None', 'none'], '')
                                
                                all_chunks.append(chunk_processed)
                                
                                # Log progress for large files
                                if len(all_chunks) % 10 == 0:  # Every 50,000 rows
                                    logging.info(f"INFO: {company_name} - Processed {len(all_chunks) * chunk_size:,} rows...")
                            
                            if not all_chunks:
                                logging.info(f"INFO: {company_name} - CSV file contains no data rows ({url_link}). Skipping.")
                                csv_is_empty_or_no_data = True
                                break
                            
                            # Combine all chunks
                            files_read = pd.concat(all_chunks, ignore_index=True)
                            logging.info(f"INFO: {company_name} - Successfully loaded {len(files_read):,} rows from large CSV file.")
                            
                        else:
                            # Normal processing for smaller files
                            temp_df = pd.read_csv(url_link, sep=separator)
                            
                            if temp_df.empty: # CSV has headers but no data rows
                                logging.info(f"INFO: {company_name} - CSV file contains no data rows ({url_link}). Skipping.")
                                csv_is_empty_or_no_data = True
                                break # Stop trying separators, we've identified the state

                            # If we reach here, CSV has data. Process it.
                            files_read = temp_df.fillna('')
                            for col in files_read.columns:
                                files_read[col] = files_read[col].astype(str).replace(['nan', 'NaN', 'None', 'none'], '')
                        read_successful = True
                        break # Successfully read and processed

                    except pd.errors.EmptyDataError: # CSV is completely empty (no headers, no data)
                        logging.info(f"INFO: {company_name} - CSV file is completely empty ({url_link}). Skipping.")
                        csv_is_empty_or_no_data = True
                        break # Stop trying separators

                    except Exception: # Other read errors (e.g., file not found, malformed)
                        continue
                
                if csv_is_empty_or_no_data:
                    processing_result.update({
                        'status': 'Skipped',
                        'drop_reason': 'CSV file contains no data rows'
                    })
                    processing_results.append(processing_result)
                    logging.info("") # Add an empty line for separation
                    continue # Skip to the next company

                if not read_successful: # Implies all separators failed with "other" exceptions
                    processing_result.update({
                        'status': 'Failed',
                        'drop_reason': 'Could not read or parse CSV'
                    })
                    processing_results.append(processing_result)
                    msg = f"FAILED: {company_name} - Could not read or parse CSV ({url_link}) after trying all separators."
                    logging.error(msg)
                    error_messages.append(msg)
                    broken_urls.append((company_name, url_link))
                    source_failures.append((company_name, f"Could not read or parse CSV ({url_link})"))
                    logging.info("") # Add an empty line
                    continue
                
                # If we reach here, files_read is a populated DataFrame
                processing_result['read_data'] = len(files_read)
                company_df = freelance_directory(files_read, company_name)

                if company_df.empty:
                    processing_result.update({
                        'status': 'Skipped',
                        'dropped': processing_result['read_data'],
                        'processed': 0,
                        'drop_reason': 'No data remained after processing'
                    })
                    processing_results.append(processing_result)
                    continue # Skip to the next company if no data after cleaning

                # If we reach here, company_df is not empty
                processing_result.update({
                    'status': 'Success',
                    'processed': len(company_df),
                    'dropped': processing_result['read_data'] - len(company_df),
                    'drop_reason': 'Successfully processed'
                })
                processing_results.append(processing_result)
                result = pd.concat([result, company_df], ignore_index=True)
            
            except Exception as e:
                msg = f"FAILED: {company_name} - {str(e)}"
                logging.error(msg)
                error_messages.append(msg)
                source_failures.append((company_name, str(e)))
                
                # Update processing result with failure reason
                processing_result.update({
                    'status': 'Failed',
                    'drop_reason': str(e)[:50]  # Truncate error message
                })
                processing_results.append(processing_result)
                
                # Add an empty line after each company
                logging.info("")
                continue

        if result.empty:
            msg = "FAILED: No data collected from any source"
            logging.error(msg)
            error_messages.append(msg)
            source_failures.append(("ALL", "No data collected from any source"))
            return
        
        # Prepare data with dates and IDs
        result, id_results, duplicate_results = prepare_data_for_upload(result, historical_data)
        
        # Print regional distribution summary
        print_regional_summary(result)
        
        # Print enhanced table format summaries
        print_simple_table(result)
        print_duplicates_table(duplicate_results)
        print_id_generation_table(id_results)
        print_summary_stats()
        
        # Save to local CSV file
        current_date_str = timestamp()
        num_rows = len(result)
        output_dir = Path("/Users/jaapjanlammers/Library/CloudStorage/GoogleDrive-jj@nineways.nl/My Drive/allGigs_log/")
        dynamic_filename = f"{num_rows}_{current_date_str}_allGigs.csv"
        full_output_path = output_dir / dynamic_filename
        
        result.to_csv(full_output_path, index=False)
        logging.info(f"Saved {num_rows} records to {full_output_path}")
        
        # Also save a local copy for analysis
        local_csv_path = "allgigs_v7_processed.csv"
        result.to_csv(local_csv_path, index=False)
        logging.info(f"Saved local copy to {local_csv_path}")
        
        # Only upload to Supabase if there are no errors, source failures, or broken URLs
        if error_messages or source_failures or broken_urls:
            if broken_urls:
                logging.error("Upload to Supabase skipped due to broken URLs. See error summary above.")
            else:
                logging.error("Upload to Supabase skipped due to errors. See error summary above.")
        else:
            # Upload to both Supabase tables with transaction-like behavior
            # Both tables must succeed together, or both fail together
            upload_results = []
            upload_successful = True
            
            # First, attempt to upload to NEW table
            try:
                logging.info(f"Attempting to upload to {NEW_TABLE}...")
                upload_result_new = supabase_upload(result, NEW_TABLE, is_historical=False)
                upload_results.append(upload_result_new)
                if upload_result_new.get('status') == 'Failed':
                    upload_successful = False
                    logging.error(f"NEW table upload failed: {upload_result_new.get('error', 'Unknown error')}")
            except Exception as e:
                upload_successful = False
                upload_results.append({
                    'table_name': NEW_TABLE,
                    'status': 'Failed',
                    'error': str(e)
                })
                logging.error(f"Exception during NEW table upload: {str(e)}")
            
            # Only proceed to historical table if NEW table succeeded
            if upload_successful:
                try:
                    logging.info(f"NEW table upload successful. Attempting to upload to {HISTORICAL_TABLE}...")
                    upload_result_historical = supabase_upload(result, HISTORICAL_TABLE, is_historical=True)
                    upload_results.append(upload_result_historical)
                    if upload_result_historical.get('status') == 'Failed':
                        upload_successful = False
                        logging.error(f"HISTORICAL table upload failed: {upload_result_historical.get('error', 'Unknown error')}")
                except Exception as e:
                    upload_successful = False
                    upload_results.append({
                        'table_name': HISTORICAL_TABLE,
                        'status': 'Failed',
                        'error': str(e)
                    })
                    logging.error(f"Exception during HISTORICAL table upload: {str(e)}")
            else:
                # NEW table failed, so skip historical table and add a placeholder
                upload_results.append({
                    'table_name': HISTORICAL_TABLE,
                    'status': 'Skipped',
                    'error': 'Skipped because NEW table upload failed'
                })
                logging.warning(f"Skipping {HISTORICAL_TABLE} upload because {NEW_TABLE} upload failed")
            
            # Print Supabase upload results table
            print_supabase_upsert_table(upload_results)

            # Upload processing results to Supabase
            upload_processing_results_to_supabase()

            # Log the transaction result
            if upload_successful:
                logging.info("✅ Both tables uploaded successfully - transaction completed")
            else:
                logging.error("❌ Upload transaction failed - no tables were updated")
        
    except Exception as e:
        msg = f"FAILED: Main process - {str(e)}"
        logging.error(msg)
        error_messages.append(msg)
        source_failures.append(("Main process", str(e)))
        raise
    finally:
        # Print/log concise error summary
        if error_messages or broken_urls or source_failures:
            from collections import Counter
            error_counts = Counter(error_messages)
            logging.info("\n--- Error Summary ---")
            for err, count in error_counts.items():
                logging.info(f"{err} ({count} time{'s' if count > 1 else ''})")
            if broken_urls:
                logging.info("\n--- Broken URLs ---")
                for company, url in broken_urls:
                    logging.info(f"{company}: {url}")
            if source_failures:
                logging.info("\n--- Source Failure Summary ---")
                from collections import defaultdict
                fail_dict = defaultdict(list)
                for company, reason in source_failures:
                    fail_dict[company].append(reason)
                for company, reasons in fail_dict.items():
                    for reason in set(reasons):
                        logging.info(f"{company}: {reason} ({reasons.count(reason)} time{'s' if reasons.count(reason) > 1 else ''})")
        else:
            logging.info("\n--- Error Summary ---\nNo errors occurred.")
        
        # Add session summary to log
        end_time = time.time()
        duration = end_time - start_time
        
        session_summary = [
            "",
            "="*120,
            "📊 RECENT ALLGIGS V7 RUN SUMMARY",
            "="*120,
            f"📅 DATE: {datetime.now().strftime('%Y-%m-%d')}",
            f"⏰ TIME: {datetime.now().strftime('%H:%M:%S')}",
            f"⏱️  DURATION: {duration:.1f} seconds ({duration/60:.1f} minutes)",
            "="*120,
            ""
        ]
        
        # Determine overall status
        if error_messages or source_failures or broken_urls:
            session_summary.append("❌ PROCESSING COMPLETED WITH ERRORS")
            if upload_results and any(r.get('status') == 'Failed' for r in upload_results):
                session_summary.append("❌ SUPABASE UPLOAD FAILED")
            session_summary.append("📁 DATA SAVED TO LOCAL BATCH FILES")
        else:
            session_summary.append("✅ PROCESSING COMPLETED SUCCESSFULLY")
            if upload_results and all(r.get('status') == 'Success' for r in upload_results):
                session_summary.append("✅ SUPABASE UPLOAD SUCCESSFUL")
            else:
                session_summary.append("❌ SUPABASE UPLOAD FAILED")
            session_summary.append("📁 DATA SAVED TO LOCAL BATCH FILES")
        
        session_summary.extend([
            "",
            "📈 PROCESSING RESULTS:",
            f"• Total Records Processed: {len(result) if 'result' in locals() else 0} across {len(processing_results)} batch files",
            f"• Batch Files Created: {len(processing_results)}",
            ""
        ])
        
        if error_messages:
            session_summary.extend([
                "🚨 UPLOAD ISSUE DETAILS:",
                "• Error: Various processing errors occurred",
                "• Impact: Some data may not have been processed correctly",
                "• Resolution: Check error summary above for details"
            ])
        
        session_summary.extend([
            "",
            f"📅 Previous Session: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "="*120,
            ""
        ])
        
        for line in session_summary:
            write_to_log_and_console(line)

if __name__ == "__main__":
    main() 
    