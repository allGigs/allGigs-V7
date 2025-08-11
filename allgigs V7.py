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
    Logic: Assume Dutch by default unless we find clear evidence of non-Dutch origin.
    
    Args:
        location (str): The location string to categorize
        rate (str, optional): The rate/salary string to check for currency indicators
        
    Returns:
        Dict[str, bool]: Dictionary with 'Dutch', 'EU', 'Rest_of_World' as keys
    """
    if pd.isna(location) or location == '':
        location_clean = ''
    else:
        location_clean = str(location).lower().strip()
    
    # 1. First check for clear EU countries and cities (highest priority)
    eu_countries_excluding_nl = [
        'germany', 'france', 'italy', 'spain', 'poland', 'belgium', 'austria',
        'sweden', 'denmark', 'finland', 'portugal', 'greece', 'czech republic',
        'czechia', 'hungary', 'romania', 'bulgaria', 'croatia', 'slovakia',
        'slovenia', 'lithuania', 'latvia', 'estonia', 'ireland', 'luxembourg',
        'malta', 'cyprus'
    ]
    
    # Major EU cities (when country is not specified)
    major_eu_cities = [
        'berlin', 'munich', 'hamburg', 'cologne', 'frankfurt',  # Germany
        'paris', 'marseille', 'lyon', 'toulouse', 'nice',       # France
        'rome', 'milan', 'naples', 'turin', 'florence',         # Italy
        'madrid', 'barcelona', 'valencia', 'seville',           # Spain
        'warsaw', 'krakow', 'gdansk', 'wroclaw',                # Poland
        'brussels', 'antwerp', 'ghent', 'bruges',               # Belgium
        'vienna', 'salzburg', 'innsbruck',                      # Austria
        'stockholm', 'gothenburg', 'malmö',                     # Sweden
        'copenhagen', 'aarhus', 'odense', 'københavn',          # Denmark
        'helsinki', 'espoo', 'tampere',                         # Finland
        'lisbon', 'porto', 'braga',                             # Portugal
        'athens', 'thessaloniki', 'patras',                     # Greece
        'prague', 'brno', 'ostrava',                            # Czech Republic
        'budapest', 'debrecen', 'szeged',                       # Hungary
        'bucharest', 'cluj-napoca', 'timișoara',                # Romania
        'sofia', 'plovdiv', 'varna',                            # Bulgaria
        'zagreb', 'split', 'rijeka',                            # Croatia
        'bratislava', 'košice',                                  # Slovakia
        'ljubljana', 'maribor',                                  # Slovenia
        'vilnius', 'kaunas', 'klaipėda',                        # Lithuania
        'riga', 'daugavpils', 'liepāja',                        # Latvia
        'tallinn', 'tartu', 'narva',                            # Estonia
        'dublin', 'cork', 'limerick',                           # Ireland
        'luxembourg', 'esch-sur-alzette',                       # Luxembourg
        'valletta', 'birkirkara',                               # Malta
        'nicosia', 'limassol', 'larnaca'                        # Cyprus
    ]
    
    if location_clean:
        for country in eu_countries_excluding_nl:
            if country in location_clean:
                return {'Dutch': False, 'EU': True, 'Rest_of_World': False}
        
        for city in major_eu_cities:
            if city in location_clean:
                return {'Dutch': False, 'EU': True, 'Rest_of_World': False}
    
    # 2. Check for "European Union" specifically mentioned
    if location_clean and 'european union' in location_clean:
        return {'Dutch': False, 'EU': True, 'Rest_of_World': False}
    
    # 3. Check for clear non-EU countries in location
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
    
    # 4. Check for USD currency (only after location checks)
    if rate and not pd.isna(rate):
        rate_str = str(rate).lower().strip()
        usd_indicators = ['$', 'usd', 'dollar', '$/hr', '$/hour', '$/day', '$/month']
        if any(indicator in rate_str for indicator in usd_indicators):
            return {'Dutch': False, 'EU': False, 'Rest_of_World': True}
    
    # 5. If we reach here, assume Dutch by default
    # This covers all Dutch locations, Dutch remote jobs, and ambiguous cases
    # Dutch jobs are exclusively Dutch, not EU
    return {'Dutch': True, 'EU': False, 'Rest_of_World': False}

def add_regional_columns(df: pd.DataFrame, location_column: str = 'Location') -> pd.DataFrame:
    """
    Add regional categorization columns to a DataFrame.
    Uses "Dutch by default" approach with context-based categorization.
    
    Args:
        df (pd.DataFrame): The DataFrame to add columns to
        location_column (str): The name of the location column to analyze
        
    Returns:
        pd.DataFrame: DataFrame with new regional columns added
    """
    if location_column not in df.columns:
        logging.warning(f"Column '{location_column}' not found in DataFrame")
        return df
    
    # Create a copy to avoid modifying the original
    df_copy = df.copy()
    
    # Apply categorization to each row with all available context
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
    
    # Extract the boolean values into separate columns
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
    'LinkIT': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'Title1',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'LinkIT',
        'Source': 'LinkIT'
    },
    'freelance.nl': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'See Vacancy',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Source': 'freelance.nl'
    },
    'Yacht': {
        'Title': 'Field1',
        'Location': 'Field2',
        'Summary': 'Text2',
        'URL': 'https://yachtfreelance.talent-pool.com/projects?openOnly=true&page=1',
        'start': 'Field3',
        'rate': 'Text',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Source': 'Yacht'
    },
    'Flextender': {
        'Title': 'Field2',
        'Location': 'Field1',
        'Summary': 'See Vacancy',
        'URL': 'URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Source': 'Flextender'
    },
    'KVK': {
        'Title': 'Title',
        'Location': 'Amsterdam',
        'Summary': 'See Vacancy',
        'URL': 'https://www.kvkhuurtin.nl/opdrachten',
        'start': 'Title1',
        'rate': 'Title3',
        'Hours': 'Not mentioned',
        'Duration': 'Title1',
        'Company': 'KVK',
        'Source': 'KVK'
    },
    'Circle8': {
        'Title': 'Title',
        'Location': 'cvacancygridcard_usp',
        'Summary': 'See Vacancy',
        'URL': 'Title_URL',
        'start': 'Date',
        'rate': 'Not mentioned',
        'Hours': 'cvacancygridcard_usp2',
        'Duration': 'cvacancygridcard_usp1',
        'Company': 'Not mentioned',
        'Source': 'Circle8'
    },
    'Bebee': {
        'Title': 'Field1_text',
        'Location': 'Info1',
        'Summary': 'Text',
        'URL': 'Field2_links',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Info',
        'Source': 'Bebee'
    },
    'LinkedIn': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'About_the_job',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Company',
        'Source': 'LinkedIn'
    },
    'LinkedInZZP': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'About_the_job',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Company',
        'Source': 'LinkedIn'
    },
    'LinkedInInterim': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'About_the_job',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Company',
        'Source': 'LinkedIn'
    },
    'politie': {
        'Title': 'Field1',
        'Location': 'Hilversum',
        'Summary': 'Text',
        'URL': 'URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'politie',
        'Source': 'politie'
    },
    'gelderland': {
        'Title': 'Title',
        'Location': 'Gelderland',
        'Summary': 'See Vacancy',
        'URL': 'https://www.werkeningelderland.nl/inhuur/',
        'start': 'vacancy_details1',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'vacancy_details3',
        'Company': 'Not mentioned',
        'Source': 'gelderland'
    },
    'werk.nl': {
        'Title': 'Title',
        'Location': 'Description1',
        'Summary': 'See Vacancy',
        'URL': 'https://www.werk.nl/werkzoekenden/vacatures/',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Title3',
        'Duration': 'Not mentioned',
        'Company': 'Description_split',  # Will be split    
        'Source': 'werk.nl'
    },
    'indeed': {
        'Title': 'Title',
        'Location': 'css1restlb',
        'Summary': 'csso11dc0',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'css18z4q2i',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'css1h7lukg',
        'Source': 'indeed'
    },
    'Planet Interim': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'text',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Price',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Source': 'Planet Interim'
    },
    'NS': {
        'Title': 'Field1',
        'Location': 'Field2',
        'Summary': 'See Vacancy',
        'URL': 'https://www.werkenbijns.nl/vacatures?keywords=Inhuur',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'NS',
        'Source': 'NS'
    },
    'hoofdkraan': {
        'Title': 'Title',
        'Location': 'colmd4',
        'Summary': 'Description',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'fontweightbold',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Source': 'hoofdkraan'
        },
    
    # 'Zoekklus': {
    #     'Title': 'Field',
    #     'URL': 'Field3_links',
    #     'Location': 'Not mentioned',
    #     'Summary': 'Not mentioned',
    #     'rate': 'Not mentioned',
    #     'Hours': 'Not mentioned',
    #     'Duration': 'Not mentioned',
    #     'Company': 'Not mentioned',
    #     'Source': 'Zoekklus'
    # },
    
    'Overheid': {
        'Title': 'Title',
        'Location': 'Content3',
        'Summary': 'Keywords',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Content',
        'Hours': 'Content2',
        'Duration': 'Not mentioned',
        'Company': 'Description',
        'Source': 'Overheid'
    },
    'rijkswaterstaat': {
        'Title': 'widgetheader',
        'Location': 'feature1',
        'Summary': 'See Vacancy',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'feature2',
        'Hours': 'feature',
        'Duration': 'Not mentioned',
        'Company': 'Rijkswaterstaat',
        'Source': 'rijkswaterstaat'
    },
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
        'Source': 'zzp opdrachten'
    },
    'Harvey Nash': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'Not mentioned',
        'URL': 'Title_URL',
        'start': 'Field1',
        'rate': 'Salary',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Field2',
        'Source': 'Harvey Nash'
    },
    'Behance': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'Description',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Company',
        'Source': 'Behance'
    },
    'Schiphol': {
        'Title': 'Field2',
        'Location': 'Hybrid',
        'Summary': 'Text5',
        'URL': 'Field1_links',
        'start': 'ASAP',
        'rate': 'Text2',  # Will be processed to check Text2 and Text4
        'Hours': '36',
        'Duration': 'Not mentioned',
        'Company': 'Schiphol',
        'Source': 'Schiphol'
    },
    'Jooble': {
        'Title': 'Keywords',
        'Location': 'caption',
        'Summary': 'geyos4,geyos41,geyos42,geyos43',
        'URL': 'Time_links',
        'start': 'ASAP',
        'rate': 'not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'z6wlhx',
        'Source': 'Jooble'
    },
    'werkzoeken.nl': {
        'Title': 'Title',
        'Location': 'Company_name1',
        'Summary': 'See Vacancy',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'offer',  # Will be processed to remove entries with "p/m"
        'Hours': 'requestedwrapper',
        'Duration': 'Not mentioned',
        'Company': 'Company_name',  # Will be processed to remove everything before "• "
        'Source': 'werkzoeken.nl'
    },
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
        'Source': 'UMC'
    },
    'FlexValue_B.V.': {
        'Title': 'Title',
        'Location': 'scjnlklf',
        'Summary': 'See Vacancy',
        'URL': 'Title_URL',
        'start': 'Date1',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Source': 'FlexValue_B.V.'
    },
    'Centric': {
        'Title': 'Field1',
        'Location': 'Field2',
        'Summary': 'See Vacancy',
        'URL': 'URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Centric',
        'Source': 'Centric'
    },
    'freelancer.com': {
        'Title': 'Like',
        'Location': 'Remote',
        'Summary': 'Description',
        'URL': 'Like_URL',
        'start': 'ASAP',
        'rate': 'Price',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Source': 'freelancer.com'
    },
    'freelancer.nl': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'cardtext',
        'URL': 'cardlink_URL',
        'start': 'ASAP',
        'rate': 'budget',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Source': 'freelancer.nl'
    },
    'Salta Group': {
        'Title': 'Keywords',
        'URL': 'Title_URL',
        'Location': 'Location',
        'Summary': 'feature_spancontainsclass_text',
        'Company': 'Company',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'start': 'ASAP',
        'Source': 'Salta Group'
    },
    'ProLinker.com': {
        'Title': 'Title',
        'Location': 'section3',
        'Summary': 'section',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Like',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Source': 'ProLinker.com'
    },
    'Flex West-Brabant': {
        'Title': 'Title',
        'Location': 'org',
        'Summary': 'See Vacancy',
        'URL': 'Title_URL',
        'start': 'Field3',
        'rate': 'Not mentioned',
        'Hours': 'Field3',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Source': 'Flex West-Brabant'
    },
    'Amstelveenhuurtin': {
        'Title': 'Title',
        'Location': 'searchresultorganisation',
        'Summary': 'See Vacancy',
        'URL': 'https://www.amstelveenhuurtin.nl/opdrachten',
        'start': 'Title5',
        'rate': 'Title3',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Source': 'Amstelveenhuurtin'
    },
    'noordoostbrabant': {
        'Title': 'Field1',
        'Location': 'Field2',
        'Summary': 'See Vacancy',
        'URL': 'https://inhuurdesk.werkeninnoordoostbrabant.nl/opdrachten#',
        'start': 'Field11',
        'rate': 'Field8',
        'Hours': 'Not mentioned',
        'Duration': 'Field11',
        'Company': 'noordoostbrabant',
        'Source': 'noordoostbrabant'
    },
    'flevoland': {
        'Title': 'Field1',
        'Location': 'Field2',
        'Summary': 'Field8',
        'URL': 'Field9_links',
        'start': 'Field3',
        'rate': 'Not mentioned',
        'Hours': 'Field5',
        'Company': 'Field6',
        'Source': 'flevoland'
    },
    'Noord-Holland': {
        'Title': 'Title',
        'Location': 'Not mentioned',
        'Summary': 'See Vacancy',
        'URL': 'https://inhuur.werkeninnoordhollandnoord.nl/opdrachten',
        'start': 'Title5',
        'rate': 'Title3',
        'Hours': 'Not mentioned',
        'Duration': 'Title5',
        'Company': 'searchresultorganisation',
        'Source': 'Noord-Holland'
    },
    'groningenhuurtin': {
        'Title': 'Title',
        'Location': 'Not mentioned',
        'Summary': 'See Vacancy',
        'URL': 'https://www.groningenhuurtin.nl/opdrachten',
        'start': 'Title5',
        'rate': 'Title3',
        'Hours': 'Not mentioned',
        'Duration': 'Title5',
        'Company': 'searchresultorganisation',
        'Source': 'groningenhuurtin'
    },
    'TalentenRegio': {
        'Title': 'Title',
        'Location': 'Title1',
        'Summary': 'See Vacancy',
        'URL': 'hiddenmobile_URL',
        'start': 'csscolumnflexer12',
        'rate': 'Not mentioned',
        'Hours': 'csscolumnflexer8',
        'Duration': 'csscolumnflexer4',
        'Company': 'Not mentioned',
        'Source': 'TalentenRegio'
    },
    'HintTech': {
        'Title': 'Title',
        'URL': 'Title_URL',
        'Company': 'vc_colmd6',
        'Hours': 'vc_colmd62',
        'rate': 'vc_colmd63',
        'Duration': 'vc_colmd64',
        'Location': 'vc_colmd61',
        'Summary': 'See Vacancy',
        'start': 'ASAP',
        'Source': 'HintTech'
    },
    'haarlemmermeerhuurtin': {
        'Title': 'Title',
        'rate': 'Title3',
        'Duration': 'Title5',
        'URL': 'https://www.haarlemmermeerhuurtin.nl/opdrachten',
        'Location': 'Not mentioned',
        'Summary': 'See Vacancy',
        'start': 'ASAP',
        'Hours': 'Not mentioned',
        'Company': 'Gemeente Haarlemmermeer',
        'Source': 'Haarlemmermeerhuurtin'
    },
    'Select': {
        'Title': 'Title',
        'Location': 'Location', 
        'Summary': 'See Vacancy',
        'URL': 'Title_URL',
        'start': 'Date',
        'rate': 'Not mentioned',
        'Hours': 'hfp_cardiconsblockitem',
        'Duration': 'Not mentioned',
        'Company': 'Company',
        'Source': 'Select'
    },
    'overheidzzp': {
        'Title': 'Title',
        'Location': 'Not mentioned',
        'Summary': 'See Vacancy',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Company_name',
        'Source': 'overheidzzp'
    },
    'POOQ': {
        'Title': 'Title',
        'Location': 'ml5',
        'Summary': 'See Vacancy',
        'URL': 'Title_URL',
        'start': 'hidden',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Source': 'POOQ'
    },
    'gemeente-projecten': {
        'Title': 'Field1',  # Will be processed to remove text in () and () themselves
        'Location': 'Field2',  # Will be processed to remove text in () and () themselves
        'Summary': 'Field8',
        'URL': 'URL',
        'start': 'ASAP',
        'rate': 'Field4',  # Will be processed to extract duration if text mentions "Voor"
        'Hours': 'Field3',
        'Duration': 'Field7',  # Will also receive extracted duration from rate field if applicable
        'Company': 'Not mentioned',
        'Source': 'gemeente-projecten'
    },
    'ggdzwhuurtin.nl': {
        'Title': 'Text',
        'Location': 'Not mentioned',
        'Summary': 'See Vacancy',
        'URL': 'https://www.ggdzwhuurtin.nl/opdrachten#',
        'start': 'ASAP',
        'rate': 'Text4',
        'Hours': 'Not mentioned',
        'Duration': 'Text6',  # Will be processed to calculate date difference in months
        'Company': 'GGD Zaanstreek-Waterland',
        'Source': 'ggdzwhuurtin.nl'
    },
    'freep': {
        'Title': 'Title',
        'Location': 'flex3',
        'Summary': 'See Vacancy',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'flex2',
        'Hours': 'flex4',
        'Duration': 'Not mentioned',
        'Company': 'mdcolspan2',
        'Source': 'freep'
    },
    'onlyhuman': {
        'Title': 'Title',
        'Location': 'flex3',
        'Summary': 'See Vacancy',
        'URL': 'URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'vacancycard',
        'Source': 'onlyhuman'
    },
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
        'Source': 'StaffingMS'
    },
    '4-Freelancers': {
        'Title': 'Functietitel',
        'URL': 'Functietitel_URL',
        'Location': 'Plaats',
        'Summary': 'Not mentioned',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Not mentioned',
        'Source': '4-Freelancers',
        'start': 'ASAP'
    },
    'flexSpot.io': {
        'Title': 'Title',
        'Location': 'reset',
        'Summary': 'See Vacancy',
        'URL': 'button_URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Company',
        'Source': 'flexSpot.io'
    },
    'ASNBank': {
        'Title': 'Field2',
        'Location': 'Text',  # Will be processed to extract first part after splitting on space
        'Summary': 'See Vacancy',
        'URL': 'Field1_links',
        'start': 'ASAP',
        'rate': 'Text',  # Will be processed to extract rate after "€"
        'Hours': 'Text',  # Will be processed to extract hours and remove "uur"
        'Duration': 'Not mentioned',
        'Company': 'ASN Bank',
        'Source': 'ASNBank'
    },
    'tennet': {
        'Title': 'widgetheader',
        'Location': 'feature1',
        'Summary': 'Title',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'feature2',
        'Hours': 'feature',
        'Duration': 'Not mentioned',
        'Company': 'TenneT',
        'Source': 'tennet'
    },
    'Interim-Netwerk': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'Summary',
        'URL': 'URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Duration',
        'Company': 'Interim-Netwerk',
        'Source': 'Interim-Netwerk'
    },
    'Friesland': {
        'Title': 'Title',
        'URL': 'Title_URL',
        'Location': 'caption',
        'Summary': 'Not mentioned',
        'start': 'Not mentioned',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Friesland',
        'Source': 'Friesland'
    },
    'Zuid-Holland': {
        'Title': 'Title',
        'URL': 'Title_URL',
        'Company': 'Company',
        'Location': 'Location',
        'Hours': 'hfp_cardiconsblockitem',
        'Summary': 'Not mentioned',
        'start': 'Not mentioned',
        'rate': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Source': 'Zuid-Holland'
    },
    'TechFreelancers': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'Description',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Salary',
        'Hours': 'Hours',
        'Duration': 'Duration',
        'Company': 'Company',
        'Source': 'TechFreelancers'
    },
    'InterimHub': {
        'Title': 'JobTitle',
        'Location': 'City',
        'Summary': 'JobDescription',
        'URL': 'ApplyURL',
        'start': 'StartDate',
        'rate': 'HourlyRate',
        'Hours': 'WorkHours',
        'Duration': 'ContractLength',
        'Company': 'ClientName',
        'Source': 'InterimHub'
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
            'freelancer': 'freelancer',
            'freelance': 'freelance',
            'linkedin': 'linkedin',
            'indeed': 'indeed',
            'prolinker': 'prolinker',
            'werkzoeken': 'werkzoeken',
            'werk': 'werk',
            'overheid': 'overheid',
            'yacht': 'yacht',
            'planet interim': 'planet_interim',
            'salta group': 'salta_group',
            'harvey nash': 'harvey_nash',
            'circle8': 'circle8',
            'behance': 'behance',
            'jooble': 'jooble',
            'centric': 'centric',
            'schiphol': 'schiphol',
            'rijkswaterstaat': 'rijkswaterstaat',
            'ns': 'ns',
            'umc': 'umc',
            'bebee': 'bebee',
            'hinttech': 'hinttech',
            'flextender': 'flextender',
            'select': 'select',
            'kvk': 'kvk',
            'politie': 'politie',
            'hoofdkraan': 'hoofdkraan',
            'zzp opdrachten': 'zzp_opdrachten',
            'flexvalue': 'flexvalue',
            'talentenregio': 'talentenregio',
            'linkit': 'linkit',
            'gelderland': 'gelderland',
            'noord-holland': 'noord_holland',
            'flevoland': 'flevoland',
            'groningen': 'groningen',
            'noordoostbrabant': 'noordoostbrabant',
            'flex west-brabant': 'flex_west_brabant',
            'amstelveen': 'amstelveen',
            'haarlemmermeerhuurtin': 'haarlemmermeer',
            'gemeente-projecten': 'gemeente_projecten',
            'ggdzwhuurtin': 'ggdz_whuurtin',
            'freep': 'freep',
            'onlyhuman': 'onlyhuman',
            'staffingms': 'staffingms',
            '4 freelancers': '4_freelancers',
            'flexspot': 'flexspot',
            'asnbank': 'asnbank',
            'tennet': 'tennet',
            'interim-netwerk': 'interim_netwerk',
            'zuid holland': 'zuid_holland',
            'linkedininterim': 'linkedininterim',
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
            if 'text' in files_read.columns:
                initial_count = len(files_read)
                # Filter for rows where 'text' column contains "Open" (case-insensitive)
                files_read = files_read[files_read['text'].astype(str).str.contains("Open", case=False, na=False)]
                filtered_count = len(files_read)
                if initial_count > filtered_count:
                    logging.info(f"Applied ProLinker.com pre-mapping filter on 'text' column for 'Open', rows changed from {initial_count} to {filtered_count}")
                elif initial_count == filtered_count and initial_count > 0:
                    logging.info(f"ProLinker.com pre-mapping filter: 'text' column checked for 'Open', no rows removed from {initial_count} rows.")
                # If initial_count is 0, no need to log anything specific beyond the standard read log
            else:
                logging.warning(f"ProLinker.com pre-mapping filter: 'text' column not found in the input CSV for {company_name}. Skipping filter.")
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
        elif company_name == 'Overheid':
            if 'Keywords' in files_read.columns:
                initial_count = len(files_read)
                # Filter out rows where 'Keywords' column contains "Loondienst" (case-insensitive)
                files_read = files_read[~files_read['Keywords'].astype(str).str.contains("Loondienst", case=False, na=False)]
                filtered_count = len(files_read)
                if initial_count > filtered_count:
                    logging.info(f"Applied Overheid pre-mapping filter on 'Keywords' column to remove 'Loondienst', rows changed from {initial_count} to {filtered_count}")
                elif initial_count == filtered_count and initial_count > 0:
                    logging.info(f"Overheid pre-mapping filter: 'Keywords' column checked for 'Loondienst', no rows removed from {initial_count} rows.")
            else:
                logging.warning(f"Overheid pre-mapping filter: 'Keywords' column not found in the input CSV for {company_name}. Skipping filter.")
        elif company_name == 'overheidzzp':
            if 'elementorelement' in files_read.columns:
                initial_count = len(files_read)
                # Filter for rows where 'elementorelement' column contains "ZZP" (case-insensitive)
                files_read = files_read[files_read['elementorelement'].astype(str).str.contains("ZZP", case=False, na=False)]
                filtered_count = len(files_read)
                if initial_count > filtered_count:
                    logging.info(f"Applied overheidzzp pre-mapping filter on 'elementorelement' column for 'ZZP', rows changed from {initial_count} to {filtered_count}")
                elif initial_count == filtered_count and initial_count > 0:
                    logging.info(f"overheidzzp pre-mapping filter: 'elementorelement' column checked for 'ZZP', no rows removed from {initial_count} rows.")
            else:
                logging.warning(f"overheidzzp pre-mapping filter: 'elementorelement' column not found in the input CSV for {company_name}. Skipping filter.")
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
        elif company_name == 'Interim-Netwerk':
            # Special processing for Interim-Netwerk: extract data from Text column
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
                            'Hours': 'Not mentioned',
                            'Company': 'Interim-Netwerk',
                            'Source': 'Interim-Netwerk'
                        })
                    
                    # Convert to DataFrame
                    files_read = pd.DataFrame(processed_data)
                    logging.info(f"Interim-Netwerk special processing: Created {len(files_read)} rows from {len(jobs)} job blocks")
                else:
                    logging.warning("Interim-Netwerk: No data found in CSV file")
                    files_read = pd.DataFrame()
            else:
                logging.warning("Interim-Netwerk: Required columns 'Text' and 'Field1_links' not found")
                files_read = pd.DataFrame()
        
        # Create a new DataFrame with standardized columns
        result = pd.DataFrame()
        
        # Map the columns according to the mapping
        for std_col, src_col_mapping_value in mapping.items():
            if src_col_mapping_value in files_read.columns:
                if company_name == 'werk.nl' and std_col == 'Company' and src_col_mapping_value == 'Description_split':
                    # Special handling for werk.nl: split Description on "-" and use first part as Company
                    result[std_col] = files_read['Description'].str.split('-').str[0].str.strip()
                else:
                    result[std_col] = files_read[src_col_mapping_value]
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
                common_column_names = {'Text2', 'Location', 'searchresultorganisation', 'Field1', 'Field4'}
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
        standard_columns = ['Title', 'Location', 'Summary', 'URL', 'start', 'rate', 'Company']
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
        if company_name == 'Haarlemmermeerhuurtin':
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
                logging.info(f"Haarlemmermeerhuurtin post-mapping: Processed rate field to remove 'per uur'")
            
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
                logging.info(f"Haarlemmermeerhuurtin post-mapping: Processed Duration field to calculate date differences")
        
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
        if company_name == 'gemeente projecten':
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
                logging.info(f"gemeente projecten post-mapping: Processed Title field to remove text in parentheses")
            
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
                logging.info(f"gemeente projecten post-mapping: Processed Location field to remove text in parentheses")
            
            # Process rate field - extract duration if text mentions "Voor"
            if 'rate' in result.columns and 'Duration' in result.columns:
                def process_rate_gemeente(index):
                    rate_val = result.iloc[index]['rate'] if 'rate' in result.columns else None
                    duration_val = result.iloc[index]['Duration'] if 'Duration' in result.columns else None
                    
                    if pd.isna(rate_val) or rate_val == '':
                        return 'Not mentioned', duration_val
                    
                    import re
                    rate_str = str(rate_val).strip()
                    
                    # Look for text that mentions "Voor" (case-insensitive)
                    if 'voor' in rate_str.lower():
                        # If "Voor" is mentioned, use the entire rate text as duration
                        final_duration = rate_str if pd.isna(duration_val) or duration_val == '' or duration_val == 'Not mentioned' else duration_val
                        
                        # Clear the rate field since it's being used for duration
                        return 'Not mentioned', final_duration
                    
                    return rate_str, duration_val
                
                # Apply the processing to each row
                processed_data = [process_rate_gemeente(i) for i in range(len(result))]
                rate_values = [item[0] for item in processed_data]
                duration_values = [item[1] for item in processed_data]
                
                result['rate'] = rate_values
                result['Duration'] = duration_values
                logging.info(f"gemeente projecten post-mapping: Processed rate field to extract duration when 'Voor' is mentioned")
        
        # GGDZWHUURTIN.NL POST-MAPPING PROCESSING
        if company_name == 'ggdzwhuurtin.nl':
            # Process Duration field - calculate difference between first and second date in months
            if 'Duration' in result.columns:
                def process_duration_ggdz(duration_str):
                    if pd.isna(duration_str) or duration_str == '':
                        return 'Not mentioned'
                    
                    try:
                        import re
                        from datetime import datetime
                        
                        duration_clean = str(duration_str).strip()
                        
                        # Common separators for date ranges (including Dutch separators)
                        separators = [' t/m ', ' to ', ' - ', ' tot ', ' until ', ' through ', ' tm ', ' t.m. ']
                        
                        for sep in separators:
                            if sep in duration_clean.lower():
                                parts = duration_clean.lower().split(sep)
                                if len(parts) == 2:
                                    start_date_str = parts[0].strip()
                                    end_date_str = parts[1].strip()
                                    
                                    # Try to parse dates with common formats
                                    date_formats = ['%d-%m-%Y', '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d', '%d.%m.%Y']
                                    
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
                                        diff_days = abs((end_date - start_date).days)
                                        if diff_days > 0:
                                            # Convert to months (using 30.44 days per month for accuracy)
                                            months = round(diff_days / 30.44)
                                            if months == 0:
                                                months = 1  # Minimum 1 month for any duration
                                            
                                            if months == 1:
                                                return "1 month"
                                            else:
                                                return f"{months} months"
                                    break
                        
                        return duration_clean  # Return original if can't parse
                    except Exception:
                        return 'Not mentioned'
                
                result['Duration'] = result['Duration'].apply(process_duration_ggdz)
                logging.info(f"ggdzwhuurtin.nl post-mapping: Processed Duration field to calculate date differences in months")
        
        # 4 FREELANCERS POST-MAPPING PROCESSING
        if company_name == '4 Freelancers':
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
                logging.info(f"noordoostbrabant post-mapping: Processed start field to extract first date from Field11")
        
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
            
            # Process Duration field - calculate months between the first and second date from Title5
            if 'Duration' in result.columns:
                def process_duration_amstelveenhuurtin(duration_str):
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
                        logging.warning(f"Error processing amstelveenhuurtin duration '{duration_str}': {e}")
                        return duration_clean
                
                result['Duration'] = result['Duration'].apply(process_duration_amstelveenhuurtin)
                logging.info(f"Amstelveenhuurtin post-mapping: Processed Duration field to calculate months between dates from Title5")
            
            # Process start field - extract the first date from Title5
            if 'start' in result.columns:
                def process_start_amstelveenhuurtin(start_str):
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
                        logging.warning(f"Error processing amstelveenhuurtin start date '{start_str}': {e}")
                        return 'ASAP'
                
                result['start'] = result['start'].apply(process_start_amstelveenhuurtin)
                logging.info(f"Amstelveenhuurtin post-mapping: Processed start field to extract first date from Title5")
        
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
        if company_name == 'Haarlemmermeerhuurtin':
            # Process Duration field - calculate months between the first and second date from Title5
            if 'Duration' in result.columns:
                def process_duration_haarlemmermeerhuurtin(duration_str):
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
                        logging.warning(f"Error processing Haarlemmermeerhuurtin duration '{duration_str}': {e}")
                        return duration_clean
                
                result['Duration'] = result['Duration'].apply(process_duration_haarlemmermeerhuurtin)
                logging.info(f"Haarlemmermeerhuurtin post-mapping: Processed Duration field to calculate months between dates from Title5")
            
            # Process start field - extract the first date from Title5
            if 'start' in result.columns:
                def process_start_haarlemmermeerhuurtin(start_str):
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
                        logging.warning(f"Error processing Haarlemmermeerhuurtin start date '{start_str}': {e}")
                        return 'ASAP'
                
                result['start'] = result['start'].apply(process_start_haarlemmermeerhuurtin)
                logging.info(f"Haarlemmermeerhuurtin post-mapping: Processed start field to extract first date from Title5")
        
        # TECHFREELANCERS POST-MAPPING PROCESSING
        if company_name == 'TechFreelancers':
            # Process Salary field - extract numeric values and add currency symbol
            if 'rate' in result.columns:
                def process_rate_techfreelancers(rate_str):
                    if pd.isna(rate_str) or rate_str == '':
                        return 'Not mentioned'
                    
                    rate_clean = str(rate_str).strip()
                    
                    # Extract numeric values (including decimals)
                    import re
                    numbers = re.findall(r'\d+(?:\.\d+)?', rate_clean)
                    
                    if numbers:
                        # Take the first number found and format as currency
                        try:
                            amount = float(numbers[0])
                            return f'€{amount:.0f}/hour'
                        except ValueError:
                            return rate_clean
                    else:
                        return rate_clean if rate_clean else 'Not mentioned'
                
                result['rate'] = result['rate'].apply(process_rate_techfreelancers)
                logging.info(f"TechFreelancers post-mapping: Processed rate field to extract numeric values and format as currency")
            
            # Process Hours field - standardize hour formats
            if 'Hours' in result.columns:
                def process_hours_techfreelancers(hours_str):
                    if pd.isna(hours_str) or hours_str == '':
                        return 'Not mentioned'
                    
                    hours_clean = str(hours_str).strip()
                    
                    # Extract numeric values
                    import re
                    numbers = re.findall(r'\d+', hours_clean)
                    
                    if numbers:
                        try:
                            hours = int(numbers[0])
                            return f'{hours} hours/week'
                        except ValueError:
                            return hours_clean
                    else:
                        return hours_clean if hours_clean else 'Not mentioned'
                
                result['Hours'] = result['Hours'].apply(process_hours_techfreelancers)
                logging.info(f"TechFreelancers post-mapping: Processed Hours field to standardize hour formats")
        
        # INTERIMHUB POST-MAPPING PROCESSING
        if company_name == 'InterimHub':
            # Process HourlyRate field - extract and format rate information
            if 'rate' in result.columns:
                def process_rate_interimhub(rate_str):
                    if pd.isna(rate_str) or rate_str == '':
                        return 'Not mentioned'
                    
                    rate_clean = str(rate_str).strip()
                    
                    # Look for common rate patterns
                    import re
                    
                    # Pattern for "€X/hour" or "€X per hour"
                    euro_pattern = re.search(r'€\s*(\d+(?:\.\d+)?)', rate_clean)
                    if euro_pattern:
                        amount = float(euro_pattern.group(1))
                        return f'€{amount:.0f}/hour'
                    
                    # Pattern for "X euro" or "X EUR"
                    number_pattern = re.search(r'(\d+(?:\.\d+)?)\s*(?:euro|EUR)', rate_clean, re.IGNORECASE)
                    if number_pattern:
                        amount = float(number_pattern.group(1))
                        return f'€{amount:.0f}/hour'
                    
                    # Pattern for just numbers (assume euros)
                    simple_number = re.search(r'(\d+(?:\.\d+)?)', rate_clean)
                    if simple_number:
                        amount = float(simple_number.group(1))
                        return f'€{amount:.0f}/hour'
                    
                    return rate_clean if rate_clean else 'Not mentioned'
                
                result['rate'] = result['rate'].apply(process_rate_interimhub)
                logging.info(f"InterimHub post-mapping: Processed rate field to extract and format rate information")
            
            # Process WorkHours field - standardize work hour formats
            if 'Hours' in result.columns:
                def process_hours_interimhub(hours_str):
                    if pd.isna(hours_str) or hours_str == '':
                        return 'Not mentioned'
                    
                    hours_clean = str(hours_str).strip()
                    
                    # Extract numeric values
                    import re
                    numbers = re.findall(r'\d+', hours_clean)
                    
                    if numbers:
                        try:
                            hours = int(numbers[0])
                            if hours <= 24:
                                return f'{hours} hours/week'
                            else:
                                return f'{hours} hours/month'
                        except ValueError:
                            return hours_clean
                    else:
                        return hours_clean if hours_clean else 'Not mentioned'
                
                result['Hours'] = result['Hours'].apply(process_hours_interimhub)
                logging.info(f"InterimHub post-mapping: Processed Hours field to standardize work hour formats")
            
            # Process ContractLength field - standardize duration formats
            if 'Duration' in result.columns:
                def process_duration_interimhub(duration_str):
                    if pd.isna(duration_str) or duration_str == '':
                        return 'Not mentioned'
                    
                    duration_clean = str(duration_str).strip()
                    
                    # Common duration patterns
                    import re
                    
                    # Pattern for months
                    months_pattern = re.search(r'(\d+)\s*(?:month|maand)', duration_clean, re.IGNORECASE)
                    if months_pattern:
                        months = int(months_pattern.group(1))
                        return f'{months} months'
                    
                    # Pattern for weeks
                    weeks_pattern = re.search(r'(\d+)\s*(?:week|weken)', duration_clean, re.IGNORECASE)
                    if weeks_pattern:
                        weeks = int(weeks_pattern.group(1))
                        return f'{weeks} weeks'
                    
                    # Pattern for days
                    days_pattern = re.search(r'(\d+)\s*(?:day|dagen)', duration_clean, re.IGNORECASE)
                    if days_pattern:
                        days = int(days_pattern.group(1))
                        return f'{days} days'
                    
                    return duration_clean if duration_clean else 'Not mentioned'
                
                result['Duration'] = result['Duration'].apply(process_duration_interimhub)
                logging.info(f"InterimHub post-mapping: Processed Duration field to standardize duration formats")
        
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

def print_simple_table():
    """Print processing results in enhanced table format with detailed reasons"""
    if not processing_results:
        return
    
    output = []
    output.append("\n" + "="*120)
    output.append("📊 DATA SOURCE PROCESSING RESULTS (Sorted by Success % - Lowest First)")
    output.append("="*120)
    
    # Enhanced header with status icons
    header = f"{'Source':<20} {'Status':<12} {'Initial':<8} {'Final':<8} {'Dropped':<8} {'Success %':<10} {'Detailed Reason':<50}"
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
        
        # Create detailed reason based on status and data
        if status == "Skipped":
            if initial == 0:
                detailed_reason = "No data found in source file"
            else:
                detailed_reason = f"All {initial} records filtered out during processing"
        elif status == "Failed":
            detailed_reason = f"Failed to process: {result.get('drop_reason', 'Unknown error')}"
        elif status == "Success":
            if dropped == 0:
                detailed_reason = "All records successfully processed (100% retention)"
            else:
                detailed_reason = f"{dropped} records dropped due to validation/duplicate detection"
        else:
            detailed_reason = result.get('drop_reason', 'Unknown status')
        
        enhanced_results.append({
            'company': company,
            'status': status,
            'initial': initial,
            'final': final,
            'dropped': dropped,
            'success_pct': success_pct,
            'detailed_reason': detailed_reason
        })
    
    # Sort by success percentage (lowest first)
    enhanced_results.sort(key=lambda x: x['success_pct'])
    
    # Data rows with better formatting
    for result in enhanced_results:
        company = result['company'][:19]  # Truncate if too long
        status = result['status']
        initial = result['initial']
        final = result['final']
        dropped = result['dropped']
        success_pct = result['success_pct']
        detailed_reason = result['detailed_reason'][:49]  # Truncate reason
        
        # Status icons
        if status == "Success":
            status_icon = "✅"
        elif status == "Failed":
            status_icon = "❌"
        elif status == "Skipped":
            status_icon = "⏭️"
        else:
            status_icon = "❓"
        
        row = f"{company:<20} {status_icon} {status:<8} {initial:<8} {final:<8} {dropped:<8} {success_pct:>7.1f}% {detailed_reason:<50}"
        output.append(row)
    
    output.append("-" * 120)
    
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
        print_simple_table()
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
        
        # Only upload to Supabase if there are no errors, source failures, or broken URLs
        if error_messages or source_failures or broken_urls:
            if broken_urls:
                logging.error("Upload to Supabase skipped due to broken URLs. See error summary above.")
            else:
                logging.error("Upload to Supabase skipped due to errors. See error summary above.")
        else:
            # Upload to both Supabase tables and collect results
            upload_results = []
            try:
                upload_result_new = supabase_upload(result, NEW_TABLE, is_historical=False)
                upload_results.append(upload_result_new)
            except Exception as e:
                upload_results.append({
                    'table_name': NEW_TABLE,
                    'status': 'Failed',
                    'error': str(e)
                })
            
            try:
                upload_result_historical = supabase_upload(result, HISTORICAL_TABLE, is_historical=True)
                upload_results.append(upload_result_historical)
            except Exception as e:
                upload_results.append({
                    'table_name': HISTORICAL_TABLE,
                    'status': 'Failed',
                    'error': str(e)
                })
            
            # Print Supabase upload results table
            print_supabase_upsert_table(upload_results)
        
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
    