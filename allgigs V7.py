import os
import pandas as pd
from datetime import datetime
import uuid
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
        logging.FileHandler('allgigs.log'),
        logging.StreamHandler()
    ]
)

# Suppress HTTP and verbose logs
import logging as _logging
_logging.getLogger("httpx").setLevel(_logging.WARNING)
_logging.getLogger("requests").setLevel(_logging.WARNING)
_logging.getLogger("supabase_py").setLevel(_logging.WARNING)

# Load environment variables
load_dotenv()

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
OUTPUT_FILE = "/Users/jaapjanlammers/Library/CloudStorage/GoogleDrive-jj@nineways.nl/My Drive/allGigs_log/allgigs.csv"

# Directory structure
BASE_DIR = Path('/Users/jaapjanlammers/Desktop/Freelancedirectory')
FREELANCE_DIR = BASE_DIR / 'Freelance Directory'
IMPORTANT_DIR = BASE_DIR / 'Important_allGigs'
# AUTOMATION_DETAILS_PATH = IMPORTANT_DIR / 'automation_details.csv' # Commented out as it's fetched from Supabase

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
        'Company': 'freelance.nl',
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
        'Company': 'Yacht Freelance',
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
        'Company': 'Flextender',
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
    'Cirle8': {
        'Title': 'Title',
        'Location': 'cvacancygridcard_usp',
        'Summary': 'See Vacancy',
        'URL': 'Title_URL',
        'start': 'Date',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Circle8',
        'Source': 'Circle8'
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
        'start': 'vacancy_details5',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'gelderland',
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
        'Company': 'Description_split',  # Will be split on "-" and use first part
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
        'Company': 'Planet Interim',
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
        'Company': 'hoofdkraan',
        'Source': 'hoofdkraan'
    },
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
        'Hours': 'Not mentioned',
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
        'Summary': 'Field4',
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
        'Summary': 'Text4',
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
        'Title': '_8w9ce2',
        'Location': 'caption',
        'Summary': 'geyos4',
        'URL': '_8w9ce2_URL',
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
        'Company': 'FlexValue_B.V.',
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
        'Title': 'Title',
        'Location': 'Remote',
        'Summary': 'Description',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Price',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'freelancer.com',
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
        'Company': 'freelancer.nl',
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
        'Company': 'ProLinker.com',
        'Source': 'ProLinker.com'
    },
    'Flex West-Brabant': {
        'Title': 'Title',
        'Location': 'org',
        'Summary': 'See Vacancy',
        'URL': 'Title_URL',
        'start': 'Field3',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Flex West-Brabant',
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
        'Company': 'Amstelveenhuurtin',
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
        'Duration': 'Not mentioned',
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
        'Duration': 'Not mentioned',
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
        'Duration': 'searchresultorganisation',
        'Company': 'TalentenRegio',
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
    'Haarlemmermeerhuurtin': {
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
    'hoofdkraan': {
        'Title': 'Title',
        'Location': 'colmd4',
        'Summary': 'See Vacancy',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'hoofdkraan',
        'Source': 'hoofdkraan'
    },
    'Harvey Nash': {
        'Title': 'Title',
        'Location': 'Not mentioned',
        'Summary': 'See Vacancy',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Harvey Nash',
        'Source': 'Harvey Nash'
    },
    'Behance': {
        'Title': 'Title',
        'Location': 'Not mentioned',
        'Summary': 'Text4',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'Not mentioned',
        'Duration': 'Not mentioned',
        'Company': 'Behance',
        'Source': 'Behance'
    },
    'Cirle8': {
        'Title': 'Title',
        'Location': 'Not mentioned',
        'Summary': 'See Vacancy',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Hours': 'cvacancygridcard_usp2',
        'Duration': 'cvacancygridcard_usp1',
        'Company': 'Circle8',
        'Source': 'Circle8'
    }
}

def timestamp():
    """Get current timestamp in YYYY-MM-DD format."""
    return datetime.now().strftime("%Y-%m-%d")

def generate_unique_id(title, url, company):
    """Generate a unique ID based on the combination of title, URL, and company."""
    combined = f"{title}|{url}|{company}".encode('utf-8')
    return hashlib.md5(combined).hexdigest()

def generate_group_id(title):
    """Generate a group ID based on title for grouping similar jobs."""
    combined = f"{title}".encode('utf-8')
    return hashlib.md5(combined).hexdigest()

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
                common_column_names = {'Text2', 'Location', 'searchresultorganisation'}
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
        
        # Remove validation and cleaning logic - just return the processed data
        logging.info(f"Successfully processed data for {company_name}")
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
    df['date'] = timestamp()
    
    # Add detailed group_id logging
    logging.info("=" * 80)
    logging.info("group_id ANALYSIS")
    logging.info("=" * 80)
    
    # Group by group_id and count rows per group
    group_counts = df.groupby('group_id').size().reset_index(name='count')
    
    # Find groups with multiple entries (duplicate titles)
    duplicate_groups = group_counts[group_counts['count'] > 1]
    
    if not duplicate_groups.empty:
        logging.info(f"Found {len(duplicate_groups)} groups with duplicate titles:")
        
        # Get detailed info for duplicate groups
        for _, group_info in duplicate_groups.head(10).iterrows():  # Show first 10 groups
            group_id = group_info['group_id']
            count = group_info['count']
            
            # Get all jobs in this group
            group_jobs = df[df['group_id'] == group_id]
            unique_titles = group_jobs['Title'].unique()
            unique_locations = group_jobs['Location'].unique()
            unique_companies = group_jobs['Company'].unique()
            
            logging.info(f"group_id: {group_id} ({count} jobs)")
            logging.info(f"  Title: {unique_titles[0]}")  # All titles should be the same
            logging.info(f"  Locations: {list(unique_locations)}")
            logging.info(f"  Companies: {list(unique_companies)}")
            logging.info("-" * 40)
        
        if len(duplicate_groups) > 10:
            logging.info(f"... and {len(duplicate_groups) - 10} more groups")
    else:
        logging.info("No duplicate titles found - all jobs have unique titles")
    
    logging.info("=" * 80)
    
    # If we have historical data, preserve dates for existing records
    if historical_data is not None and not historical_data.empty:
        for idx in df.index:
            if df.loc[idx, 'UNIQUE_ID'] in set(historical_data['UNIQUE_ID']):
                df.loc[idx, 'date'] = historical_data[
                    historical_data['UNIQUE_ID'] == df.loc[idx, 'UNIQUE_ID']
                ].iloc[0]['date']
    
    # Remove duplicates using UNIQUE_ID
    duplicates_count = df.duplicated(subset=['UNIQUE_ID']).sum()
    if duplicates_count > 0:
        logging.info(f"Removing {duplicates_count} duplicates from new data based on UNIQUE_ID")
        df = df.drop_duplicates(subset=['UNIQUE_ID'], keep='first')
    
    return df

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

        # For historical table, remove group_id column if it exists
        if is_historical and 'group_id' in df.columns:
            logging.info(f"Removing group_id column for historical table upload to {table_name}")
            df = df.drop(columns=['group_id'])
        
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
    except Exception as e:
        logging.error(f"Failed to upload data to Supabase: {str(e)}")
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

def main():
    error_messages = []  # Collect error messages for summary
    broken_urls = []     # Collect broken URLs for summary
    source_failures = [] # Collect (company, reason) for summary
    try:
        start_time = time.time()
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

                # Add a clear title before processing each company
                logging.info(f"\n========== Processing: {company_name} ==========")
                logging.info(f"Source CSV file/URL: {url_link}")
                
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
                    logging.info("") # Add an empty line for separation
                    continue # Skip to the next company

                if not read_successful: # Implies all separators failed with "other" exceptions
                    msg = f"FAILED: {company_name} - Could not read or parse CSV ({url_link}) after trying all separators."
                    logging.error(msg)
                    error_messages.append(msg)
                    broken_urls.append((company_name, url_link))
                    source_failures.append((company_name, f"Could not read or parse CSV ({url_link})"))
                    logging.info("") # Add an empty line
                    continue
                
                # If we reach here, files_read is a populated DataFrame
                logging.info(f"Successfully read data for {company_name} with {len(files_read)} rows")
                company_df = freelance_directory(files_read, company_name)

                if company_df.empty:
                    logging.info(f"INFO: {company_name} - No data remained after processing and cleaning. Skipping.")
                    logging.info("") # Add an empty line for separation
                    continue # Skip to the next company if no data after cleaning

                # If we reach here, company_df is not empty
                result = pd.concat([result, company_df], ignore_index=True)
                logging.info(f"Processed {company_name}: {len(company_df)} rows")
                # Add an empty line after each company
                logging.info("")
            
            except Exception as e:
                msg = f"FAILED: {company_name} - {str(e)}"
                logging.error(msg)
                error_messages.append(msg)
                source_failures.append((company_name, str(e)))
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
        result = prepare_data_for_upload(result, historical_data)
        
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
            # Upload to both Supabase tables
            supabase_upload(result, NEW_TABLE, is_historical=False)
            supabase_upload(result, HISTORICAL_TABLE, is_historical=True)
        
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

if __name__ == "__main__":
    main() 
    