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
        'Company': 'LinkIT'
    },
    'freelance.nl': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'See Vacancy',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Company': 'freelance.nl'
    },
    'Yacht': {
        'Title': 'Field1',
        'Location': 'Field2',
        'Summary': 'Text2',
        'URL': 'https://yachtfreelance.talent-pool.com/projects?openOnly=true&page=1',
        'start': 'Field3',
        'rate': 'Text',
        'Company': 'Yacht Freelance'
    },
    'Flextender': {
        'Title': 'Field2',
        'Location': 'Field1',
        'Summary': 'See Vacancy',
        'URL': 'URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Company': 'Flextender'
    },
    'KVK': {
        'Title': 'Title',
        'Location': 'Amsterdam',
        'Summary': 'See Vacancy',
        'URL': 'https://www.kvkhuurtin.nl/opdrachten',
        'start': 'Title1',
        'rate': 'Title3',
        'Company': 'KVK'
    },
    'Cirle8': {
        'Title': 'Title',
        'Location': 'cvacancygridcard_usp',
        'Summary': 'See Vacancy',
        'URL': 'Title_URL',
        'start': 'Date',
        'rate': 'Not mentioned',
        'Company': 'Circle8'
    },
    'LinkedIn': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'About_the_job',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Company': 'LinkedIn'
    },
    'politie': {
        'Title': 'Field1',
        'Location': 'Hilversum',
        'Summary': 'Text',
        'URL': 'URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Company': 'politie'
    },
    'gelderland': {
        'Title': 'Title',
        'Location': 'Gelderland',
        'Summary': 'See Vacancy',
        'URL': 'https://www.werkeningelderland.nl/inhuur/',
        'start': 'vacancy_details5',
        'rate': 'Not mentioned',
        'Company': 'gelderland'
    },
    'werk.nl': {
        'Title': 'Title',
        'Location': 'Description1',
        'Summary': 'See Vacancy',
        'URL': 'https://www.werk.nl/werkzoekenden/vacatures/',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Company': 'Description'
    },
    'indeed': {
        'Title': 'Title',
        'Location': 'css1restlb',
        'Summary': 'csso11dc0',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'css18z4q2i',
        'Company': 'css1h7lukg'
    },
    'Planet Interim': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'text',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Price',
        'Company': 'Planet Interim'
    },
    'NS': {
        'Title': 'Field1',
        'Location': 'Field2',
        'Summary': 'See Vacancy',
        'URL': 'https://www.werkenbijns.nl/vacatures?keywords=Inhuur',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Company': 'NS'
    },
    'hoofdkraan': {
        'Title': 'Title',
        'Location': 'colmd4',
        'Summary': 'Description',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'fontweightbold',
        'Company': 'hoofdkraan'
    },
    'Overheid': {
        'Title': 'Title',
        'Location': 'Content3',
        'Summary': 'See Vacancy',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Content',
        'Company': 'Description'
    },
    'rijkswaterstaat': {
        'Title': 'widgetheader',
        'Location': 'feature1',
        'Summary': 'See Vacancy',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'feature2',
        'Company': 'Rijkswaterstaat'
    },
    'zzp opdrachten': {
        'Title': 'Title',
        'Location': 'jobdetails6',
        'Summary': 'See Vacancy',
        'URL': 'https://www.zzp-opdrachten.nl/alle',
        'start': 'ASAP',
        'rate': 'jobdetails',
        'Company': 'Title2'
    },
    'Harvey Nash': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'Field4',
        'URL': 'Title_URL',
        'start': 'Field1',
        'rate': 'Salary',
        'Company': 'Field2'
    },
    'Behance': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'Text4',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Company': 'Company'
    },
    'Schiphol': {
        'Title': 'Field2',
        'Location': 'Hybrid',
        'Summary': 'Text5',
        'URL': 'Field1_links',
        'start': 'ASAP',
        'rate': 'Text2',
        'Company': 'Schiphol'
    },
    'Jooble': {
        'Title': '_8w9ce2',
        'Location': 'caption',
        'Summary': 'geyos4',
        'URL': '_8w9ce2_URL',
        'start': 'ASAP',
        'rate': 'not mentioned',
        'Company': 'z6wlhx'
    },
    'werkzoeken.nl': {
        'Title': 'Title',
        'Location': 'Company_name1',
        'Summary': 'See Vacancy',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'offer',
        'Company': 'Company_name'
    },
    'Centric': {
        'Title': 'Field1',
        'Location': 'Field2',
        'Summary': 'See Vacancy',
        'URL': 'URL',
        'start': 'ASAP',
        'rate': 'Not mentioned',
        'Company': 'Centric'
    },
    'freelancer.com': {
        'Title': 'Title',
        'Location': 'Remote',
        'Summary': 'Description',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Price',
        'Company': 'freelancer.com'
    },
    'freelancer.nl': {
        'Title': 'Title',
        'Location': 'Location',
        'Summary': 'cardtext',
        'URL': 'cardlink_URL',
        'start': 'ASAP',
        'rate': 'budget',
        'Company': 'freelancer.nl'
    },
    'Salta Group': {
        'Title': 'Keywords',
        'URL': 'Title_URL',
        'Location': 'Location',
        'Summary': 'feature_spancontainsclass_text',
        'Company': 'Company',
        'rate': 'Not mentioned',
        'start': 'ASAP'
    },
    'ProLinker.com': {
        'Title': 'Title',
        'Location': 'section3',
        'Summary': 'section',
        'URL': 'Title_URL',
        'start': 'ASAP',
        'rate': 'Like',
        'Company': 'ProLinker.com'
    }
}

def timestamp():
    """Get current timestamp in YYYY-MM-DD format."""
    return datetime.now().strftime("%Y-%m-%d")

def generate_unique_id(title, url, company):
    """Generate a unique ID based on the combination of title, URL, and company."""
    combined = f"{title}|{url}|{company}".encode('utf-8')
    return hashlib.md5(combined).hexdigest()

def validate_dataframe(df, required_columns):
    """Validate that DataFrame has required columns and data."""
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    if df.empty:
        raise ValueError("DataFrame is empty")
    
    return True

def validate_data_quality(df, required_columns):
    """
    Enhanced data validation with quality checks and detailed reporting.
    Returns a tuple (is_valid, issues_dict) where is_valid is a boolean and 
    issues_dict contains detailed information about any data quality issues.
    """
    issues = {
        'empty_values': {},
        'invalid_urls': [],
        'special_chars': {},
        'data_type_issues': {}
    }
    
    try:
        # Basic validation first
        validate_dataframe(df, required_columns)
        
        # Check for empty or whitespace-only values
        for col in required_columns:
            empty_count = df[col].astype(str).str.strip().eq('').sum()
            if empty_count > 0:
                issues['empty_values'][col] = empty_count
        
        # URL format validation
        if 'URL' in df.columns:
            invalid_urls = df[~df['URL'].str.match(r'^https?://', na=False)]
            if not invalid_urls.empty:
                issues['invalid_urls'] = invalid_urls.index.tolist()
        
        # Check for potentially problematic special characters
        special_chars_pattern = r'[<>{}\"]'
        for col in ['Title', 'Summary', 'Location']:
            if col in df.columns:
                special_chars = df[df[col].str.contains(special_chars_pattern, na=False, regex=True)]
                if not special_chars.empty:
                    issues['special_chars'][col] = special_chars.index.tolist()
        
        # Data type validation
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
                if not all(isinstance(val, expected_type) for val in df[col].dropna()):
                    issues['data_type_issues'][col] = expected_type.__name__
        
        # Calculate if there are any issues
        has_issues = any(
            issue_dict for issue_dict in issues.values()
            if issue_dict and (isinstance(issue_dict, dict) or isinstance(issue_dict, list))
        )
        
        # Log validation results
        # (Suppress detailed warnings/info about data quality issues)
        # if has_issues:
        #     logging.warning("Data quality issues found:")
        #     if issues['empty_values']:
        #         logging.warning(f"Empty values found: {issues['empty_values']}")
        #     if issues['invalid_urls']:
        #         logging.warning(f"Invalid URLs found in rows: {issues['invalid_urls']}")
        #     if issues['data_type_issues']:
        #         logging.warning(f"Data type issues found: {issues['data_type_issues']}")
        # else:
        #     logging.info("Data validation passed successfully")
        
        return not has_issues, issues
    
    except Exception as e:
        logging.error(f"Data validation error: {str(e)}")
        return False, {'error': str(e)}

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
        
        # Create a new DataFrame with standardized columns
        result = pd.DataFrame()
        
        # Map the columns according to the mapping
        for std_col, src_col_mapping_value in mapping.items():
            if src_col_mapping_value in files_read.columns:
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
                # Determine if src_col_mapping_value is an intentional literal that should be preserved
                is_intentional_literal = (
                    src_col_mapping_value in {'ASAP', 'Not mentioned', 'See Vacancy'} or \
                    (std_col == 'URL' and (src_col_mapping_value.startswith('http://') or src_col_mapping_value.startswith('https://'))) or \
                    std_col == 'Company' # Value for 'Company' mapping is always an intended literal.
                )
                
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
                elif not is_intentional_literal:
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
                result.loc[result[col_name] == '', col_name] = fallback_value
        
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
        
        # Perform enhanced data validation
        is_valid, issues = validate_data_quality(result, standard_columns)
        
        if not is_valid:
            # Fix URL issues if possible
            if issues['invalid_urls']:
                for idx in issues['invalid_urls']:
                    url = result.loc[idx, 'URL']
                    if not url.startswith(('http://', 'https://')):
                        result.loc[idx, 'URL'] = f'https://{url.lstrip("/")}'
            
            # Remove problematic special characters
            special_chars_cleaned = False
            if issues['special_chars']:
                import re
                for col, indices in issues['special_chars'].items():
                    for idx in indices:
                        old_val = result.loc[idx, col]
                        new_val = re.sub(r'[<>{}\"]', '', old_val)
                        result.loc[idx, col] = new_val
                        if old_val != new_val:
                            special_chars_cleaned = True
            
            # Handle empty values
            if issues['empty_values']:
                for col, count in issues['empty_values'].items():
                    if col in ['Title', 'URL', 'Company']:  # Required fields
                        result = result.dropna(subset=[col])
                    else:  # Optional fields
                        default_values = {
                            'Location': 'Not specified',
                            'Summary': 'No description available',
                            'start': 'ASAP',
                            'rate': 'Not mentioned'
                        }
                        if col in default_values:
                            result[col] = result[col].fillna(default_values[col])
            
            # Re-run validation after cleaning
            final_valid, final_issues = validate_data_quality(result, standard_columns)
            if not final_valid:
                if final_issues.get('special_chars'):
                    logging.warning(f"Special characters remain in {company_name} after cleaning: {final_issues['special_chars']}")
                # logging.warning(f"Some data quality issues remain for {company_name} after attempted fixes")
            else:
                logging.info(f"Successfully validated and cleaned data for {company_name}")
        else:
            logging.info(f"Successfully validated and cleaned data for {company_name}")
        
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
    Prepare DataFrame for upload by adding date and unique ID.
    If historical_data is provided, preserves dates for existing records.
    """
    # Add UNIQUE_ID and date columns
    df['UNIQUE_ID'] = df.apply(
        lambda row: generate_unique_id(row['Title'], row['URL'], row['Company']),
        axis=1
    )
    df['date'] = timestamp()
    
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