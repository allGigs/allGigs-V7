import os
import json
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

# Load configuration from config.json
    try:
    with open('config.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
    except FileNotFoundError:
    print("Warning: config.json not found, using default values")
    config = {}

# Supabase configuration
SUPABASE_URL = config.get('supabase_url', "https://lfwgzoltxrfutexrjahr.supabase.co")
SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
if not SUPABASE_SERVICE_ROLE_KEY:
    raise ValueError("SUPABASE_SERVICE_ROLE_KEY environment variable is not set")

# Create Supabase client with service role key to bypass RLS
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# Load configuration values
BATCH_SIZE = config.get('batch_size', 500)
TABLES = config.get('tables', {})
NEW_TABLE = TABLES.get('new_table', "Allgigs_All_vacancies_NEW")
HISTORICAL_TABLE = TABLES.get('historical_table', "Allgigs_All_vacancies")
FRENCH_JOBS_TABLE = TABLES.get('french_jobs_table', "french freelance jobs")

# French job sources that should go to French_Freelance_Jobs table
FRENCH_SOURCES = config.get('french_sources', [
    '404Works',
    'Codeur.com',
    'Welcome to the Jungle',
    'comet',
    'Freelance-Informatique'
])

# Skip companies configuration
SKIP_COMPANIES = set(config.get('skip_companies', []))

# Source mappings configuration
SOURCE_MAPPINGS = config.get('source_mappings', {})

# Directory structure
BASE_DIR = Path('/Users/jaapjanlammers/Desktop/Freelancedirectory')
FREELANCE_DIR = BASE_DIR / 'Freelance Directory'
IMPORTANT_DIR = BASE_DIR / 'Important_allGigs'

# Load company mappings from JSON file
def load_company_mappings():
    """Load company mappings from JSON file."""
    json_path = Path(__file__).parent / 'company_mappings.json'
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # Remove the _comment key if it exists
            return {k: v for k, v in data.items() if k != '_comment'}
    except FileNotFoundError:
        logging.error(f"Company mappings file not found at {json_path}")
        return {}
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing company mappings JSON: {e}")
        return {}

# Load company mappings at module level
COMPANY_MAPPINGS = load_company_mappings()

# Load French patterns from JSON file
def load_french_patterns():
    """Load French patterns from JSON file."""
    json_path = Path(__file__).parent / 'french_patterns.json'
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"French patterns file not found at {json_path}")
        return {}
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing French patterns JSON: {e}")
        return {}

# Load French patterns at module level
FRENCH_PATTERNS = load_french_patterns()

# Load geographical patterns from JSON file
def load_geographic_patterns():
    """Load geographical patterns from JSON file."""
    json_path = Path(__file__).parent / 'geographic_patterns.json'
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Geographic patterns file not found at {json_path}")
        return {}
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing geographic patterns JSON: {e}")
        return {}

# Load geographical patterns at module level
GEOGRAPHIC_PATTERNS = load_geographic_patterns()

# Load remote patterns from JSON file
def load_remote_patterns():
    """Load remote patterns from JSON file."""
    json_path = Path(__file__).parent / 'remote_patterns.json'
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Remote patterns file not found at {json_path}")
        return {}
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing remote patterns JSON: {e}")
        return {}

# Load remote patterns at module level
REMOTE_PATTERNS = load_remote_patterns()

# Load language patterns from JSON file
def load_language_patterns():
    """Load language patterns from JSON file."""
    json_path = Path(__file__).parent / 'language_patterns.json'
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Language patterns file not found at {json_path}")
        return {}
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing language patterns JSON: {e}")
        return {}

# Load language patterns at module level
LANGUAGE_PATTERNS = load_language_patterns()

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
        Dict[str, bool]: Dictionary with 'Dutch', 'French', 'EU', 'Rest_of_World' as keys
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
    remote_patterns = REMOTE_PATTERNS.get('remote_patterns', [])
    hybrid_patterns = REMOTE_PATTERNS.get('hybrid_patterns', [])

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
        elif 'remote (france)' in location_clean or 'france remote' in location_clean:
            remote_region = 'french'
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

        # Check company context for French companies - DISABLED per user request
        # French jobs should only be marked based on location, not company origin

        # Check source context (Dutch job boards suggest Dutch remote)
        if not remote_region and source:
            dutch_sources = ['freelance.nl', 'interimnetwerk']
            if any(dutch_source in str(source).lower() for dutch_source in dutch_sources):
                remote_region = 'dutch'

        # Check source context (French job boards suggest French remote)
        if not remote_region and source:
            # Use FRENCH_SOURCES from config (normalize to lowercase for comparison)
            french_sources_normalized = [s.lower().strip() for s in FRENCH_SOURCES]
            if any(french_source in str(source).lower() for french_source in french_sources_normalized):
                remote_region = 'french'

    # 3. APPLY REMOTE REGIONAL LOGIC
    if is_remote and remote_region:
        if remote_region == 'dutch':
            return {'Dutch': True, 'French': False, 'EU': False, 'Rest_of_World': False}
        elif remote_region == 'french':
            return {'Dutch': False, 'French': True, 'EU': False, 'Rest_of_World': False}
        elif remote_region.startswith('eu'):
            return {'Dutch': False, 'French': False, 'EU': True, 'Rest_of_World': False}


    # 4. REGULAR LOCATION ANALYSIS (for non-remote or hybrid office locations)
    
    # 4.1. Check for Dutch cities and regions FIRST
    dutch_cities_regions = GEOGRAPHIC_PATTERNS.get('dutch_cities_regions', [])
    
    # 4.2. Check for French cities and regions
    french_cities_regions = FRENCH_PATTERNS.get('french_cities_regions', [])
    
    if location_clean:
        for dutch_location in dutch_cities_regions:
            if dutch_location in location_clean:
                return {'Dutch': True, 'French': False, 'EU': False, 'Rest_of_World': False}
        
        for french_location in french_cities_regions:
            if french_location in location_clean:
                return {'Dutch': False, 'French': True, 'EU': False, 'Rest_of_World': False}
    
    # 4.3. Check for EU countries excluding Netherlands and France
    eu_countries_excluding_nl_fr = GEOGRAPHIC_PATTERNS.get('eu_countries_excluding_nl_fr', [])

    major_eu_cities = GEOGRAPHIC_PATTERNS.get('major_eu_cities', [])

    if location_clean:
        for country in eu_countries_excluding_nl_fr:
            if country in location_clean:
                return {'Dutch': False, 'French': False, 'EU': True, 'Rest_of_World': False}

        for city in major_eu_cities:
            if city in location_clean:
                return {'Dutch': False, 'French': False, 'EU': True, 'Rest_of_World': False}

    # 5. Check for "European Union" mentions
    if location_clean and 'european union' in location_clean:
        return {'Dutch': False, 'French': False, 'EU': True, 'Rest_of_World': False}

    # 6. Check for non-EU countries
    rest_of_world_countries = GEOGRAPHIC_PATTERNS.get('rest_of_world_countries', [])

    if location_clean:
        for country in rest_of_world_countries:
            if country in location_clean:
                return {'Dutch': False, 'French': False, 'EU': False, 'Rest_of_World': True}

    # 7. Check for USD currency
    if rate and not pd.isna(rate):
        rate_str = str(rate).lower().strip()
        usd_indicators = LANGUAGE_PATTERNS.get('currency_indicators', {}).get('usd_indicators', [])
        if any(indicator in rate_str for indicator in usd_indicators):
            return {'Dutch': False, 'French': False, 'EU': False, 'Rest_of_World': True}

    # 7.5. LANGUAGE DETECTION - Check for Dutch and French language indicators
    def detect_dutch_language(text_fields):
        """Detect Dutch language in text fields."""
        if not text_fields:
            return False
        
        # Dutch language indicators
        dutch_indicators = LANGUAGE_PATTERNS.get('dutch_indicators', [])
        
        # Combine all text fields
        combined_text = ' '.join([str(field).lower() for field in text_fields if field and not pd.isna(field)])
        
        # Count Dutch indicators
        dutch_count = sum(1 for indicator in dutch_indicators if indicator in combined_text)
        
        # If we find 3 or more Dutch indicators, consider it Dutch
        return dutch_count >= 3

    def detect_french_language(text_fields):
        """Detect French language in text fields."""
        if not text_fields:
            return False
        
        # French language indicators
        french_indicators = FRENCH_PATTERNS.get('french_language_indicators', [])
        
        # Combine all text fields
        combined_text = ' '.join([str(field).lower() for field in text_fields if field and not pd.isna(field)])
        
        # Count French indicators
        french_count = sum(1 for indicator in french_indicators if indicator in combined_text)
        
        # If we find 3 or more French indicators, consider it French
        return french_count >= 3
    
    # Check for language in available text fields
    text_fields = [location, title, summary, company]
    
    # Check French language first (more specific)
    if detect_french_language(text_fields):
        return {'Dutch': False, 'French': True, 'EU': False, 'Rest_of_World': False}
    
    # Check Dutch language
    if detect_dutch_language(text_fields):
        return {'Dutch': True, 'French': False, 'EU': False, 'Rest_of_World': False}

    # 8. Default to Dutch
    return {'Dutch': True, 'French': False, 'EU': False, 'Rest_of_World': False}

def classify_job_industry(title, summary=''):
    """Classify job into industry category"""
    if pd.isna(title):
        return 'Other/General'
    
    text = str(title).lower()
    if summary and not pd.isna(summary):
        text += ' ' + str(summary).lower()
    
    # Preprocessing: Remove seniority prefixes to focus on core role
    import re
    text = re.sub(r'\b(senior|junior|medior|lead)\s+', '', text)
    
    # Handle work arrangement tags - Enhanced filtering
    skip_patterns = LANGUAGE_PATTERNS.get('work_arrangement_patterns', {}).get('skip_patterns', [])
    
    # Enhanced work arrangement tag detection
    words = text.split()
    if len(words) <= 8:  # Increased from 3 to catch more complex work arrangement combinations
        if any(pattern in text for pattern in skip_patterns):
            # Additional check for hour specifications
            hour_patterns = LANGUAGE_PATTERNS.get('work_arrangement_patterns', {}).get('hour_patterns', [])
            if any(hour_pattern in text for hour_pattern in hour_patterns):
                return 'No title information'  # Mark as insufficient data for industry classification
            # Check for basic work arrangement patterns
            if any(basic_pattern in text for basic_pattern in skip_patterns):
                return 'No title information'  # Mark as insufficient data for industry classification
    
    # Special title-based rules
    title_lower = str(title).lower()
    if 'front-end' in title_lower or 'frontend' in title_lower:
        return 'IT & Software Development'
    if 'developer' in title_lower:
        return 'IT & Software Development'
    
    # Check for "IT" with strict word boundaries in title
    import re
    if re.search(r'\bit\b', title_lower):
        return 'IT & Software Development'
    
    # Check for "security" anywhere in title
    if 'security' in title_lower:
        return 'Security & Safety'
    
    # Load industry keywords from JSON file
    try:
        with open('industry_keywords.json', 'r', encoding='utf-8') as f:
            industry_keywords_data = json.load(f)
    except FileNotFoundError:
        # Fallback to built-in keywords if JSON file not found
        industry_keywords_data = {
            'IT & Software Development': ['developer', 'programmer', 'software'],
            'Other/General': ['general']
        }

    # Map JSON categories to our standard categories where possible
    keywords = {}
    for json_category, json_keywords in industry_keywords_data.items():
        # Map JSON categories to our standard categories
        if json_category == 'IT: Software Development':
            category = 'IT & Software Development'
        elif json_category == 'IT: Data & Analytics':
            category = 'Data & Analytics'
        elif json_category == 'Finance':
            category = 'Finance & Accounting'
        elif json_category == 'Project Management':
            category = 'Project Management'
        elif json_category == 'Arts and Design':
            category = 'Creative & Media'
        elif json_category == 'Customer Service':
            category = 'Customer Service & Support'
        elif json_category == 'Human Resources':
            category = 'Human Resources'
        elif json_category == 'Healthcare':
            category = 'Healthcare & Medical'
        elif json_category == 'Marketing':
            category = 'Marketing & Communications'
        elif json_category == 'Sales':
            category = 'Sales & Business Development'
        elif json_category == 'Law Enforcement & Security':
            category = 'Security & Safety'
        elif json_category == 'Hospitality':
            category = 'Hospitality & Tourism'
        elif json_category == 'Logistics':
            category = 'Supply Chain & Logistics'
        elif json_category == 'Education':
            category = 'Education & Training'
        elif json_category == 'Engineering':
            category = 'Engineering'
        elif json_category == 'Construction':
            category = 'Engineering'  # Map construction to engineering
        elif json_category == 'Food & Agriculture':
            category = 'Food & Agriculture'
        elif json_category == 'Government & Public Sector':
            category = 'Government & Public Sector'
        elif json_category == 'Consulting':
            category = 'Consulting'
        elif json_category == 'Legal':
            category = 'Legal'
        elif json_category == 'Retail':
            category = 'Retail'
        else:
            # Keep original category name for unmapped ones
            category = json_category

        # Add all keywords from JSON to the category
        if category not in keywords:
            keywords[category] = []
        keywords[category].extend(json_keywords)

    # Ensure we have all the standard categories, even if not in JSON
    standard_categories = [
        'IT & Software Development', 'Data & Analytics', 'Finance & Accounting',
        'Project Management', 'Creative & Media', 'Engineering', 'Food & Agriculture',
        'Education & Training', 'Customer Service & Support', 'Human Resources',
        'Healthcare & Medical', 'Marketing & Communications', 'Consulting',
        'Government & Public Sector', 'Sales & Business Development',
        'Security & Safety', 'Legal', 'Hospitality & Tourism',
        'Supply Chain & Logistics', 'Operations & Management',
        'No clear title', 'Other/General'
    ]

    for category in standard_categories:
        if category not in keywords:
            keywords[category] = []
    
    import re
    
    for industry, words in keywords.items():
        for word in words:
            # Create word boundary pattern
            pattern = r'\b' + re.escape(word) + r'\b'
            if re.search(pattern, text, re.IGNORECASE):
                return industry
    
    return 'Other/General'

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
    remote_patterns = REMOTE_PATTERNS.get('remote_patterns', [])
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
            try:
                region_classification = categorize_location(location, None, company, source, title, summary)
            except Exception:
                region_classification = None

            if isinstance(region_classification, dict):
                if region_classification.get('Dutch'):
                    return 'Remote (Netherlands)'
                if region_classification.get('EU'):
                    return 'Remote (EU)'
                if region_classification.get('Rest_of_World'):
                    return 'Remote (Rest of World)'
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

    # Extract regional boolean values with safety checks
    df_copy['Dutch'] = categorizations.apply(lambda x: x.get('Dutch', True) if x is not None else True)
    df_copy['EU'] = categorizations.apply(lambda x: x.get('EU', False) if x is not None else False)
    df_copy['Rest_of_World'] = categorizations.apply(lambda x: x.get('Rest_of_World', False) if x is not None else False)

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
        
        # Handle special cases for known sources using config file mappings
        # Use the global SOURCE_MAPPINGS from config, but ensure it's a dictionary
        source_mappings = SOURCE_MAPPINGS if isinstance(SOURCE_MAPPINGS, dict) else {}
        
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

def special_freelance_processing(df, company_name):
    """Special processing for freelance.nl - extract hours from Field2"""
    if 'Hours' in df.columns and 'Field2' in df.columns:
        def extract_hours_freelance(row):
            field2_val = row.get('Field2', '')
            if pd.isna(field2_val) or field2_val == '':
                return 'Not mentioned'
            field2_str = str(field2_val).lower()
            import re
            patterns = [
                r'aantal uur per week[:s]*(\d+)', 
                r'(\d+)\s*uurs*\s*per\s*week', 
                r'hours[:s]*.*?(\d+)\s*hours?\s*per\s*week', 
                r'(\d+)\s*hours?\s*per\s*week', 
                r'full-time\s*((\d+)\s*hours?\s*per\s*week)'
            ]
            for pattern in patterns:
                match = re.search(pattern, field2_str)
                if match:
                    return match.group(1) if match.group(1) else match.group(2)
            return 'Not mentioned'
        
        df['Hours'] = df.apply(extract_hours_freelance, axis=1)
        logging.info(f"🔧 {company_name}: Extracted hours information from Field2")
        return df

def special_hoofdkraan_processing(df, company_name):
    """Special processing for Hoofdkraan - remove 'Locatie:' prefix from Location"""
    if 'Location' in df.columns:
        def process_location_hoofdkraan(location_str):
            if pd.isna(location_str) or location_str == '':
                return 'Not mentioned'
            location_clean = str(location_str).replace('Locatie:', '').replace('locatie:', '').strip()
            location_clean = ' '.join(location_clean.split())
            return location_clean if location_clean else 'Not mentioned'
        
        df['Location'] = df['Location'].apply(process_location_hoofdkraan)
        logging.info(f"🔧 {company_name}: Processed Location to remove 'Locatie:' prefix")
    return df

def special_harvey_nash_processing(df, company_name):
    """Special processing for Harvey Nash - remove words in parentheses and words starting with 'JP'"""
    if 'Title' in df.columns:
        def process_title_harvey_nash(title_str):
            if pd.isna(title_str) or title_str == '':
                return 'Not mentioned'
            import re
            title_clean = str(title_str)
            title_clean = re.sub(r'\([^)]*\)', '', title_clean)
            words = title_clean.split()
            filtered_words = [word for word in words if not word.upper().startswith('JP')]
            title_clean = ' '.join(filtered_words).strip()
            title_clean = ' '.join(title_clean.split())
            return title_clean if title_clean else 'Not mentioned'
        
        df['Title'] = df['Title'].apply(process_title_harvey_nash)
        logging.info(f"🔧 {company_name}: Processed Title to remove parentheses and JP words")
    return df

def special_linkedin_processing(df, company_name):
    """Special processing for LinkedIn - remove line breaks from Summary"""
    if 'Summary' in df.columns:
        def process_summary_linkedin(summary_str):
            if pd.isna(summary_str) or summary_str == '':
                return 'See Vacancy'
            summary_clean = str(summary_str)
            summary_clean = summary_clean.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
            summary_clean = ' '.join(summary_clean.split())
            return summary_clean if summary_clean else 'See Vacancy'
        
        df['Summary'] = df['Summary'].apply(process_summary_linkedin)
        logging.info(f"🔧 {company_name}: Processed Summary to remove line breaks")
    return df

def special_umc_processing(df, company_name):
    """Special processing for UMC - remove 'p.w.' from Hours"""
    if 'Hours' in df.columns:
        def process_hours_umc(hours_str):
            if pd.isna(hours_str) or hours_str == '':
                return 'Not mentioned'
            hours_clean = str(hours_str)
            hours_clean = hours_clean.replace('p.w.', '').replace('P.W.', '').replace('P.w.', '').replace('p.W.', '')
            hours_clean = hours_clean.strip()
            return hours_clean if hours_clean else 'Not mentioned'
        
        df['Hours'] = df['Hours'].apply(process_hours_umc)
        logging.info(f"🔧 {company_name}: Processed Hours to remove 'p.w.'")
    return df

def special_interimnetwerk_processing(df, company_name):
    """Special processing for InterimNetwerk - extract data from Text column"""
    if 'Text' in df.columns and 'Field1_links' in df.columns:
                # Take only the first row of Text column but keep all Field1_links
        if len(df) > 0:
            text_content = df['Text'].iloc[0]
                    # Combine all Field1_links from all rows to get complete URL list
                    all_field1_links = []
            for idx, row in df.iterrows():
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
                                                    'Company': 'InterimNetwerk',
                        'Source': 'InterimNetwerk',
        'Type source': 'Job board',
                    'date': timestamp(),
                    'UNIQUE_ID': generate_unique_id(title, url, 'InterimNetwerk')
                        })
                    
                    # Convert to DataFrame
            df = pd.DataFrame(processed_data)
            logging.info(f"InterimNetwerk special processing: Created {len(df)} rows from {len(jobs)} job blocks")
                else:
            logging.warning(f"🔧 {company_name}: No data found in CSV file")
            df = pd.DataFrame()
            else:
        logging.warning(f"🔧 {company_name}: Required columns 'Text' and 'Field1_links' not found")
        df = pd.DataFrame()
    return df

def process_location_tennet(location_str):
    """Process location for tennet - remove everything after '/'"""
    if pd.isna(location_str) or location_str == '':
        return 'Not mentioned'
    
    location_clean = str(location_str).strip()
    
    # Remove everything after "/" (including the "/" itself)
    if '/' in location_clean:
        location_clean = location_clean.split('/')[0].strip()
    
    return location_clean if location_clean else 'Not mentioned'

def combine_summary_fields_tennet(index, files_read):
    """Combine summary fields for tennet - combine Field5 through Field14"""
    # Get the original row from the input DataFrame
    if index >= len(files_read):
                        return 'Not mentioned'
                    
    row = files_read.iloc[index]
    
    # List of field names to combine
    field_names = ['Field5', 'Field6', 'Field7', 'Field8', 'Field9', 'Field10', 'Field11', 'Field12', 'Field13', 'Field14']
    
    # Collect non-empty values
    values = []
    for field_name in field_names:
        if field_name in files_read.columns:
            value = row[field_name]
            if not pd.isna(value) and str(value).strip():
                values.append(str(value).strip())
    
    # Combine with spaces, or return default if no content
    return ' '.join(values) if values else 'See Vacancy'

def process_summary_circle8(summary_str):
    """Process summary for Circle8 - provide fallback text if no information is provided"""
    if pd.isna(summary_str) or summary_str == '' or str(summary_str).strip() == '':
        return 'We were not able to find description'
    
    summary_clean = str(summary_str).strip()
    return summary_clean if summary_clean else 'We were not able to find description'

def process_summary_flexvalue(summary_str):
    """Process summary for FlexValue_B.V. - remove everything before 'opdrachtbeschrijving'"""
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

def process_rate_flexvalue(rate_str):
    """Process rate for FlexValue_B.V. - extract text between 'Tarief' and 'all-in'"""
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

def process_hours_flexvalue(hours_str):
    """Process hours for FlexValue_B.V. - extract number after 'Uren per week'"""
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
                    
def process_title_amstelveenhuurtin(title_str):
    """Process title for Amstelveenhuurtin - remove words in brackets and words starting with 'SO' followed by a number"""
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

def process_location_amstelveenhuurtin(location_str):
    """Process location for Amstelveenhuurtin - extract text between 'standplaats:' and '|'"""
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

def process_hours_amstelveenhuurtin(hours_str):
    """Process hours for Amstelveenhuurtin - extract text between 'Uren:' and '|'"""
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

def process_start_amstelveenhuurtin(start_str):
    """Process start for Amstelveenhuurtin - extract everything before 't/m'"""
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

def process_hours_hinttech(hours_str):
    """Process hours for HintTech - remove 'per week' and keep only numbers"""
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

def process_duration_hinttech(duration_str):
    """Process duration for HintTech - calculate difference between start and end dates"""
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
                
def process_location_indeed(index, files_read):
    """Process location for indeed - extract location from Field2 column"""
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
                
def process_rate_indeed(index, files_read):
    """Process rate for indeed - extract rate from Field2 column"""
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

def process_summary_indeed(index, files_read, result):
    """Process summary for indeed - use Field2 but remove the first line"""
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

def extract_strict_rate(text_str):
    """Extract strict rate for werk.nl - extract only relevant numbers (rates/salaries) from Text column"""
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

def process_title_twine(title_str):
    """Process title for twine - remove 'Easy Apply ' text"""
                    if pd.isna(title_str) or title_str == '':
                        return 'Not mentioned'
                    
    title_clean = str(title_str).strip()
                    
    # Remove "Easy Apply " (case insensitive)
    title_clean = title_clean.replace('Easy Apply ', '').replace('easy apply ', '')
                    
                    # Clean up extra spaces
                    title_clean = ' '.join(title_clean.split()).strip()
                    
                    return title_clean if title_clean else 'Not mentioned'
                
def process_rate_haarlemmermeer(rate_str):
    """Process rate for haarlemmermeerhuurtin - remove 'per uur' and keep only the amount"""
                    if pd.isna(rate_str) or rate_str == '':
                        return 'Not mentioned'
    # Remove "per uur" (case insensitive) and clean up
    rate_clean = str(rate_str).lower().replace('per uur', '').replace('peruur', '').strip()
    # Remove extra spaces and return
    rate_clean = ' '.join(rate_clean.split())
    return rate_clean if rate_clean else 'Not mentioned'

def process_duration_haarlemmermeer(duration_str):
    """Process duration for haarlemmermeerhuurtin - calculate difference between start and end dates"""
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

def process_rate_werk(rate_str):
    """Process rate for werk.nl - extract rate information"""
                    if pd.isna(rate_str) or rate_str == '':
                        return 'Not mentioned'
                    
                        rate_clean = str(rate_str).strip()
    # Extract rate information - this is a placeholder function
    # You may need to implement specific logic based on werk.nl's rate format
    return rate_clean if rate_clean else 'Not mentioned'

def process_location_tennet(location_str):
    """Process location for tennet - clean location information"""
    if pd.isna(location_str) or location_str == '':
                        return 'Not mentioned'
                        
    location_clean = str(location_str).strip()
    # Clean up location information - this is a placeholder function
    # You may need to implement specific logic based on tennet's location format
    return location_clean if location_clean else 'Not mentioned'

def process_hours_freelance_nl(hours_str):
    """Process hours for freelance.nl - extract hours information"""
                    if pd.isna(hours_str) or hours_str == '':
                        return 'Not mentioned'
                    
                    hours_clean = str(hours_str).strip()
    # Extract hours information - this is a placeholder function
    # You may need to implement specific logic based on freelance.nl's hours format
                    return hours_clean if hours_clean else 'Not mentioned'
                
def apply_special_processing(df, company_name):
    """Apply special processing based on company name"""
    try:
        # Load special processing configuration
        with open('special_processing.json', 'r') as f:
            special_processing = json.load(f)
        
        if company_name in special_processing:
            processing_type = special_processing[company_name].get('type')
            
            # Map processing types to functions
            processing_functions = {
                'special_freelance_processing': special_freelance_processing,
                'special_hoofdkraan_processing': special_hoofdkraan_processing,
                'special_harvey_nash_processing': special_harvey_nash_processing,
                'special_linkedin_processing': special_linkedin_processing,
                'special_umc_processing': special_umc_processing,
                'special_interimnetwerk_processing': special_interimnetwerk_processing,
            }
            
            if processing_type in processing_functions:
                df = processing_functions[processing_type](df, company_name)
                logging.info(f"🔧 {company_name}: Applied {processing_type}")
                            else:
                logging.warning(f"🔧 {company_name}: Processing type {processing_type} not implemented")
        
        return df
        
    except FileNotFoundError:
        logging.warning(f"🔧 {company_name}: special_processing.json not found, skipping special processing")
        return df
                    except Exception as e:
        logging.error(f"🔧 {company_name}: Error in special processing: {e}")
        return df

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

        # PLACEHOLDER DETECTION DISABLED per user request
        # The aggressive placeholder blanking logic has been disabled to preserve all data
        # that matches mapping values, as these may be legitimate data rather than placeholders
        pass
        
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

        
        
        
        
        # FIELD MERGING POST-MAPPING PROCESSING
        # Handle sources that use Field5|Field6|Field7|Field8|Field9|Field10|Field11|Field12|Field13|Field14 mapping
        field_merging_sources = ['flexSpot.io', 'freep', 'Friesland', 'freelancer.com', 'ProLinker.com', 'overheidzzp', 'Zuid-Holland']
        if company_name in field_merging_sources:
            # Process Summary field - combine Field5-Field14
            if 'Summary' in result.columns:
                def combine_summary_fields(index):
                    if index >= len(files_read):
                        return 'Not mentioned'
                    
                    row = files_read.iloc[index]
                    
                    # List of field names to combine
                    field_names = ['Field5', 'Field6', 'Field7', 'Field8', 'Field9', 'Field10', 'Field11', 'Field12', 'Field13', 'Field14']
                    
                    # Collect non-empty values
                    values = []
                    for field_name in field_names:
                        if field_name in files_read.columns:
                            value = row[field_name]
                            if not pd.isna(value) and str(value).strip():
                                values.append(str(value).strip())
                    
                    # Combine with spaces, or return default if no content
                    return ' '.join(values) if values else 'See Vacancy'
                
                # Apply the processing to each row by index
                result['Summary'] = [combine_summary_fields(i) for i in range(len(result))]
                logging.info(f"{company_name} post-mapping: Combined Field5-Field14 into Summary field")
        

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
        
        
        # COMPANY-SPECIFIC POST-MAPPING PROCESSING
        
        
        
        
        
        
        

        # OVERHEIDZZP POST-MAPPING PROCESSING
        
        
        
        
        # GGDZWHUURTIN.NL POST-MAPPING PROCESSING
        
        # 4 FREELANCERS POST-MAPPING PROCESSING
        
        
        
        
        
        
        
        # FLEVOLAND POST-MAPPING PROCESSING
        
        
        

        
        

        
        
        
        
        # INDUSTRY CLASSIFICATION - Apply to all jobs
        # Create Industry column for all jobs
        result['Industry'] = result.apply(lambda row: classify_job_industry(row['Title'], row.get('Summary', '')), axis=1)
        logging.info(f"Applied industry classification to {len(result)} jobs")
        
        # REGIONAL CATEGORIZATION - Apply to all jobs
        # Create Dutch, French, EU, Rest_of_World boolean columns
        logging.info("Applying regional categorization...")
        regional_categories = result.apply(
            lambda row: categorize_location(
                location=row.get('Location', ''),
                rate=row.get('rate', ''),
                company=row.get('Company', ''),
                source=row.get('Source', ''),
                title=row.get('Title', ''),
                summary=row.get('Summary', '')
            ), 
            axis=1
        )
        
        # Extract the boolean values into separate columns
        result['Dutch'] = regional_categories.apply(lambda x: x.get('Dutch', False))
        result['French'] = regional_categories.apply(lambda x: x.get('French', False))
        result['EU'] = regional_categories.apply(lambda x: x.get('EU', False))
        result['Rest_of_World'] = regional_categories.apply(lambda x: x.get('Rest_of_World', False))
        
        # Log regional distribution
        dutch_count = result['Dutch'].sum()
        french_count = result['French'].sum()
        eu_count = result['EU'].sum()
        rest_count = result['Rest_of_World'].sum()
        logging.info(f"Regional categorization complete: Dutch={dutch_count}, French={french_count}, EU={eu_count}, Rest_of_World={rest_count}")
        
        # Apply special processing if configured
        result = apply_special_processing(result, company_name)
        
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
        
        # SIMPLE FRENCH JOB SPLIT
        logging.info("Splitting data between French and non-French sources...")
        
        # French sources list from config
        french_sources = FRENCH_SOURCES
        
        # Split the data - handle case sensitivity robustly
        # Normalize both the data and the source list for comparison
        result_sources_normalized = result['Source'].astype(str).str.lower().str.strip()
        french_sources_normalized = [source.lower().strip() for source in french_sources]
        
        # Create mask for French sources
        french_mask = result_sources_normalized.isin(french_sources_normalized)
        french_jobs = result[french_mask].copy()
        non_french_jobs = result[~french_mask].copy()
        
        french_count = len(french_jobs)
        non_french_count = len(non_french_jobs)
        
        # Clear notification about the split
        logging.info("="*80)
        logging.info("🇫🇷 FRENCH JOB SPLIT NOTIFICATION 🇫🇷")
        logging.info("="*80)
        logging.info(f"📊 TOTAL JOBS PROCESSED: {len(result)}")
        logging.info(f"🇫🇷 FRENCH JOBS: {french_count} → 'french freelance jobs' table")
        logging.info(f"🌍 NON-FRENCH JOBS: {non_french_count} → 'Allgigs_All_vacancies_NEW' table")
        
        if french_count > 0:
            french_sources_found = french_jobs['Source'].value_counts().to_dict()
            logging.info(f"🇫🇷 French sources found: {french_sources_found}")
            
            # Debug: Show all unique sources in the data for comparison
            all_sources_in_data = result['Source'].value_counts().to_dict()
            logging.info(f"🔍 All sources in data: {list(all_sources_in_data.keys())}")
            logging.info(f"🔍 Expected French sources: {french_sources}")
        else:
            logging.info("🇫🇷 No French jobs found in current data")
            
            # Debug: Show all unique sources in the data for comparison
            all_sources_in_data = result['Source'].value_counts().to_dict()
            logging.info(f"🔍 All sources in data: {list(all_sources_in_data.keys())}")
            logging.info(f"🔍 Expected French sources: {french_sources}")
            logging.info(f"🔍 Normalized French sources: {french_sources_normalized}")
        
        logging.info("="*80)
        
        # Only upload to Supabase if there are no errors, source failures, or broken URLs
        if error_messages or source_failures or broken_urls:
            if broken_urls:
                logging.error("Upload to Supabase skipped due to broken URLs. See error summary above.")
            else:
                logging.error("Upload to Supabase skipped due to errors. See error summary above.")
        else:
            # Use the already split data from above
            logging.info(f"Using split data: {french_count} French jobs, {non_french_count} non-French jobs")
            
            # Upload to Supabase tables with transaction-like behavior
            upload_results = []
            upload_successful = True
            
            # Upload French jobs to dedicated table
            if french_count > 0:
                try:
                    logging.info(f"Uploading {french_count} French jobs to {FRENCH_JOBS_TABLE}...")
                    upload_result_french = supabase_upload(french_jobs, FRENCH_JOBS_TABLE, is_historical=False)
                    upload_results.append(upload_result_french)
                    if upload_result_french.get('status') == 'Failed':
                        upload_successful = False
                        logging.error(f"French jobs table upload failed: {upload_result_french.get('error', 'Unknown error')}")
                except Exception as e:
                    upload_successful = False
                    upload_results.append({
                        'table_name': FRENCH_JOBS_TABLE,
                        'status': 'Failed',
                        'error': str(e)
                    })
                    logging.error(f"Exception during French jobs table upload: {str(e)}")
            else:
                logging.info("No French jobs found - skipping French jobs table upload")
                upload_results.append({
                    'table_name': FRENCH_JOBS_TABLE,
                    'status': 'Skipped',
                    'error': 'No French jobs found'
                })
            
            # Upload non-French jobs to NEW table (only if French upload succeeded or was skipped)
            if upload_successful and non_french_count > 0:
                try:
                    logging.info(f"Uploading {non_french_count} non-French jobs to {NEW_TABLE}...")
                    upload_result_new = supabase_upload(non_french_jobs, NEW_TABLE, is_historical=False)
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
            elif non_french_count == 0:
                logging.info("No non-French jobs found - skipping NEW table upload")
                upload_results.append({
                    'table_name': NEW_TABLE,
                    'status': 'Skipped',
                    'error': 'No non-French jobs found'
                })
            else:
                # French upload failed, so skip NEW table
                upload_results.append({
                    'table_name': NEW_TABLE,
                    'status': 'Skipped',
                    'error': 'Skipped because French jobs upload failed'
                })
                logging.warning(f"Skipping {NEW_TABLE} upload because French jobs upload failed")
            
            # Only proceed to historical table if both previous uploads succeeded
            if upload_successful:
                try:
                    logging.info("Main uploads successful. Attempting to upload to historical table...")
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
                # Main uploads failed, so skip historical table
                upload_results.append({
                    'table_name': HISTORICAL_TABLE,
                    'status': 'Skipped',
                    'error': 'Skipped because main uploads failed'
                })
                logging.warning(f"Skipping {HISTORICAL_TABLE} upload because main uploads failed")
            
            # Print Supabase upload results table
            print_supabase_upsert_table(upload_results)

            # Upload processing results to Supabase
            upload_processing_results_to_supabase()

            # Log the transaction result
            if upload_successful:
                logging.info("✅ All tables uploaded successfully - transaction completed")
            else:
                logging.error("❌ Upload transaction failed - some tables may not have been updated")
        
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
    