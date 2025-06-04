import pandas
import requests
import json
import time
import os # Added from helper file
from datetime import datetime # Added import
import logging # Added for logging
from supabase import create_client, Client # Added for Supabase
from dotenv import load_dotenv # Added for Supabase

# Load environment variables for Supabase
load_dotenv()

# Supabase configuration
SUPABASE_URL = "https://lfwgzoltxrfutexrjahr.supabase.co" # Same as in allgigs V7.py
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
if not SUPABASE_KEY:
    # If running without a .env file (e.g. in some CI/CD or if logger is not yet set up)
    # print("CRITICAL: SUPABASE_KEY environment variable is not set. Exiting.")
    # exit(1) # Or raise an error, depending on desired behavior before logging is set up.
    # For now, we'll let it proceed and the logger will catch it if supabase client fails.
    pass

supabase_client: Client = None
try:
    if SUPABASE_KEY:
        supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
    else:
        # This case will be handled more gracefully once logger is initialized
        pass
except Exception as e:
    # print(f"CRITICAL: Failed to initialize Supabase client: {e}. Exiting.")
    # exit(1)
    pass

# Global variables for credentials and tokens
# Task_ID_data = pandas.read_csv('/Users/jaapjanlammers/Desktop/Freelancedirectory/Important_allGigs/automation_details.csv', sep= ';') # Commented out
base_api_url = 'https://openapi.octoparse.com/'
login_username = 'jj@nineways.nl'
login_password = 'rLyQiH2Th&8Ct4IX'

# These will be populated by log_in and refresh_token_function
access_token = None
refresh_token = None

def log_in(base_url_param, username_param, password_param, logger):
    global access_token, refresh_token
    logger.info('Get token:')
    token_url = base_url_param + 'token'
    headers = {'Content-Type': 'application/json'}
    payload = json.dumps({
        'username': username_param,
        'password': password_param,
        'grant_type': 'password'
    })

    try:
        response = requests.post(token_url, headers=headers, data=payload)
        response.raise_for_status()
        token_entity = response.json()
        logger.info("JSON Response:")
        logger.info(token_entity)
        if 'data' in token_entity and 'access_token' in token_entity['data'] and 'refresh_token' in token_entity['data']:
            access_token = token_entity['data']['access_token']
            refresh_token = token_entity['data']['refresh_token']
            logger.info(f"Successfully retrieved access token: {access_token[:20]}...")
            logger.info(f"Successfully retrieved refresh token: {refresh_token[:20]}...")
            return True
        else:
            logger.error("Access and/or refresh token not found in the 'data' section of the response.")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"An error occurred during the token request: {e}")
        if 'response' in locals() and response is not None:
            logger.error(f"HTTP Status Code: {response.status_code}")
            logger.error("Raw Response Content:")
            logger.error(response.text)
        else:
            logger.error("No response object available.")
        return False
    except json.JSONDecodeError as e:
        logger.error(f"JSONDecodeError: {e}")
        if 'response' in locals() and response is not None:
            logger.error("Raw Response Content:")
            logger.error(response.text)
        else:
            logger.error("No response object available for JSON decoding error.")
        return False
        
def refresh_token_function(base_url_param, current_refresh_token_param, logger):
    global access_token, refresh_token # Ensure we're updating the correct global variables
    try:
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        refresh_url = f"{base_url_param}token"
        logger.info(f"Refreshing token:")

        data = {
            'refresh_token': current_refresh_token_param,
            'grant_type': 'refresh_token'
        }

        response = requests.post(refresh_url, headers=headers, data=data)
        logger.info(f"HTTP Status Code: {response.status_code}")
        logger.info(f"Raw Response Content:\n{response.text}")

        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get('access_token') # Update global access_token
            new_refresh_token_val = token_data.get('refresh_token')
            if new_refresh_token_val: # Octoparse might not always return a new refresh token
                refresh_token = new_refresh_token_val # Update global refresh_token
            
            logger.info(f"Token refreshed successfully. New access token: {access_token[:30] if access_token else 'None'}...")
            if new_refresh_token_val:
                logger.info(f"New refresh token: {refresh_token[:30] if refresh_token else 'None'}...")
            return True
        else:
            logger.error(f"Token refresh failed with status code: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"An error occurred during the token refresh request: {str(e)}")
        return False

# --- Functions from Octoparse data V2 Helper file.py ---
def clear_task_data(base_url, access_token_param, task_id, task_name, logger):
    """Clear a specific task using the API."""
    try:
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token_param}'
        }
        api_url = f"{base_url}data/remove"
        logger.info(f"Attempting to clear task with ID: {task_id}")
        logger.info(f"Using URL: {api_url}")
        payload = {"taskId": task_id}
        response = requests.post(api_url, headers=headers, json=payload)
        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Response content: {response.text}")
        if response.status_code == 200:
            logger.info(f"Successfully cleared task: {task_name}")
            return True
        else:
            logger.warning(f"Failed to clear task '{task_name}'. Status code: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Error in clear_task_data: {str(e)}")
        return False

def start_task(base_url, access_token_param, task_id, task_name, logger):
    """Start a specific task using the Octoparse API."""
    try:
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token_param}'
        }
        api_url = f"{base_url}cloudextraction/start"
        logger.info(f"Attempting to start task with ID: {task_id}")
        logger.info(f"Using URL: {api_url}")
        payload = {"taskId": task_id}
        response = requests.post(api_url, headers=headers, json=payload)
        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Response content: {response.text}")
        if response.status_code == 200:
            logger.info(f"Successfully started task: {task_name}")
            return True
        else:
            logger.warning(f"Failed to start task '{task_name}'. Status code: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Error in start_task: {str(e)}")
        return False

def check_task_status(base_url, access_token_param, task_id, task_name, logger):
    """Check if a task has completed running.
    Returns:
        True: if task is confirmed finished.
        'STATUS_CHECK_PERMISSION_DENIED': if API returns 403.
        False: for other errors or if task is not finished.
    """
    try:
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token_param}'
        }
        api_url = f"{base_url}cloudextraction/statuses/v2"
        payload = {"taskIds": [task_id]}
        logger.info(f"\nChecking status for task: {task_name}")
        logger.info(f"Using URL: {api_url}")
        logger.info(f"Payload: {payload}")
        response = requests.post(api_url, headers=headers, json=payload)
        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Response content: {response.text}")

        if response.status_code == 200:
            status_data = response.json()
            logger.info(f"Full status data: {status_data}")
            if 'data' in status_data and len(status_data['data']) > 0:
                task_status = status_data['data'][0].get('status', '').lower()
                logger.info(f"Task status: {task_status}")
                is_complete = task_status == 'finished'
                if is_complete:
                    logger.info(f"Task '{task_name}' has completed")
                else:
                    logger.info(f"Task '{task_name}' is still running with status: {task_status}")
                return is_complete
            else:
                logger.warning(f"No status data found for task '{task_name}'")
                return False # Or handle as an error if 200 but no data is unexpected
        elif response.status_code == 403:
            logger.warning(f"Permission denied (403) while checking status for task '{task_name}'.")
            return 'STATUS_CHECK_PERMISSION_DENIED'
        else:
            logger.warning(f"Failed to check status for task '{task_name}'. Status code: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Error checking task status: {str(e)}")
        return False

def get_automation_details_from_supabase(client: Client, logger_param) -> pandas.DataFrame:
    """Fetches automation details from the 'automation_details' table in Supabase."""
    if not client:
        logger_param.error("Supabase client is not initialized. Cannot fetch automation details.")
        return pandas.DataFrame()
    try:
        logger_param.info("Fetching automation details from Supabase table 'automation_details'...")
        response = client.table('automation_details').select("*").execute()
        if response.data:
            df = pandas.DataFrame(response.data)
            logger_param.info(f"Successfully fetched {len(df)} automation detail records from Supabase.")
            
            # Ensure 'Type' column (case-sensitive based on previous updates) exists
            # Also check for 'Task_ID' and 'Company_name' as these are critical for Octoparse operations
            required_cols_supabase = ['Task_ID', 'Company_name', 'Type'] 
            missing_cols_supabase = [col for col in required_cols_supabase if col not in df.columns]
            
            if missing_cols_supabase:
                logger_param.error(f"Missing critical columns in data fetched from Supabase 'automation_details' table: {missing_cols_supabase}")
                logger_param.error(f"Available columns: {df.columns.tolist()}")
                return pandas.DataFrame() # Return empty DataFrame on critical error
            return df
        else:
            logger_param.warning("No data found in Supabase 'automation_details' table.")
            return pandas.DataFrame()
    except Exception as e:
        logger_param.error(f"Error fetching automation details from Supabase: {e}")
        return pandas.DataFrame()

def Clear_all_tasks(task_id_df, base_url_param, current_access_token, logger):
    """Clear all tasks specified in the task_id_df, logging their type (local/cloud)."""
    logger.info("\nTask_ID dictionary contents:")
    required_columns = ['Task_ID', 'Company_name', 'Type'] # Changed 'type' to 'Type'
    missing_columns = [col for col in required_columns if col not in task_id_df.columns]

    if missing_columns:
        logger.error(f"Error: The following required columns are missing in the input CSV data: {missing_columns}.")
        if hasattr(task_id_df, 'columns'):
            logger.error(f"Available columns: {task_id_df.columns.tolist()}")
        else:
            logger.error("Input data does not appear to be a pandas DataFrame with columns.")
        return
        
    logger.info(f"Task_ID values: {task_id_df['Task_ID']}")
    logger.info(f"Company_name values: {task_id_df['Company_name']}")
    logger.info(f"Type values: {task_id_df['Type']}") # Changed 'type' to 'Type'

    # Ensure all_tasks_tuples includes the Type
    all_tasks_tuples = list(zip(task_id_df['Task_ID'], task_id_df['Company_name'], task_id_df['Type'])) # Changed 'type' to 'Type'

    if not all_tasks_tuples:
        logger.info("\nNo tasks found in the Task_ID data. Exiting task processing.")
        return

    logger.info("\nStep 1: Clearing all tasks...")
    for task_id, task_name, task_type in all_tasks_tuples: # Unpack type here
        logger.info(f"\nProcessing task: {task_name} (ID: {task_id}), Type: {task_type}")
        logger.info(f"Attempting to clear data for {task_type} task: {task_name} (ID: {task_id})")
        clear_task_data(base_url_param, current_access_token, task_id, task_name, logger)
    
    logger.info("\nAll task clearing attempts finished.")
    # The "__main__" block will print the final "Script finished."

# Main execution block
if __name__ == "__main__":
    # --- Setup Logging ---
    log_file_path = os.path.join(os.path.dirname(__file__) if os.path.dirname(__file__) else '.', 'octoparse_script.log')
    
    # Create a logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO) # Set the logging level

    # Prevent duplicate handlers if script is re-run in same session (e.g. in an interactive interpreter)
    if not logger.handlers:
        # Create a file handler to write logs to a file
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setLevel(logging.INFO)

        # Create a console handler to print logs to the console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Create a formatter and set it for both handlers
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add both handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    # --- End Logging Setup ---

    logger.info("Script started.")
    start_time = datetime.now()
    logger.info(f"Script start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Initialize Supabase client if not already
    if not supabase_client and SUPABASE_KEY:
        try:
            supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
            logger.info("Supabase client initialized successfully in main block.")
        except Exception as e:
            logger.error(f"Failed to initialize Supabase client in main block: {e}")
            supabase_client = None # Ensure it's None if initialization fails
    elif not SUPABASE_KEY:
        logger.error("SUPABASE_KEY is not set. Cannot initialize Supabase client.")

    # Fetch automation details from Supabase
    Task_ID_data = get_automation_details_from_supabase(supabase_client, logger)

    if Task_ID_data.empty:
        logger.error("Failed to load Task ID data from Supabase. Exiting.")
        # Optionally, print final script duration even on early exit
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"Script finished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Total script duration: {duration}")
        exit(1) # Exit if critical data is missing

    # Log in to Octoparse API
    if log_in(base_api_url, login_username, login_password, logger):
        if access_token:
            logger.info("\nLogin successful. Proceeding to clear tasks...")
            # Call the Clear_all_tasks function
            Clear_all_tasks(Task_ID_data, base_api_url, access_token, logger)
            logger.info("\nScript finished.")
        else:
            logger.error("\nLogin successful, but access token was not obtained. Cannot proceed.")
    else:
        logger.error("\nLogin failed. Please check credentials and API availability.")

# Removed verify_task_exists function
# Removed clear_task_data function
# Removed Clear_task function
# The calls to log_in and Clear_task are now within the if __name__ == '__main__': block.

#Start_task_items(Task_ID) 