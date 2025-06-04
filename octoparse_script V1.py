import pandas
import requests
import json
import time
import os # Added from helper file
from datetime import datetime # Added import
import logging # Added for logging

# Global variables for credentials and tokens
Task_ID_data = pandas.read_csv('/Users/jaapjanlammers/Desktop/Freelancedirectory/Important_allGigs/automation_details.csv', sep= ';')
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

def Clear_start_and_download_tasks(task_id_df, base_url_param, current_access_token, _, logger):
    """Clear all tasks, then sequentially start tasks with a 1-minute interval between each start."""
    logger.info("\nTask_ID dictionary contents:")
    if 'Task_ID' not in task_id_df.columns or 'Company_name' not in task_id_df.columns:
        logger.error("Error: 'Task_ID' or 'Company_name' column not found in the input CSV data.")
        if hasattr(task_id_df, 'columns'):
            logger.error(f"Available columns: {task_id_df.columns.tolist()}")
        else:
            logger.error("Input data does not appear to be a pandas DataFrame with columns.")
        return
        
    # logger.info(f"Keys: {task_id_df.keys()}") # Original line, might be redundant if columns check is good - commented out
    logger.info(f"Task_ID values: {task_id_df['Task_ID']}")
    logger.info(f"Company_name values: {task_id_df['Company_name']}")

    all_tasks_tuples = list(zip(task_id_df['Task_ID'], task_id_df['Company_name']))

    if not all_tasks_tuples:
        logger.info("\nNo tasks found in the Task_ID data. Exiting task processing.")
        return

    logger.info("\nStep 1: Clearing all tasks...")
    for task_id, task_name in all_tasks_tuples:
        logger.info(f"\nClearing task: {task_name} (ID: {task_id})")
        clear_task_data(base_url_param, current_access_token, task_id, task_name, logger)
    
    logger.info("\nStep 2: Sequentially starting tasks with a 1-minute interval between starts...")
    for i, (task_id, task_name) in enumerate(all_tasks_tuples):
        logger.info(f"\n--- Initiating start for task {i+1}/{len(all_tasks_tuples)}: {task_name} (ID: {task_id}) ---")
        
        logger.info(f"Attempting to start task: {task_name}")
        if start_task(base_url_param, current_access_token, task_id, task_name, logger):
            logger.info(f"Successfully initiated start for task: {task_name}.")
        else:
            logger.warning(f"Failed to start task '{task_name}'.")

        if i < len(all_tasks_tuples) - 1:
            logger.info(f"\nWaiting 1 minute before attempting to start the next task ({i+2}/{len(all_tasks_tuples)})...")
            time.sleep(60) # 1 minute
        else:
            logger.info(f"\nAll tasks have been initiated for starting.")

    logger.info(f"\nAll task clearing and starting stages finished.")
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
        # Create a console handler to also print logs to the console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        # Create a formatter and set it for both handlers
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        # Add the handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    logger.info("Script started.") # Added start timestamp
    start_time = datetime.now()

    if not access_token or not refresh_token:
        logger.info("Attempting initial login...")
        if not log_in(base_api_url, login_username, login_password, logger):
            logger.error("Initial login failed. Exiting script.")
            exit()
    else:
        logger.info("Tokens already exist. Attempting to refresh token...")
        if not refresh_token_function(base_api_url, refresh_token, logger):
            logger.warning("Failed to refresh token. Attempting full login as fallback...")
            if not log_in(base_api_url, login_username, login_password, logger):
                logger.error("Fallback login failed. Exiting script.")
                exit()

    # Correctly call the main function with all required arguments, including the newly added logger
    Clear_start_and_download_tasks(Task_ID_data, base_api_url, access_token, refresh_token, logger)

    end_time = datetime.now()
    logger.info(f"Script finished in {end_time - start_time}.") 