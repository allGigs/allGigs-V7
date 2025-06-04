import requests
import json

def verify_task_exists(base_url, access_token, task_id):
    """
    Verify if a task exists before attempting to clear it.
    
    Args:
        base_url (str): Base URL of the API
        access_token (str): Current access token
        task_id (str): ID of the task to verify
        
    Returns:
        bool: True if task exists, False otherwise
    """
    try:
        headers = {
            'Authorization': f'Bearer {access_token}'
        }
        
        # Ensure base_url ends with a slash
        if not base_url.endswith('/'):
            base_url += '/'
            
        api_url = f"{base_url}api/v1/tasks/{task_id}"
        print(f"Verifying task ID: {task_id}")
        
        response = requests.get(api_url, headers=headers)
        
        if response.status_code == 200:
            print(f"Task ID {task_id} exists and is valid")
            return True
        elif response.status_code == 404:
            print(f"Task ID {task_id} does not exist or has been deleted")
            return False
        else:
            print(f"Error verifying task: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"Error verifying task: {str(e)}")
        return False

def clear_task_data(base_url, access_token, task_id, task_name):
    """
    Clear a specific task using the API.
    
    Args:
        base_url (str): Base URL of the API
        access_token (str): Current access token
        task_id (str): ID of the task to clear
        task_name (str): Name of the task (for logging)
        
    Returns:
        bool: True if task was cleared successfully
    """
    try:
        # First verify the task exists
        if not verify_task_exists(base_url, access_token, task_id):
            print(f"Skipping task '{task_name}' as it does not exist")
            return False
            
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }
        
        # Ensure base_url ends with a slash
        if not base_url.endswith('/'):
            base_url += '/'
            
        api_url = f"{base_url}api/v1/tasks/{task_id}/clear"
        print(f"Attempting to clear task with ID: {task_id}")
        print(f"Using URL: {api_url}")
        
        response = requests.post(
            api_url,
            headers=headers
        )
        
        if response.status_code == 200:
            print(f"Successfully cleared task: {task_name}")
            return True
        elif response.status_code == 404:
            print(f"Task not found. Please verify the task ID: {task_id}")
            print(f"Response content: {response.text}")
            return False
        else:
            print(f"Failed to clear task '{task_name}'. Status code: {response.status_code}")
            print(f"Response content: {response.text}")
            return False
            
    except Exception as e:
        print(f"Error in clear_task_data: {str(e)}")
        return False

def Clear_task(Task_ID):
    """
    Clear multiple tasks with automatic token refresh and retry logic.
    
    Args:
        Task_ID (dict): Dictionary containing Task_ID and Task_name
    """
    global access_token, refresh_token
    
    print("\nTask_ID dictionary contents:")
    print(f"Keys: {Task_ID.keys()}")
    print(f"Task_ID values: {Task_ID['Task_ID']}")
    print(f"Task_name values: {Task_ID['Task_name']}")
    
    for index, Task in enumerate(Task_ID['Task_ID']):
        Task_ID_name = Task_ID['Task_name'].iloc[index]
        print(f"\nProcessing task: {Task_ID_name}")
        print(f"Task ID: {Task}")
        
        # First attempt to clear the task
        if clear_task_data(base_url, access_token, Task, Task_ID_name):
            continue
            
        # If first attempt failed, try refreshing token
        print(f"Clearing task '{Task_ID_name}' failed. Attempting to refresh token.")
        if refresh_token:
            if refresh_token_function(base_url, refresh_token):
                print("Token refreshed successfully. Retrying clear task.")
                if clear_task_data(base_url, access_token, Task, Task_ID_name):
                    continue
                    
        # If token refresh failed, try logging in again
        print("Token refresh failed. Attempting to log in again.")
        if log_in(base_url, username, password):
            print("Re-login successful. Retrying clear task.")
            if clear_task_data(base_url, access_token, Task, Task_ID_name):
                continue
                
        print(f"Failed to clear task '{Task_ID_name}' after all attempts.")
        print("Please check if:")
        print("1. The task ID is correct")
        print("2. You have the necessary permissions")
        print("3. The task is in a state that can be cleared")
        print("4. The API endpoint is correct (should be /api/v1/tasks/{task_id}/clear)")
        break  # Stop processing if all attempts failed
        
    return None 