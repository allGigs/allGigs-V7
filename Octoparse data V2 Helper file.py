import requests
import json
import time
import os

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
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }
        
        # Use the correct endpoint from the documentation
        api_url = f"{base_url}data/remove"
        print(f"Attempting to clear task with ID: {task_id}")
        print(f"Using URL: {api_url}")
        
        # Prepare the request body according to documentation
        payload = {
            "taskId": task_id
        }
        
        response = requests.post(
            api_url,
            headers=headers,
            json=payload
        )
        
        print(f"Response status code: {response.status_code}")
        print(f"Response content: {response.text}")
        
        if response.status_code == 200:
            print(f"Successfully cleared task: {task_name}")
            return True
        else:
            print(f"Failed to clear task '{task_name}'. Status code: {response.status_code}")
            print(f"Response content: {response.text}")
            return False
            
    except Exception as e:
        print(f"Error in clear_task_data: {str(e)}")
        return False

def start_task(base_url, access_token, task_id, task_name):
    """
    Start a specific task using the Octoparse API.
    
    Args:
        base_url (str): Base URL of the API
        access_token (str): Current access token
        task_id (str): ID of the task to start
        task_name (str): Name of the task (for logging)
        
    Returns:
        bool: True if task was started successfully
    """
    try:
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }
        
        # Use the correct endpoint from the documentation
        api_url = f"{base_url}cloudextraction/start"
        print(f"Attempting to start task with ID: {task_id}")
        print(f"Using URL: {api_url}")
        
        # Prepare the request body according to documentation
        payload = {
            "taskId": task_id
        }
        
        response = requests.post(
            api_url,
            headers=headers,
            json=payload
        )
        
        print(f"Response status code: {response.status_code}")
        print(f"Response content: {response.text}")
        
        if response.status_code == 200:
            print(f"Successfully started task: {task_name}")
            return True
        else:
            print(f"Failed to start task '{task_name}'. Status code: {response.status_code}")
            print(f"Response content: {response.text}")
            return False
            
    except Exception as e:
        print(f"Error in start_task: {str(e)}")
        return False

def check_task_status(base_url, access_token, task_id, task_name):
    """
    Check if a task has completed running.
    
    Args:
        base_url (str): Base URL of the API
        access_token (str): Current access token
        task_id (str): ID of the task to check
        task_name (str): Name of the task (for logging)
        
    Returns:
        bool: True if task is complete, False otherwise
    """
    try:
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }
        
        # Use the correct endpoint from the documentation
        api_url = f"{base_url}cloudextraction/statuses/v2"
        payload = {
            "taskIds": [task_id]  # API expects an array of task IDs
        }
        
        print(f"\nChecking status for task: {task_name}")
        print(f"Using URL: {api_url}")
        print(f"Payload: {payload}")
        
        response = requests.post(
            api_url,
            headers=headers,
            json=payload
        )
        
        print(f"Response status code: {response.status_code}")
        print(f"Response content: {response.text}")
        
        if response.status_code == 200:
            status_data = response.json()
            print(f"Full status data: {status_data}")
            
            # Check if we have data and the task status
            if 'data' in status_data and len(status_data['data']) > 0:
                task_status = status_data['data'][0].get('status', '').lower()
                print(f"Task status: {task_status}")
                
                # Consider 'Finished' as completion state
                is_complete = task_status == 'finished'
                
                if is_complete:
                    print(f"Task '{task_name}' has completed")
                else:
                    print(f"Task '{task_name}' is still running with status: {task_status}")
                
                return is_complete
            else:
                print(f"No status data found for task '{task_name}'")
                return False
        else:
            print(f"Failed to check status for task '{task_name}'. Status code: {response.status_code}")
            print(f"Response content: {response.text}")
            return False
            
    except Exception as e:
        print(f"Error checking task status: {str(e)}")
        return False

def download_task_data(base_url, access_token, task_id, task_name):
    """
    Download data from a specific task using the API.
    
    Args:
        base_url (str): Base URL of the API
        access_token (str): Current access token
        task_id (str): ID of the task to download data from
        task_name (str): Name of the task (for logging)
        
    Returns:
        bool: True if data was downloaded successfully
    """
    try:
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }
        
        # Use the correct endpoint from the documentation
        api_url = f"{base_url}data/download"
        print(f"Attempting to download data for task ID: {task_id}")
        print(f"Using URL: {api_url}")
        
        # Prepare the request body according to documentation
        payload = {
            "taskId": task_id
        }
        
        response = requests.post(
            api_url,
            headers=headers,
            json=payload
        )
        
        print(f"Response status code: {response.status_code}")
        
        if response.status_code == 200:
            # Use the specified absolute path for downloads
            downloads_dir = "/Users/jaapjanlammers/Desktop/Freelancedirectory"
            if not os.path.exists(downloads_dir):
                os.makedirs(downloads_dir)
                print(f"Created directory: {downloads_dir}")
            
            # Save the downloaded data to a file in the specified directory using only task name
            filename = os.path.join(downloads_dir, f"{task_name}.csv")
            with open(filename, 'wb') as f:
                f.write(response.content)
            print(f"Successfully downloaded data for task: {task_name}")
            print(f"Data saved to: {filename}")
            return True
        else:
            print(f"Failed to download data for task '{task_name}'. Status code: {response.status_code}")
            print(f"Response content: {response.text}")
            return False
            
    except Exception as e:
        print(f"Error in download_task_data: {str(e)}")
        return False

def Clear_start_and_download_tasks(Task_ID, base_url, access_token, _):
    """
    Clear all tasks first, then maintain 6 running tasks at all times.
    Start new tasks as soon as others complete.
    
    Args:
        Task_ID (dict): Dictionary containing Task_ID and Task_name
        base_url (str): Base URL of the API
        access_token (str): Current access token
    """
    print("\nTask_ID dictionary contents:")
    print(f"Keys: {Task_ID.keys()}")
    print(f"Task_ID values: {Task_ID['Task_ID']}")
    print(f"Task_name values: {Task_ID['Task_name']}")
    
    # Step 1: Clear all tasks first
    print("\nStep 1: Clearing all tasks...")
    for index, Task in enumerate(Task_ID['Task_ID']):
        Task_ID_name = Task_ID['Task_name'].iloc[index]
        print(f"\nClearing task: {Task_ID_name}")
        clear_task_data(base_url, access_token, Task, Task_ID_name)
    
    # Step 2: Process tasks maintaining 6 running tasks at all times
    print("\nStep 2: Starting tasks and maintaining 6 running tasks...")
    max_running_tasks = 6
    running_tasks = []  # List of (task_id, task_name) tuples
    completed_tasks = set()  # Set of completed task IDs
    all_tasks = list(zip(Task_ID['Task_ID'], Task_ID['Task_name']))
    task_index = 0
    
    while task_index < len(all_tasks) or running_tasks:
        # Start new tasks if we have capacity
        while len(running_tasks) < max_running_tasks and task_index < len(all_tasks):
            Task, Task_ID_name = all_tasks[task_index]
            if Task not in completed_tasks:
                print(f"\nStarting task: {Task_ID_name}")
                if start_task(base_url, access_token, Task, Task_ID_name):
                    running_tasks.append((Task, Task_ID_name))
                else:
                    print(f"Failed to start task '{Task_ID_name}'.")
            task_index += 1
        
        # Check status of running tasks
        for Task, Task_ID_name in running_tasks[:]:
            if check_task_status(base_url, access_token, Task, Task_ID_name):
                print(f"\nDownloading data for completed task: {Task_ID_name}")
                if download_task_data(base_url, access_token, Task, Task_ID_name):
                    completed_tasks.add(Task)
                else:
                    print(f"Failed to download data for task '{Task_ID_name}'.")
                running_tasks.remove((Task, Task_ID_name))
        
        # If we have running tasks, wait before checking again
        if running_tasks:
            print(f"\nCurrently running {len(running_tasks)} tasks. Waiting for completion...")
            time.sleep(30)  # Check status every 30 seconds
    
    print("\nAll tasks have been processed!")
    print(f"Total tasks completed: {len(completed_tasks)}")
    return None 