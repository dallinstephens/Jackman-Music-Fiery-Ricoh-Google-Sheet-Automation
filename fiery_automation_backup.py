import requests
import json
import time
import logging
import urllib3
import threading

# NEW IMPORTS for google-api-python-client
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Suppress the InsecureRequestWarning from urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Set up logging for console output
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
# Fiery Server Details
FIERY_IP = "192.168.1.191"
FIERY_USERNAME = "admin"
FIERY_PASSWORD = "Fiery.1"
# IMPORTANT: This key might be expired. If the script fails, get a new one from Fiery.com.
# >>>>>>>>>>>>>> IMPORTANT: REPLACE THIS WITH YOUR NEW FIERY API KEY! <<<<<<<<<<<<<<
FIERY_API_KEY = "paWHCNpxi9NukVRSLFMUFnVlawVAv5z83YPahZKND8dtGauptch7GXK/PBJWo3SVvXOaUp2ElXIktXB98fz/W6TOhMAg5GjwF2YpjgNMgwxWU9rW/Kpsx1P5i2Aql9ct+YGFzPkjEi7i3OtZnfxXH4uS4fwU4NFiNuLu55aFgLVhJvLCIyeZxdZXPQYcAX2OdRg9b2z1wogK+7wuBiKviEj2zHkMziN6v4LMKAhv6cS3xPK/GZrEabbzWRduwJ0dRq1QfiZ22KCFvYGIpNsBniB+9KBzQsLZmAb8wbtcakP+NLIknVbcXvestjpJwViy3AEUfY/jqGmQdJ06xT2fpg=="

# Google Sheet Details
GOOGLE_SHEET_ID = "1CUjCvNDpFtFpHWIisjnFAkC3CjYmIjC5wlRuDEuOu6E"
SERVICE_ACCOUNT_FILE = 'service_account.json'
SHEET_NAME = "Print Jobs"

# Fiery API Base URL
FIERY_BASE_URL = f"https://{FIERY_IP}/live/api/v5"

# --- Google Sheets Functions ---
def get_sheets_service_instance():
    """Authenticates with Google Sheets API and returns a new service object."""
    try:
        scope = ['https://www.googleapis.com/auth/spreadsheets']
        creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scope)
        service = build('sheets', 'v4', credentials=creds)
        return service
    except Exception as e:
        logging.error(f"Error initializing Google Sheets service instance: {e}")
        return None

def get_google_sheet_data(sheet_id, sheet_name=SHEET_NAME):
    logging.info("Starting get_google_sheet_data with google-api-python-client...")
    service = get_sheets_service_instance() # Get service here
    if not service:
        return None, None
    
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=f'{sheet_name}!A:Z'
        ).execute()

        values = result.get('values', [])

        if not values:
            logging.info('No data found in the sheet.')
            return [], service
        else:
            logging.info(f"Successfully retrieved {len(values)} rows from '{sheet_name}'.")
            return values, service

    except HttpError as err:
        logging.error(f"Google Sheets API error: {err}")
        return None, None
    except Exception as e:
        logging.error(f"An unexpected error occurred while accessing Google Sheet: {e}")
        return None, None

def update_google_sheet_status(service, sheet_id, sheet_name, row_index, status_col_letter, notes_col_letter, status, notes=""):
    try:
        sheet_row_number = row_index + 2
        values = [[status, notes]]
        body = {'values': values}
        
        service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=f'{sheet_name}!{status_col_letter}{sheet_row_number}:{notes_col_letter}{sheet_row_number}',
            valueInputOption='RAW',
            body=body
        ).execute()

        logging.info(f"Sheet updated for row {sheet_row_number}: Status='{status}', Notes='{notes}'")
        return True
    except HttpError as err:
        logging.error(f"Google Sheets API update error for row {sheet_row_number}: {err}")
        return False
    except Exception as e:
        logging.error(f"Error updating Google Sheet for row {sheet_row_number}: {e}")
        return False

def clear_google_sheet_columns_full(service, spreadsheet_id, sheet_name, start_col_letter, end_col_letter):
    """
    Clears all data in a range of columns in a Google Sheet starting from row 2.
    Uses open-ended ranges for robustness.
    """
    logging.info(f"Preparing to fully clear columns {start_col_letter}-{end_col_letter} in sheet '{sheet_name}' from row 2.")
    try:
        status_range = f'{sheet_name}!{start_col_letter}2:{start_col_letter}'
        notes_range = f'{sheet_name}!{end_col_letter}2:{end_col_letter}'
        
        clear_requests_body = {
            'ranges': [status_range, notes_range]
        }
        
        service.spreadsheets().values().batchClear(
            spreadsheetId=spreadsheet_id,
            body=clear_requests_body
        ).execute()

        logging.info(f"Successfully cleared columns '{start_col_letter}' and '{end_col_letter}' in sheet '{sheet_name}' from row 2.")
    except HttpError as err:
        logging.error(f"Google Sheets API error during full column clearing: {err}")
        raise # Re-raise to prevent script from proceeding if initial clear fails critically
    except Exception as e:
        logging.error(f"An unexpected error occurred during full column clearing: {e}")
        raise # Re-raise

# --- MODIFIED: Function to run in a separate thread for delayed clearing ---
def clear_sheet_columns_after_delay_thread(spreadsheet_id, sheet_name, columns_to_clear, delay_minutes=5):
    """
    This function will be run in a separate thread. It waits for a delay
    and then clears the specified columns from row 2 downwards.
    It re-initializes its own sheets_service.
    """
    print(f"--- THREAD START: Background clearing thread initiated. Waiting {delay_minutes} minutes...")
    logging.info(f"Background thread initiated: Waiting {delay_minutes} minutes to clear columns: %s...", ', '.join(columns_to_clear))

    time.sleep(delay_minutes * 60)

    print(f"--- THREAD AFTER SLEEP: {delay_minutes} minutes elapsed. Attempting to re-initialize Google Sheets service.")
    logging.info(f"Background thread: {delay_minutes} minutes elapsed. Attempting to re-initialize Google Sheets service.")

    sheets_service_for_thread = get_sheets_service_instance()
    if not sheets_service_for_thread:
        print("--- THREAD ERROR: Failed to re-initialize Google Sheets service. Cannot clear columns.")
        logging.error("Background thread: Failed to re-initialize Google Sheets service. Cannot clear columns.")
        return

    print(f"--- THREAD CLEARING: Proceeding to clear columns: %s.", ', '.join(columns_to_clear))
    logging.info(f"Background thread: Proceeding to clear columns: %s.", ', '.join(columns_to_clear))
    try:
        clear_requests = []
        for col in columns_to_clear:
            clear_requests.append(f'{sheet_name}!{col}2:{col}')

        clear_requests_body = {
            'ranges': clear_requests
        }

        sheets_service_for_thread.spreadsheets().values().batchClear(
            spreadsheetId=spreadsheet_id,
            body=clear_requests_body
        ).execute()

        print(f"--- THREAD SUCCESS: Successfully cleared columns: %s after delay.", ', '.join(columns_to_clear))
        logging.info(f"Background thread: Successfully cleared columns: %s after delay.", ', '.join(columns_to_clear))
    except HttpError as err:
        print(f"--- THREAD ERROR (HTTP): Google Sheets API error during delayed clearing: {err}")
        logging.error(f"Background thread: Google Sheets API error during delayed clearing: {err}")
    except Exception as e:
        print(f"--- THREAD ERROR (GENERIC): An unexpected error occurred during delayed clearing: {e}")
        logging.error(f"Background thread: An unexpected error occurred during delayed clearing: {e}")

# --- Fiery API Functions (no changes) ---
def fiery_login(session):
    login_url = f"{FIERY_BASE_URL}/login"
    headers = {"Accept": "application/json"}
    login_params = {
        "apikey": FIERY_API_KEY,
        "username": FIERY_USERNAME,
        "password": FIERY_PASSWORD
    }
    
    logging.info("Attempting to log into Fiery API...")
    try:
        response = session.post(login_url, headers=headers, params=login_params, verify=False, timeout=10)
        response.raise_for_status()
        data = response.json()
        logging.debug(f"Fiery API Login Response: {data}")
        if data.get('data', {}).get('item', {}).get('authenticated') == True:
            logging.info("Successfully authenticated with Fiery API!")
            return True
        else:
            logging.error("Fiery API Login failed. Check username, password, or API key.")
            if "error" in data:
                logging.error(f"Error details from Fiery: {data['error']}")
            return False
            
    except requests.exceptions.HTTPError as errh:
        logging.error(f"HTTP Error logging into Fiery: {errh}")
        logging.error(f"Response status code: {errh.response.status_code}")
        logging.error(f"Response text: {errh.response.text}")
        return False
    except requests.exceptions.ConnectionError as errc:
        logging.error(f"Connection Error logging into Fiery: {errc}")
        logging.error("Please check Fiery IP address and network connectivity.")
        return False
    except requests.exceptions.Timeout as errt:
        logging.error(f"Timeout logging into Fiery: {errt}")
        return False
    except requests.exceptions.RequestException as err:
        logging.error(f"An unexpected error occurred during Fiery login: {err}")
        return False

def get_held_jobs(session):
    """Retrieves all jobs that are in a 'held' state."""
    jobs_url = f"{FIERY_BASE_URL}/jobs/held"
    headers = {"Accept": "application/json"}
    
    logging.info(f"Retrieving all jobs in 'held' state from Fiery API...")
    try:
        response = session.get(jobs_url, headers=headers, verify=False, timeout=60)
        response.raise_for_status()
        
        jobs_data = response.json()
        jobs_list = jobs_data.get('data', {}).get('items', [])
        
        if jobs_list:
            logging.info(f"Found {len(jobs_list)} jobs in 'held' state.")
            return jobs_list
        else:
            logging.info("No jobs found in 'held' state or unexpected response format.")
            return []
            
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP Error retrieving held jobs: {http_err}")
        logging.error(f"Response status code: {http_err.response.status_code}")
        logging.error(f"Response text: {http_err.response.text}")
    except requests.exceptions.ConnectionError as conn_err:
        logging.error(f"Connection Error retrieving held jobs: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        logging.error(f"Error retrieving held jobs: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        logging.error(f"An unexpected error occurred during job retrieval: {req_err}")
    return []

def change_job_copies_and_print(session, job_id, new_copies):
    """Changes the number of copies for a job and then prints it."""
    url = f"{FIERY_BASE_URL}/jobs/{job_id}"
    
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"  
    }

    payload = {
        "attributes": {
            "numcopies": str(new_copies)
        }
    }

    logging.info(f"Attempting to set copies for Job ID {job_id} to {new_copies} (as str: '{str(new_copies)}') via JSON body with 'attributes.numcopies' structure...")

    try:
        response = session.put(url, headers=headers, json=payload, verify=False, timeout=10)
        response.raise_for_status()

        response_data = response.json()
        
        logging.debug(f"Fiery API response for setting copies (attributes.numcopies): {response_data}")

        if response_data.get('data', {}).get('item', {}).get('id') == job_id:
            logging.info(f"SUCCESS: Successfully sent request to set copies for Job ID {job_id} to {new_copies} via 'attributes.numcopies' structure. VERIFY IN CWS.")
        else:
            logging.error(f"ERROR: Fiery API did not return expected job ID after setting copies for Job ID {job_id}: {response_data}")
            return False

        logging.info(f"Attempting to print Job ID {job_id}...")
        print_url = f"{FIERY_BASE_URL}/jobs/{job_id}/print"  
        response = session.put(print_url, headers=headers, json={}, verify=False, timeout=10)
        response.raise_for_status()
        logging.info(f"Successfully sent print command for Job ID {job_id}.")
        
        return True
        
    except requests.exceptions.HTTPError as errh:
        logging.error(f"HTTP Error setting copies or printing job {job_id}: {errh}")
        logging.error(f"Response status code: {errh.response.status_code}")
        logging.error(f"Response text: {errh.response.text}")
        return False
    except Exception as e:
        logging.error(f"Error setting copies or printing job {job_id}: {e}")
        return False

def list_all_fiery_queues(session): # Example function (not used in main logic)
    logging.info("Attempting to list all Fiery queues...")
    queues_url = f"{FIERY_BASE_URL}/queues"
    headers = {"Accept": "application/json"}

    try:
        response = session.get(queues_url, headers=headers, verify=False, timeout=30)
        response.raise_for_status()

        queues_data = response.json()
        
        queues_list = queues_data.get('data', {}).get('items', [])
        
        if queues_list:
            logging.info(f"Successfully retrieved {len(queues_list)} queues:")
            for queue in queues_list:
                logging.info(f"  Queue ID: {queue.get('id', 'N/A')}, Name: {queue.get('name', 'N/A')}")
            return queues_list
        else:
            logging.info("No queues found or unexpected response format after parsing 'data.items'.")
            return []
            
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP Error listing queues: {http_err}")
        logging.error(f"Response status code: {http_err.response.status_code}")
        logging.error(f"Response text: {http_err.response.text}")
    except requests.exceptions.RequestException as req_err:
        logging.error(f"An unexpected error occurred during queue listing: {req_err}")
    return []

# --- Main Script Logic ---
def main():
    logging.info("Starting Fiery Automation Script...")

    # Authenticate and get data from Google Sheet
    # sheets_service is now returned from get_google_sheet_data
    sheet_values, sheets_service = get_google_sheet_data(GOOGLE_SHEET_ID, SHEET_NAME)

    if sheet_values is None or sheets_service is None: # Check if service was initialized
        logging.error("Could not retrieve data from Google Sheet or initialize service. Exiting.")
        return

    headers = sheet_values[0] if sheet_values else []
    data_rows = sheet_values[1:] if sheet_values and len(sheet_values) > 1 else []

    sheet_data = [] # This will store rows as dictionaries
    for row_idx_in_values, row in enumerate(data_rows): # Iterate through data_rows for 0-indexed reference
        row_dict = {}
        for col_idx, header in enumerate(headers):
            row_dict[header] = row[col_idx] if col_idx < len(row) else ""
        sheet_data.append(row_dict)

    if not sheet_data:
        logging.info("No valid data rows found in Google Sheet (excluding headers).")

    # Determine column letters for Status and Notes from headers
    def col_idx_to_letter(idx):
        letter = ''
        while idx >= 0:
            letter = chr(65 + (idx % 26)) + letter
            idx = (idx // 26) - 1
        return letter

    status_col_letter = None
    notes_col_letter = None
    for idx, header_name in enumerate(headers):
        if header_name == "Status":
            status_col_letter = col_idx_to_letter(idx)
        elif header_name == "Notes":
            notes_col_letter = col_idx_to_letter(idx)

    # --- Clear Status and Notes columns BEFORE processing jobs ---
    if status_col_letter and notes_col_letter:
        try:
            clear_google_sheet_columns_full(sheets_service, GOOGLE_SHEET_ID, SHEET_NAME, status_col_letter, notes_col_letter)
        except Exception as e:
            logging.error(f"Failed to clear Google Sheet columns initially: {e}. Exiting as this is a critical step.")
            return # Exit if initial clear fails
    else:
        logging.error("Missing column headers for initial clear. Exiting.")
        return # Exit if initial clear cannot be performed

    session = requests.Session()

    logged_in = fiery_login(session)
    if not logged_in:
        logging.error("Failed to log into Fiery API. Exiting.")
        return

    held_jobs = get_held_jobs(session)
    if not held_jobs:
        logging.info("No jobs found in 'held' state or failed to retrieve them.")

    # Process each job in the Google Sheet
    for i, row_dict in enumerate(sheet_data): # 'i' is the 0-indexed row number in sheet_data
        job_title_from_sheet = row_dict.get("Job Title", "").strip()
        copies_from_sheet = row_dict.get("Copies", "").strip()
        
        # --- Adjusted Code Block ---
        # Check if either 'Job Title' or 'Copies' is empty.
        # This handles cases where clearContent() leaves an empty string.
        if job_title_from_sheet == "" or copies_from_sheet == "":
            logging.info(f"Skipping row {i + 2}: Missing 'Job Title' or 'Copies' (likely cleared).")
            # You can decide to update the sheet status here or simply do nothing
            # if the cells are empty. The original script's logic is fine for this.
            continue
        # --- End of Adjusted Code Block ---

        logging.info(f"\nProcessing spreadsheet entry: Job Title '{job_title_from_sheet}', Copies '{copies_from_sheet}'. (Sheet row {i+2})")

        # Normalize sheet title for comparison: strip whitespace, remove leading '#', take first word, uppercase
        normalized_sheet_id_for_comparison = job_title_from_sheet.split(' ')[0].replace('#', '').strip().upper()

        logging.info(f"Normalized sheet ID for comparison: '{normalized_sheet_id_for_comparison}'")

        matching_fiery_job_ids = []
        matching_fiery_job_titles = [] # To include full Fiery job titles in notes

        # Iterate through all held jobs to find ALL matches
        for fiery_job in held_jobs:
            fiery_job_id = fiery_job.get("id")
            fiery_job_title = fiery_job.get("title", "").strip()

            # Normalize Fiery title: strip whitespace, remove leading '#', take first word, uppercase
            normalized_fiery_title_cleaned = fiery_job_title.replace('#', '').strip().upper()
            fiery_first_word = normalized_fiery_title_cleaned.split(' ')[0] if normalized_fiery_title_cleaned else ""

            logging.debug(f"Comparing sheet '{normalized_sheet_id_for_comparison}' with Fiery job '{fiery_first_word}' (full title: '{fiery_job_title}')")

            if normalized_sheet_id_for_comparison == fiery_first_word:
                matching_fiery_job_ids.append(fiery_job_id)
                matching_fiery_job_titles.append(fiery_job_title) # Store the original full title
                logging.info(f"Match found! Fiery Job ID: {fiery_job_id}, Full Title: '{fiery_job_title}'. Adding to list for processing.")
            
            # Important: DO NOT 'break' here. Continue to find all matches.

        if matching_fiery_job_ids:
            try:
                num_copies = int(copies_from_sheet) # Convert copies to integer once
                all_jobs_successful = True
                notes_messages = []

                for job_id_to_process, job_full_title in zip(matching_fiery_job_ids, matching_fiery_job_titles):
                    logging.info(f"Attempting to process matched Fiery Job ID: {job_id_to_process}, Title: '{job_full_title}' with {num_copies} copies.")
                    
                    # Call the function to change copies and print for each matched job
                    if change_job_copies_and_print(session, job_id_to_process, num_copies):
                        notes_messages.append(f"'{job_full_title}' Qty: {num_copies}")
                    else:
                        all_jobs_successful = False
                        notes_messages.append(f"Failed to process '{job_full_title}'.")
                
                # Determine final status for the spreadsheet row
                final_status = "Printed" if all_jobs_successful else "Error (Partial/Full Failure)"
                final_notes = f"Processed {len(matching_fiery_job_ids)} jobs: {'; '.join(notes_messages)}"

                if sheets_service and status_col_letter and notes_col_letter:
                    update_google_sheet_status(sheets_service, GOOGLE_SHEET_ID, SHEET_NAME, i,
                                               status_col_letter, notes_col_letter, final_status, final_notes)

            except ValueError:
                logging.error(f"Error: Invalid 'Copies' value '{copies_from_sheet}' for job '{job_title_from_sheet}'. Must be a number.")
                if sheets_service and status_col_letter and notes_col_letter:
                    update_google_sheet_status(sheets_service, GOOGLE_SHEET_ID, SHEET_NAME, i,
                                               status_col_letter, notes_col_letter, "Error", "Invalid 'Copies' value")
        else:
            logging.info(f"No Fiery jobs found matching '{job_title_from_sheet}' (first word '{normalized_sheet_id_for_comparison}') in 'held' queue.")
            if sheets_service and status_col_letter and notes_col_letter:
                update_google_sheet_status(sheets_service, GOOGLE_SHEET_ID, SHEET_NAME, i,
                                           status_col_letter, notes_col_letter, "Not Found", f"No matching Fiery jobs found for '{job_title_from_sheet}'")
    
    logging.info("Fiery Automation Script Finished processing jobs. Data is now visible in Google Sheet.")

    # --- Start a background thread to clear columns after a delay ---
    # Define a list of all columns to be cleared
    columns_to_clear = ['D', 'E', 'L', 'M', 'O']

    # sheets_service is no longer passed as it's re-initialized in the thread
    clear_thread = threading.Thread(target=clear_sheet_columns_after_delay_thread,
                                    args=(GOOGLE_SHEET_ID, SHEET_NAME,
                                          columns_to_clear, 5))
    clear_thread.daemon = True
    clear_thread.start()
    logging.info("Background clearing thread started. Main script will now exit.")

    logging.info("Main script execution complete. The .command window will now close.")

if __name__ == "__main__":
    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
    main()