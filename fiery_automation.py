import socket
import sys

# --- NETWORK STABILITY FIX: Force IPv4 & Long Timeout ---
# This prevents the script from "hanging" if the shop's Wi-Fi jitters 
# or tries to use IPv6 unnecessarily.
orig_getaddrinfo = socket.getaddrinfo
def getaddrinfo_ipv4(host, port, family=0, type=0, proto=0, flags=0):
    return orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)
socket.getaddrinfo = getaddrinfo_ipv4
socket.setdefaulttimeout(120)

import requests
import json
import time
import logging
import urllib3
import threading
import httplib2 
import google_auth_httplib2 
from dotenv import load_dotenv
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Suppress the InsecureRequestWarning from urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Set up logging for console output
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load .env file contents
load_dotenv() 

# Define printer keys
C5300S_KEY = os.getenv('PRINTER_1_KEY', 'C5300S') 
C5300S_2_KEY = os.getenv('PRINTER_2_KEY', 'C5300S_2') 

PRINTER_CONFIGS = {
    C5300S_KEY: {
        "ip": os.getenv('FIERY_C5300S_IP'),
        "username": os.getenv('FIERY_C5300S_USERNAME'),
        "password": os.getenv('FIERY_C5300S_PASSWORD'),
        "api_key": os.getenv('FIERY_C5300S_API_KEY')
    },
    C5300S_2_KEY: {
        "ip": os.getenv('FIERY_C5300S_2_IP'),
        "username": os.getenv('FIERY_C5300S_2_USERNAME'),
        "password": os.getenv('FIERY_C5300S_2_PASSWORD'),
        "api_key": os.getenv('FIERY_C5300S_2_API_KEY')
    }
}

# Google Sheet Details
GOOGLE_SHEET_ID = os.getenv('GOOGLE_SHEET_ID')
SERVICE_ACCOUNT_FILE = 'service_account.json'
SHEET_NAME = "Print Jobs"

# --- Google Sheets Functions ---
def get_sheets_service_instance():
    """Authenticates with Google Sheets API with a custom 120s timeout."""
    try:
        scope = ['https://www.googleapis.com/auth/spreadsheets']
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            logging.error(f"Missing credential file: {SERVICE_ACCOUNT_FILE}")
            return None
            
        creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scope)
        
        # Create a patient HTTP client
        http_patient = httplib2.Http(timeout=120)
        
        # Wrap credentials and timeout together
        authorized_http = google_auth_httplib2.AuthorizedHttp(creds, http=http_patient)
        
        # Build service using the authorized client
        service = build('sheets', 'v4', http=authorized_http, cache_discovery=False)
        return service
    except Exception as e:
        logging.error(f"Error initializing Google Sheets service instance: {e}")
        return None

def get_google_sheet_data(sheet_id, sheet_name=SHEET_NAME):
    logging.info("Connecting to Google Sheets...")
    service = get_sheets_service_instance() 
    if not service:
        return None, None
    
    # Try the request up to 3 times
    for attempt in range(3):
        try:
            if attempt > 0:
                logging.info(f"Retrying connection (Attempt {attempt + 1})...")
            
            result = service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=f'{sheet_name}!A:Z'
            ).execute()

            values = result.get('values', [])
            if not values:
                logging.info('No data found in the sheet.')
                return [], service
            else:
                logging.info(f"Successfully retrieved {len(values)} rows.")
                return values, service

        except Exception as e:
            if "timed out" in str(e).lower() or "deadline" in str(e).lower():
                logging.warning("Connection timed out. Waiting 5s...")
                time.sleep(5)
                continue
            logging.error(f"Error accessing Google Sheet: {e}")
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
        return True
    except Exception as e:
        logging.error(f"Error updating Sheet row {row_index + 2}: {e}")
        return False

def clear_google_sheet_columns_full(service, spreadsheet_id, sheet_name, start_col_letter, end_col_letter):
    try:
        status_range = f'{sheet_name}!{start_col_letter}2:{start_col_letter}'
        notes_range = f'{sheet_name}!{end_col_letter}2:{end_col_letter}'
        service.spreadsheets().values().batchClear(
            spreadsheetId=spreadsheet_id,
            body={'ranges': [status_range, notes_range]}
        ).execute()
        logging.info("Cleared old status/notes.")
    except Exception as e:
        logging.error(f"Error clearing columns: {e}")
        raise 

def clear_sheet_columns_after_delay_thread(spreadsheet_id, sheet_name, columns_to_clear, delay_minutes=5):
    time.sleep(delay_minutes * 60)
    sheets_service_for_thread = get_sheets_service_instance()
    if not sheets_service_for_thread: return
    try:
        clear_requests = [f'{sheet_name}!{col}2:{col}' for col in columns_to_clear]
        sheets_service_for_thread.spreadsheets().values().batchClear(
            spreadsheetId=spreadsheet_id,
            body={'ranges': clear_requests}
        ).execute()
        logging.info("Background cleanup complete.")
    except: pass

# --- Fiery API Functions ---
def fiery_login(session, config):
    FIERY_BASE_URL = f"https://{config['ip']}/live/api/v5"
    login_url = f"{FIERY_BASE_URL}/login"
    login_params = {
        "apikey": config['api_key'],
        "username": config['username'],
        "password": config['password']
    }
    try:
        response = session.post(login_url, params=login_params, verify=False, timeout=15)
        response.raise_for_status()
        if response.json().get('data', {}).get('item', {}).get('authenticated'):
            logging.info(f"Connected to Fiery at {config['ip']}")
            return True, FIERY_BASE_URL
        return False, None
    except Exception as e:
        logging.error(f"Fiery login error: {e}")
        return False, None

def get_held_jobs(session, fiery_base_url):
    try:
        response = session.get(f"{fiery_base_url}/jobs/held", verify=False, timeout=30)
        return response.json().get('data', {}).get('items', [])
    except Exception as e:
        logging.error(f"Error getting held jobs: {e}")
    return []

def change_job_copies_and_print(session, job_id, new_copies, fiery_base_url):
    headers = {"Content-Type": "application/json"}
    payload = {"attributes": {"numcopies": str(new_copies)}}
    try:
        session.put(f"{fiery_base_url}/jobs/{job_id}", headers=headers, json=payload, verify=False, timeout=15)
        session.put(f"{fiery_base_url}/jobs/{job_id}/print", headers=headers, json={}, verify=False, timeout=15)
        return True
    except Exception as e:
        logging.error(f"Error processing Fiery job {job_id}: {e}")
        return False

# --- Main Script Logic ---
def main():
    if len(sys.argv) < 2:
        logging.error("Missing printer key argument.")
        return

    printer_key = sys.argv[1]
    if printer_key not in PRINTER_CONFIGS or not PRINTER_CONFIGS[printer_key].get('ip'):
        logging.error(f"Invalid printer key: {printer_key}")
        return

    CONFIG = PRINTER_CONFIGS[printer_key]
    sheet_values, sheets_service = get_google_sheet_data(GOOGLE_SHEET_ID, SHEET_NAME)

    if not sheet_values or not sheets_service:
        logging.error("Could not reach Google Sheets. Check internet or credentials.")
        return

    headers = sheet_values[0]
    data_rows = sheet_values[1:]

    # Map columns
    status_col = 'N'
    notes_col = 'P'
    for idx, name in enumerate(headers):
        letter = chr(65 + idx) if idx < 26 else 'AA'
        if name == "Status": status_col = letter
        elif name == "Notes": notes_col = letter

    try:
        clear_google_sheet_columns_full(sheets_service, GOOGLE_SHEET_ID, SHEET_NAME, status_col, notes_col)
    except Exception as e:
        logging.error(f"Critical error during initial sheet clear: {e}")
        return

    session = requests.Session()
    logged_in, fiery_base_url = fiery_login(session, CONFIG)
    if not logged_in: return

    held_jobs = get_held_jobs(session, fiery_base_url)

    for i, row in enumerate(data_rows):
        row_dict = {headers[c]: row[c] if c < len(row) else "" for c in range(len(headers))}
        job_title = row_dict.get("Job Title", "").strip()
        copies = row_dict.get("Copies", "").strip()
        
        if not job_title or not copies: continue

        # Normalizes the sheet title to its first word for comparison
        norm_title = job_title.split(' ')[0].replace('#', '').upper()
        
        # --- FIX APPLIED HERE: Using exact match of the first word ---
        # This ensures '01983' does not match '01983C' (unless '01983C' is on the sheet)
        matches = [
            j for j in held_jobs 
            if j.get("title", "").replace('#', '').upper().split(' ')[0] == norm_title
        ]

        if matches:
            try:
                num = int(copies)
                results = []
                all_successful = True
                for mj in matches:
                    ok = change_job_copies_and_print(session, mj['id'], num, fiery_base_url)
                    results.append(f"{mj['title']} ({'OK' if ok else 'FAIL'})")
                    if not ok: all_successful = False
                
                status = "Printed" if all_successful else "Error"
                update_google_sheet_status(sheets_service, GOOGLE_SHEET_ID, SHEET_NAME, i, status_col, notes_col, status, "; ".join(results))
                logging.info(f"Processed: {job_title}")
            except:
                update_google_sheet_status(sheets_service, GOOGLE_SHEET_ID, SHEET_NAME, i, status_col, notes_col, "Error", "Invalid Qty")
        else:
            update_google_sheet_status(sheets_service, GOOGLE_SHEET_ID, SHEET_NAME, i, status_col, notes_col, "Not Found", "No held match")

    # Background cleanup
    threading.Thread(target=clear_sheet_columns_after_delay_thread, args=(GOOGLE_SHEET_ID, SHEET_NAME, ['D','E','L','M','O'], 5), daemon=True).start()
    logging.info("Task complete.")

if __name__ == "__main__":
    main()