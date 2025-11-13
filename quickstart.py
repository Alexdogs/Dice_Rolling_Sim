import os
import random
import time
from pathlib import Path
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Constants
SPREADSHEET_ID = '1oUOWFWj-nYWiXKMb4cD1cd4RPKAX5nmMrWcRbpL_cXI'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
RESULTS_RANGE = 'Sheet1!A42002:A80000'  # Main results range
TIME_CELL = 'Sheet1!AB75'  # Cell for execution time (was incorrectly referenced as AB48 in print statement)
ITERATIONS = 500

# File paths
EC2_PATH = '/home/ec2-user/test.json'
LOCAL_PATH = '/Users/alexbielanski/pythonProject1/test.json'

# Target numbers and their ranges
TARGET_NUMBERS = {
    'a': {'range': (1, 4), 'target': 4},
    'b': {'range': (1, 6), 'target': 6},
    'c': {'range': (1, 8), 'target': 8},
    'd': {'range': (1, 10), 'target': 10},
    'e': {'range': (1, 12), 'target': 12},
    'f': {'range': (1, 20), 'target': 20}
}


def get_credentials():
    """Initialize and return Google Sheets credentials."""
    service_account_file = EC2_PATH if Path(EC2_PATH).exists() else LOCAL_PATH
    if not Path(service_account_file).exists():
        raise FileNotFoundError("Service account JSON file not found in either location")

    return service_account.Credentials.from_service_account_file(
        service_account_file, scopes=SCOPES)


def check_winning_numbers():
    """Check if randomly generated numbers match target values."""
    return all(
        random.randint(*num['range']) == num['target']
        for num in TARGET_NUMBERS.values()
    )


def generate_numbers():
    """Generate numbers and measure execution time."""
    start_time = time.time()
    results = []

    for iteration in range(ITERATIONS):
        attempts = 0
        while not check_winning_numbers():
            attempts += 1

        results.append(attempts)
        elapsed = time.time() - start_time
        print(f"Completed {iteration + 1} iterations in {elapsed:.2f} seconds")

    total_time = time.time() - start_time
    print(f"\nTotal execution time: {total_time:.2f} seconds")
    return results, total_time


def update_spreadsheet(service, results, total_time):
    """Update spreadsheet with results and execution time."""
    try:
        # Update main results
        wrapped_results = [[value] for value in results]
        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=RESULTS_RANGE,
            valueInputOption="RAW",
            body={'values': wrapped_results}
        ).execute()
        print(f"{result.get('updatedCells')} cells updated.")

        # Update execution time
        time_result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=TIME_CELL,
            valueInputOption="RAW",
            body={'values': [[total_time]]}
        ).execute()
        print(f"Execution time ({total_time:.2f} seconds) written to cell {TIME_CELL}")

    except Exception as e:
        print(f"An error occurred: {e}")


def confirm_cell_update():
    """Confirm with user if cells have been updated."""
    answer = input("Did you update the cells? (y/n): ").lower()
    if answer not in ['y', 'yes']:
        print("Program terminated. Please update cells before continuing.")
        exit()


def main():
    """Main program execution."""
    credentials = get_credentials()
    service = build('sheets', 'v4', credentials=credentials)

    confirm_cell_update()
    results, total_time = generate_numbers()
    update_spreadsheet(service, results, total_time)


if __name__ == "__main__":
    main()