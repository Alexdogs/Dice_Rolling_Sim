import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
import random


SERVICE_ACCOUNT_FILE = '/Users/alexbielanski/pythonProject1/test.json'


SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)


service = build('sheets', 'v4', credentials=credentials)


SPREADSHEET_ID = '1oUOWFWj-nYWiXKMb4cD1cd4RPKAX5nmMrWcRbpL_cXI'

def generate_numbers():
    def repeat():
        a = random.randint(1, 4)
        b = random.randint(1, 6)
        c = random.randint(1, 8)
        d = random.randint(1, 10)
        e = random.randint(1, 12)
        f = random.randint(1, 20)

        return a == 4 and b == 6 and c == 8 and d == 10 and e == 12 and f == 20
    mylist = []

    for x in range(500):
        i = 0
        while True:
            if repeat():
                break
            i += 1

        # Store the number of iterations for this loop
        mylist.append(i)

        # Print if the loop count is 1
        if i == 1:
            print("You did it!")
            print(i)

    return mylist  # Return the list of all iterations


results = generate_numbers()
print("Number of iterations for each attempt:", results)


wrapped_results = [[value] for value in results]
print("Wrapped results:", wrapped_results)


RANGE_NAME = 'Sheet2!A2:A502'


body = {
    'values': wrapped_results
}


try:
    result = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_NAME,
        valueInputOption="RAW",
        body=body
    ).execute()

    print(f"{result.get('updatedCells')} cells updated.")
except Exception as e:
    print("An error occurred while writing to the sheet:", e)
