# d4-d100_until_one_autorotate_on_10M_oauth.py
import os
import time
import datetime as dt
import numpy as np

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# ==== CONFIG ====
PRIMARY_SPREADSHEET_ID = '1oUOWFWj-nYWiXKMb4cD1cd4RPKAX5nmMrWcRbpL_cXI'  # your main workbook
DATA_SHEET = 'Sheet1'
INDEX_SHEET = 'Index'

# OAuth scopes: Sheets + Drive (create files & manage permissions)
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

CHECK_EVERY = 100_000         # rolls per iteration
CHUNK_WRITE = 9_000           # rows per API call
P_SUCCESS = 1.0 / (4*6*8*10*12*20*100)  # d4..d100
SEED = None                   # set integer for reproducibility or None

CREDENTIALS_FILE = "1credentials.json"   # downloaded OAuth client (desktop)
TOKEN_FILE = "token.json"               # created on first auth

# ==== AUTH (OAuth user) ====
def get_oauth_services():
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
    sheets = build("sheets", "v4", credentials=creds)
    drive = build("drive", "v3", credentials=creds)
    return sheets, drive

# ==== UTIL =====
def nice_time(seconds: float) -> str:
    m, s = divmod(seconds, 60)
    return f"{int(m)} min {s:.3f} sec" if m else f"{s:.3f} sec"

def is_10m_limit_error(err: Exception) -> bool:
    if not isinstance(err, HttpError):
        return False
    try:
        details = getattr(err, "error_details", None) or []
        for d in details:
            msg = d.get("message", "")
            if "above the limit of 10000000 cells" in msg:
                return True
    except Exception:
        pass
    return "above the limit of 10000000 cells" in str(err)

def ensure_index_sheet(sheets, spreadsheet_id):
    meta = sheets.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields="sheets.properties"
    ).execute()
    titles = {s["properties"]["title"] for s in meta["sheets"]}
    if INDEX_SHEET not in titles:
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": INDEX_SHEET}}}]}
        ).execute()
        sheets.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"'{INDEX_SHEET}'!A:C",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [["Timestamp", "Spreadsheet Title", "Spreadsheet ID"]]}
        ).execute()

def index_log(sheets, parent_spreadsheet_id, new_id, title):
    ts = dt.datetime.now().isoformat(timespec="seconds")
    sheets.spreadsheets().values().append(
        spreadsheetId=parent_spreadsheet_id,
        range=f"'{INDEX_SHEET}'!A:C",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [[ts, title, new_id]]}
    ).execute()

def share_with_user(drive, file_id, user_email, role="writer"):
    # Optional when using OAuth (you already own the file). Safe to leave as-is.
    try:
        drive.permissions().create(
            fileId=file_id,
            body={"type": "user", "role": role, "emailAddress": user_email},
            fields="id"
        ).execute()
        print(f"Shared {file_id} with {user_email} ({role})", flush=True)
    except HttpError as e:
        print(f"⚠️ Could not share file: {e}", flush=True)

def create_new_spreadsheet_like(sheets, drive, base_title="Runs", data_sheet=DATA_SHEET, auto_share_email=None):
    title = f"{base_title} {dt.datetime.now().strftime('%Y-%m-%d %H.%M.%S')}"
    resp = sheets.spreadsheets().create(
        body={
            "properties": {"title": title},
            "sheets": [{"properties": {"title": data_sheet}}]
        }
    ).execute()
    new_id = resp["spreadsheetId"]
    if auto_share_email:
        share_with_user(drive, new_id, auto_share_email)
    return new_id, title

def append_chunk_or_rotate(sheets, drive, values_rows, active_spreadsheet_id):
    try:
        sheets.spreadsheets().values().append(
            spreadsheetId=active_spreadsheet_id,
            range=f"'{DATA_SHEET}'!A:A",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": values_rows}
        ).execute()
        return active_spreadsheet_id
    except HttpError as e:
        if is_10m_limit_error(e):
            new_id, title = create_new_spreadsheet_like(sheets, drive, "Runs Part", DATA_SHEET)
            print(f"[Rotate] 10M limit reached. Created new spreadsheet: {title} ({new_id})", flush=True)
            try:
                ensure_index_sheet(sheets, PRIMARY_SPREADSHEET_ID)
                index_log(sheets, PRIMARY_SPREADSHEET_ID, new_id, title)
            except Exception:
                pass
            sheets.spreadsheets().values().append(
                spreadsheetId=new_id,
                range=f"'{DATA_SHEET}'!A:A",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": values_rows}
            ).execute()
            return new_id
        raise

def write_chunked_autorotate(sheets, drive, values, active_spreadsheet_id):
    for i in range(0, len(values), CHUNK_WRITE):
        sub = values[i:i + CHUNK_WRITE]
        rows = [[int(x)] for x in sub]
        active_spreadsheet_id = append_chunk_or_rotate(sheets, drive, rows, active_spreadsheet_id)
        print(f"Appended {len(sub):,} rows -> {active_spreadsheet_id}:{DATA_SHEET}!A:A", flush=True)
    return active_spreadsheet_id

# ==== MAIN ====
def main():
    sheets, drive = get_oauth_services()

    print(">>> Rolling d4, d6, d8, d10, d12, d20, d100 until we hit all 1s…", flush=True)
    rng = np.random.default_rng(SEED)
    start_time = time.perf_counter()

    active_spreadsheet_id = PRIMARY_SPREADSHEET_ID

    results = []
    total_generated = 0
    while True:
        # geometric() returns trial count to first success (min = 1)
        trials = rng.geometric(P_SUCCESS, size=CHECK_EVERY)
        results.extend(trials.tolist())
        total_generated += len(trials)

        if 1 in trials:
            stop_idx = int(np.where(trials == 1)[0][0])
            results = results[:len(results) - (len(trials) - stop_idx - 1)]
            print(f"Hit a 1 after {len(results):,} total runs!", flush=True)
            break
        print(f"[{time.strftime('%H:%M:%S')}] Generated {total_generated:,} runs…", flush=True)

    active_spreadsheet_id = write_chunked_autorotate(sheets, drive, results, active_spreadsheet_id)

    elapsed = time.perf_counter() - start_time
    t_str = nice_time(elapsed)
    try:
        sheets.spreadsheets().values().update(
            spreadsheetId=active_spreadsheet_id,
            range=f"'{DATA_SHEET}'!M2",
            valueInputOption="RAW",
            body={"values": [[t_str]]}
        ).execute()
    except Exception:
        pass

    print(f"Done ✅ Total runs: {len(results):,}  Runtime: {t_str}", flush=True)

if __name__ == "__main__":
    main()

