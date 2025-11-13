import time
import numpy as np
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ==== CONFIG ====
SERVICE_ACCOUNT_FILE = '/Users/alexbielanski/pythonProject1/test.json'
SPREADSHEET_ID = '1oUOWFWj-nYWiXKMb4cD1cd4RPKAX5nmMrWcRbpL_cXI'
SHEET_NAME = 'Sheet4'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

CHECK_EVERY = 100_000
CHUNK_WRITE = 9_000
P_SUCCESS = 1.0 / (4 * 6 * 8 * 10 * 12 * 20)
SEED = None

# ==== AUTH ====
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
sheets = build('sheets', 'v4', credentials=credentials)

# ==== HELPERS ====
def nice_time(seconds: float) -> str:
    m, s = divmod(seconds, 60)
    return f"{int(m)} min {s:.3f} sec" if m else f"{s:.3f} sec"

def write_chunked(values, start_row=2):
    """Write values to column A in chunks of ≤ CHUNK_WRITE rows per update."""
    total = len(values)
    for start in range(0, total, CHUNK_WRITE):
        sub = values[start:start + CHUNK_WRITE]
        wrapped = [[int(x)] for x in sub]
        srow = start_row + start
        erow = srow + len(sub) - 1
        sheets.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_NAME}'!A{srow}:A{erow}",
            valueInputOption="RAW",
            body={"values": wrapped}
        ).execute()
        print(f"Wrote rows A{srow}:A{erow}", flush=True)

# ==== MAIN LOOP ====
def main():
    print(">>> Running until a result equals 1 (first-try success)…", flush=True)
    rng = np.random.default_rng(SEED)
    start_time = time.perf_counter()
    results = []
    row_cursor = 2
    total_generated = 0

    while True:
        trials = rng.geometric(P_SUCCESS, size=CHECK_EVERY)
        failures = trials - 1
        results.extend(failures.tolist())
        total_generated += len(failures)

        if 1 in failures:
            stop_idx = int(np.where(failures == 1)[0][0])
            results = results[:len(results) - (len(failures) - stop_idx - 1)]
            print(f"Hit a 1 after {len(results):,} total runs!")
            break

        print(f"[{time.strftime('%H:%M:%S')}] Generated {total_generated:,} runs…", flush=True)

    write_chunked(results, start_row=row_cursor)

    elapsed = time.perf_counter() - start_time
    t_str = nice_time(elapsed)
    sheets.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{SHEET_NAME}'!M2",
        valueInputOption="RAW",
        body={"values": [[t_str]]}
    ).execute()

    print(f"Done ✅ Total runs: {len(results):,}  Runtime: {t_str}", flush=True)

if __name__ == "__main__":
    main()
