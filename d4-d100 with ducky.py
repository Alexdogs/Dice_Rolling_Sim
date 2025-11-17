import time
import datetime as dt
import numpy as np
import duckdb
import pandas as pd

DB_FILE = "runs_1_0.duckdb"
TABLE_NAME = "runs"
CHECK_EVERY = 100_000
P_SUCCESS = 1.0 / (4*6*8*10*12*20*100)
SEED = None

def main():
    con = duckdb.connect(DB_FILE)

    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            run_index BIGINT,
            trials BIGINT,
            batch_timestamp TEXT
        );
    """)

    print("Starting simulation…")
    start = dt.datetime.now()

    rng = np.random.default_rng(SEED)
    run_index = 1
    total_generated = 0

    while True:
        trials = rng.geometric(P_SUCCESS, size=CHECK_EVERY)
        ts = dt.datetime.now().isoformat()

        # Create real DataFrame
        df = pd.DataFrame({
            "run_index": np.arange(run_index, run_index + len(trials)),
            "trials": trials,
            "batch_timestamp": [ts] * len(trials)
        })

        # Register it for SQL
        con.register("df", df)

        # Check for success
        success_idx = np.where(trials == 1)[0]
        if success_idx.size > 0:
            stop = success_idx[0]
            con.execute(f"INSERT INTO {TABLE_NAME} SELECT * FROM df LIMIT {stop + 1}")
            print(f"Success at run {run_index + stop:,}")
            break

        # Insert the whole batch normally
        con.execute(f"INSERT INTO {TABLE_NAME} SELECT * FROM df")

        run_index += CHECK_EVERY
        total_generated += CHECK_EVERY

        if total_generated % 1_000_000 == 0:
            print(f"Generated {total_generated:,} rows…")

    end = dt.datetime.now()
    print("Total time:", end - start)
    con.close()

if __name__ == "__main__":
    main()
