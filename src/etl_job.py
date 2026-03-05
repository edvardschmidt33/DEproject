# src/etl_job.py

import glob
import pandas as pd
import h5py
import os

INPUT_DIR = "data/hdf5"
OUTPUT_FILE = "data/parquet/msd.parquet"


def extract_data(file_path):

    with h5py.File(file_path, "r") as f:
        tempo = f["analysis"]["songs"]["tempo"][0]
        year = f["musicbrainz"]["songs"]["year"][0]

    return {"year": int(year), "tempo": float(tempo)}


def main():

    files = glob.glob(f"{INPUT_DIR}/**/*.h5", recursive=True)

    rows = []

    for file in files:
        try:
            row = extract_data(file)

            # ignore songs without year
            if row["year"] > 0:
                rows.append(row)

        except Exception:
            continue

    df = pd.DataFrame(rows)

    os.makedirs("data/parquet", exist_ok=True)

    df.to_parquet(OUTPUT_FILE, index=False)

    print("Parquet dataset written:", OUTPUT_FILE)
    print("Rows:", len(df))


if __name__ == "__main__":
    main()