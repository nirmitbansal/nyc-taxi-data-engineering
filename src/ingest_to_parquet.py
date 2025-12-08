import os
import glob
import pandas as pd
from src.config import RAW_DATA_PATH, PROCESSED_DIR, CHUNK_SIZE

def basic_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    # Don't drop columns – keep everything
    df = df.copy()

    # 🔹 NEW: drop index column if present
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Try to convert common datetime columns, only if they exist
    datetime_candidates = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
        "pickup_datetime",
        "dropoff_datetime"
    ]
    for col in datetime_candidates:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Clean numeric columns if they exist
    if "fare_amount" in df.columns:
        df = df[df["fare_amount"] >= 0]

    if "trip_distance" in df.columns:
        df = df[df["trip_distance"] >= 0]

    return df


def ingest_to_parquet():
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    csv_files = glob.glob(RAW_DATA_PATH)
    print(f"Found {len(csv_files)} CSV files.")

    file_index = 0
    for file in csv_files:
        print(f"\nProcessing file: {file}")
        reader = pd.read_csv(file, chunksize=CHUNK_SIZE, low_memory=False)

        for chunk in reader:
            cleaned = basic_cleaning(chunk)

            # Skip empty chunks
            if cleaned.empty or cleaned.shape[1] == 0:
                continue

            out_path = os.path.join(PROCESSED_DIR, f"chunk_{file_index}.parquet")
            cleaned.to_parquet(out_path, index=False)
            print(f"Saved {out_path} ({len(cleaned)} rows)")
            file_index += 1

    print("\nIngestion completed.")

if __name__ == "__main__":
    ingest_to_parquet()
