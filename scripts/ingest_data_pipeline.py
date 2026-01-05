import os
import sys
from pathlib import Path
import snowflake.connector
from dotenv import load_dotenv
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Load .env if running locally
env_path = Path(__file__).resolve().parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)

# Environment variables
SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.environ.get("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ROLE = os.environ.get("SNOWFLAKE_ROLE", "access")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "cal_wh")
SNOWFLAKE_DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "musicdata")
SNOWFLAKE_RAW_SCHEMA = os.environ.get("SNOWFLAKE_RAW_SCHEMA", "raw")
LOCAL_DATA_PATH = os.environ.get("LOCAL_DATA_PATH")

# Validation
required_vars = {
    "SNOWFLAKE_ACCOUNT": SNOWFLAKE_ACCOUNT,
    "SNOWFLAKE_USER": SNOWFLAKE_USER,
    "SNOWFLAKE_PASSWORD": SNOWFLAKE_PASSWORD,
    "LOCAL_DATA_PATH": LOCAL_DATA_PATH,
}

missing_vars = [var for var, value in required_vars.items() if not value]
if missing_vars:
    print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
    sys.exit(1)


def clean_data(df):
    """Cleaning data from raw CSV"""
    # Strip whitespace from column names
    df.columns = df.columns.str.strip()
    
    # Strip whitespace from string columns
    string_columns = df.select_dtypes(include=['object']).columns
    for col in string_columns:
        if df[col].dtype == 'object':
            df[col] = df[col].str.strip()
    
    # Replace empty strings with None
    df.replace(['', 'NULL', 'null', 'N/A', 'NA'], None, inplace=True)
    
    # Remove duplicates
    original_rows = len(df)
    df.drop_duplicates(inplace=True)
    duplicates_removed = original_rows - len(df)
    if duplicates_removed > 0:
        print(f"  Removed {duplicates_removed} duplicate rows")
    
    return df


def convert_csv_to_parquet(csv_path, parquet_path, chunk_size=100000):
    """Convert CSV to Parquet format"""
    print(f"Converting {csv_path} to Parquet...")
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        first_line = f.readline()
        delimiter = '\t' if '\t' in first_line else ','
    
    parquet_writer = None
    total_rows = 0
    
    for chunk in pd.read_csv(
        csv_path,
        chunksize=chunk_size,
        delimiter=delimiter,
        low_memory=False,
        encoding='utf-8',
        on_bad_lines='skip'
    ):
        chunk = clean_data(chunk)
        table = pa.Table.from_pandas(chunk)
        
        if parquet_writer is None:
            parquet_writer = pq.ParquetWriter(
                parquet_path,
                table.schema,
                compression='snappy'
            )
        
        parquet_writer.write_table(table)
        total_rows += len(chunk)
    
    if parquet_writer:
        parquet_writer.close()
    
    print(f"✓ Converted {total_rows:,} rows to Parquet")
    return total_rows