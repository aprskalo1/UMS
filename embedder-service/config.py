AUDIO_PATH = "audio/processed/"
FAISS_INDEX_PATH = "faiss/music.index"
MAPPING_PATH = "faiss/mapping.csv"

EMBEDDING_DIM = 768
TARGET_SAMPLING_RATE = 24000

YT_START_SECONDS = 0
YT_CLIP_SECONDS = 30
BATCH_LIMIT = 16

import os

DB_CONN_STR = os.getenv("DB_CONN_STR", "")

MSSQL_DRIVER = os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")
MSSQL_SERVER = os.getenv("MSSQL_SERVER", "localhost")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE", "UMSDB")
MSSQL_USERNAME = os.getenv("MSSQL_USERNAME", "sa")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD", "SQL")
MSSQL_TRUSTED = os.getenv("MSSQL_TRUSTED", "true").lower() in ("1", "true", "yes")


def build_default_conn_str() -> str:
    if DB_CONN_STR:
        return DB_CONN_STR
    if MSSQL_TRUSTED:
        return f"Driver={{{MSSQL_DRIVER}}};Server={MSSQL_SERVER};Database={MSSQL_DATABASE};Trusted_Connection=yes;"
    return f"Driver={{{MSSQL_DRIVER}}};Server={MSSQL_SERVER};Database={MSSQL_DATABASE};UID={MSSQL_USERNAME};PWD={MSSQL_PASSWORD};"
