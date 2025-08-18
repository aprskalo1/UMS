import os
from urllib.parse import quote_plus
from pydantic import BaseModel
from typing import List, Dict, Optional
import yaml


class SceneConfig(BaseModel):
    name: str
    keywords: List[str]
    negative_keywords: List[str] = []
    daily_admit_cap: int = 200


class GlobalConfig(BaseModel):
    duration_min_sec: int = 90
    duration_max_sec: int = 480
    allow_categories: List[int] = [10]
    search_max_playlists_per_scene: int = 40
    search_results_per_query: int = 25
    per_playlist_seed_count: int = 3


class Settings(BaseModel):
    mssql: Optional[Dict[str, str]] = None
    country_scenes: Dict[str, List[SceneConfig]]
    global_: GlobalConfig

    @staticmethod
    def load(path: str = "config.yaml") -> "Settings":
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return Settings(
            mssql=data.get("mssql"),
            country_scenes=data["country_scenes"],
            global_=data["global"]
        )


YT_API_KEY = os.getenv("YT_API_KEY", "")
if not YT_API_KEY:
    print("⚠️  Set YT_API_KEY environment variable.")

DB_CONN_STR = os.getenv("DB_CONN_STR", "")

MSSQL_DRIVER = os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")
MSSQL_SERVER = os.getenv("MSSQL_SERVER", "localhost")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE", "UMSDB")
MSSQL_USERNAME = os.getenv("MSSQL_USERNAME", "sa")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD", "SQL")
MSSQL_TRUSTED = os.getenv("MSSQL_TRUSTED", "true").lower() in ("1", "true", "yes")

MSSQL_ENCRYPT = os.getenv("MSSQL_ENCRYPT", "yes")
MSSQL_TRUST_CERT = os.getenv("MSSQL_TRUST_CERT", "yes")


def build_default_conn_str() -> str:
    if DB_CONN_STR:
        return DB_CONN_STR
    base = f"Driver={{{MSSQL_DRIVER}}};Server={MSSQL_SERVER};Database={MSSQL_DATABASE};"
    if MSSQL_TRUSTED:
        return base + f"Trusted_Connection=yes;Encrypt={MSSQL_ENCRYPT};TrustServerCertificate={MSSQL_TRUST_CERT};"
    return (base +
            f"UID={MSSQL_USERNAME};PWD={MSSQL_PASSWORD};"
            f"Encrypt={MSSQL_ENCRYPT};TrustServerCertificate={MSSQL_TRUST_CERT};")


def to_sqlalchemy_url(odbc_conn_str: str) -> str:
    return "mssql+pyodbc:///?odbc_connect=" + quote_plus(odbc_conn_str)


def get_sqlalchemy_url(settings: Optional[Settings] = None) -> str:
    if settings and settings.mssql and settings.mssql.get("dsn"):
        dsn = settings.mssql["dsn"]
        if dsn.strip().lower().startswith("driver="):
            return to_sqlalchemy_url(dsn)
        return dsn
    return to_sqlalchemy_url(build_default_conn_str())
