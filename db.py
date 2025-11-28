import os
from dotenv import load_dotenv
import mysql.connector

load_dotenv()

def get_connection(db_name=None, autocommit=True):
    """
    db_name = None → nimmt Default aus .env (DB_NAME_RAW oder DB_NAME)
    db_name = "ticker" → nutzt DB_NAME_TICKER aus .env
    db_name = "analytics" → nutzt DB_NAME_ANALYTICS aus .env
    Oder direkt:
    db_name = "tickerdb"
    """

    # Wenn db_name ein env-key ist (z. B. "RAW", "TICKER", "ANALYTICS"):
    if db_name and not db_name.startswith("DB_"):
        env_key = f"DB_NAME_{db_name.upper()}"
        db_name = os.getenv(env_key)

    # Wenn db_name immer noch None → fallback:
    if not db_name:
        db_name = os.getenv("DB_NAME") or os.getenv("DB_NAME_RAW")

    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=db_name,
        autocommit=autocommit
    )
