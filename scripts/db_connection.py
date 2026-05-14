import os
import psycopg2
from dotenv import load_dotenv


def get_connection():
    load_dotenv()

    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "olist_dw")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD")

    if not db_password:
        raise ValueError("DB_PASSWORD is missing. Add it to your .env file.")

    return psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password,
    )