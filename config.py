import sqlalchemy.exc
import psycopg2
from sqlalchemy import create_engine

DB_USER = "postgres"
DB_PASS = "postgres"
IP = "db_postgres"
DB_PORT = "5432"
DB_NAME = "ml_postgres_in"
DB_NAME_BASE = "postgres"
TBL_NAME = "ml_table_from_s3"
conn_db = "postgresql://{DB_USER}:{DB_PASS}@{IP}:{DB_PORT}/{DB_NAME_BASE}"
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{IP}:{DB_PORT}/{DB_NAME}")
engine2 = create_engine(
    f"postgresql://{DB_USER}:{DB_PASS}@{IP}:{DB_PORT}/{DB_NAME_BASE}"
)

conn_dic = {
    "DB_USER": "postgres",
    "DB_PASS": "postgres",
    "IP": "db_postgres",
    "DB_PORT": "5432",
    "DB_NAME": "ml_postgres_in",
    "DB_NAME_BASE": "postgres",
    "TBL_NAME" = "ml_table_from_s3",
}

if __name__ == "__main__":
    ()