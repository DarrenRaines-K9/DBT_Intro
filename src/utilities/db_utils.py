from dotenv import load_dotenv
import os
import pyodbc
import pandas as pd

load_dotenv(override=True)

SERVER = os.getenv("DB_SERVER")
USERNAME = os.getenv("DB_USERNAME")
PASSWORD = os.getenv("DB_PASSWORD")
DRIVER = os.getenv("DB_DRIVER")
db_name = os.getenv("DB_NAME")


def get_connection_string(db_name):
    return (
        f"DRIVER={DRIVER};"
        f"SERVER={SERVER};"
        f"DATABASE={db_name};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        f"TrustServerCertificate=yes"
    )


def insert_data(data, table_name, db_name):
    conn_str = get_connection_string(db_name)
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        columns = list(data.columns)
        placeholders = ", ".join(["?"] * len(columns))
        column_names = ", ".join(columns)
        sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"

        print(f"Inserting {len(data)} rows into {table_name}...")
        for idx, row in data.iterrows():
            values = []
            for val in row:
                if (
                    pd.isna(val)
                    or val == ""
                    or (isinstance(val, str) and val.strip() == "")
                ):
                    values.append(None)
                else:
                    values.append(val)
            cursor.execute(sql, tuple(values))
        conn.commit()
        print(f"Successfully inserted {len(data)} rows into {table_name}.")


if __name__ == "__main__":
    data = pd.read_csv("data/customers(1).csv")
    insert_data(data, "dbt_schema.customers", db_name)
