"""Run SQL query and return dataframe"""
import time

import pandas as pd
import psycopg2

from dotenv import dotenv_values


def run_query(sql):
    """Run SQL query and return dataframe"""
    config = dotenv_values(".env")

    # Establish DB connection
    con = psycopg2.connect(
        database=config["DATABASE"],
        user=config["USERNAME"],
        password=config["PASSWORD"],
        host=config["HOST"],
        port=config["PORT"],
    )  # , options='-c statement_timeout=90000000'
    print("Database opened successfully")

    start_time = time.time()
    sql_query = pd.read_sql_query(sql, con, parse_dates=True)
    df = pd.DataFrame(sql_query)

    # Commit and close connection
    con.commit()
    print(f"run_query executed in {(time.time() - start_time)} seconds.")
    con.close()

    return df
