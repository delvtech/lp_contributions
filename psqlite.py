import random
import string

import sqlite3
import pandas as pd

from sqlalchemy import create_engine
from dateutil import parser


def __random_string(length=10):
    return "".join(random.choice(string.ascii_lowercase) for i in range(length))


"""
>>> import psqlite as pql
>>> pql.register(df, name='my_df')
>>> out_df = pql.q('SELECT * FROM my_df WHERE field = 5')
>>> pql.drop('my_df')
"""

ENGINE = create_engine("sqlite://", echo=False)
TABLE_REGISTRY = tables = set([])


def q(query, return_result=True):
    if return_result:
        return pd.read_sql_query(query, con=ENGINE)
    ENGINE.execute(query)


def qnr(query):
    q(query, return_result=False)


def register(df, name):
    df.to_sql(name, con=ENGINE, if_exists="replace")
    TABLE_REGISTRY.add(name)


def create(query, name):
    ENGINE.execute("CREATE TABLE {name} AS {query}".format(name=name, query=query))
    TABLE_REGISTRY.add(name)


def drop(name):
    ENGINE.execute("DROP TABLE {name}".format(name=name))
    if name in TABLE_REGISTRY:
        TABLE_REGISTRY.remove(name)


def drop_all():
    table_registry_copy = list(TABLE_REGISTRY)
    for table in table_registry_copy:
        drop(table)


def qq(df, query):
    rs = __random_string()
    register(df, rs)
    result = q(query.format(rs))
    drop(rs)
    return result


def date_table(start="2009-01-03", end="2021-04-14", name="all_dates"):
    """
    Sometimes, when you want a continuous date series, it's helpful to OUTER JOIN against
    a table with all possible dates within a range. This creates that table with the name
    `all_dates` within the in-memory database.
    """
    df = (
        pd.date_range(start=start, end=end)
        .to_frame(name="each_date")
        .reset_index(drop=True)
    )
    register(df, "_date_table_tmp")
    if name in TABLE_REGISTRY:
        drop(name)
    create("SELECT DATE_PARSE(each_date) AS each_date FROM _date_table_tmp", name)
    drop("_date_table_tmp")
    print(
        "Registered {} in the in-memory SQLite instance. Each date is called `each_date`.".format(
            name
        )
    )
    return df


def date_diff_in_seconds(left, right):
    """ """
    try:
        left = parser.parse(left)
        right = parser.parse(right)
    except:
        return None
    return (left - right).total_seconds()


def date_parse(s, date_format="%Y-%m-%d"):
    """
    Parses an arbitrarily-formatted datestring and returns
    a new datestring in the desired format

    For datestrings, use:

    '%Y-%m-%d %H:%M:%S'
    """
    try:
        # t = parser.parse(s, parser.parserinfo(dayfirst=True))
        t = parser.parse(s)
        return t.strftime(date_format)
    except:
        return None


def datetime_parse(s):
    try:
        t = parser.parse(s)
        return t.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return None


def YEAR(s):
    return date_parse(s, "%Y")


def YEARMONTH(s):
    return date_parse(s, "%Y-%m")


def sqlite_power(x, n):
    return x**n


ENGINE.raw_connection().connection.create_function(
    "date_diff_in_seconds", 2, date_diff_in_seconds
)
ENGINE.raw_connection().connection.create_function("date_parse", 1, date_parse)
ENGINE.raw_connection().connection.create_function("datetime_parse", 1, datetime_parse)
ENGINE.raw_connection().connection.create_function("YEAR", 1, YEAR)
ENGINE.raw_connection().connection.create_function("YEARMONTH", 1, YEARMONTH)
ENGINE.raw_connection().connection.create_function("POWER", 2, sqlite_power)
