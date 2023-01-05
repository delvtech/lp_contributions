# This script runs the queries to extract the data from the database.
# The output is a csv file with the data extracted
# The csv file is saved in the same folder as the script.

import time

import pandas as pd

from run_query import run_query

# Element liquidity
start_time = time.time()
# query = open("element_lps.sql", "r", encoding="utf-8")
sql_query = run_query(open("./element_lps.sql").read())
# query.close()
element_liquidity = pd.DataFrame(sql_query)
print(f"liquidity took {(time.time() - start_time):0.1f}s seconds, ")
element_liquidity.to_csv("./element_liquidity.csv", index=False)

# Element transfers
start_time = time.time()
# query = open("element_transfers.sql", "r", encoding="utf-8")
# read_csv = pd.read_csv("./element_liquidity.csv")
sql_query = run_query(open("./element_transfers.sql").read())
# query.close()
element_transfers = pd.DataFrame(sql_query)
print(f"transfers took {(time.time() - start_time):0.1f}s seconds, ")
element_transfers.to_csv("./element_transfers.csv", index=False)

print(
    f"liquidity has data from {element_liquidity['evt_block_time'].min()} to {element_liquidity['evt_block_time'].max()}"
)
print(
    f"containing data from {element_transfers['evt_block_time'].min()} to {element_transfers['evt_block_time'].max()}"
)
