import re
import requests
import pandas as pd
import psqlite as pql
import numpy as np
import time
import statistics
import json

from collections import defaultdict
from datetime import datetime, timedelta, date

from run_query import run_query

CUTOFF_STARTING_DATE = "2021-01-01"
CUTOFF_ENDING_DATE = "2022-12-31"

# read from csv
element_liquidity = pd.read_csv("./element_liquidity.csv")
element_transfers = pd.read_csv("./element_transfers.csv")
print(
    f"# of rows, liquidity: {len(element_liquidity)}, transfers: {len(element_transfers)}"
)
# cut off dates
print(f"selecting dates between {CUTOFF_STARTING_DATE} and {CUTOFF_ENDING_DATE}")
element_liquidity = element_liquidity.loc[
    (element_liquidity["evt_block_time"] >= CUTOFF_STARTING_DATE)
    & (element_liquidity["evt_block_time"] <= CUTOFF_ENDING_DATE)
]
element_transfers = element_transfers.loc[
    (element_transfers["block_time"] >= CUTOFF_STARTING_DATE)
    & (element_transfers["block_time"] <= CUTOFF_ENDING_DATE)
]
print(
    f"# of rows, liquidity: {len(element_liquidity)}, transfers: {len(element_transfers)}"
)

# fix usd value
element_liquidity["price"] = (
    element_liquidity["deposit_size_base_usd"] / element_liquidity["deposit_size_base"]
)
element_liquidity["deposit_size_base_usd"] = (
    element_liquidity["lp_tokens_acquired"] * element_liquidity["price"]
)

# get token names
token_names = pd.read_csv("element_tokens.csv")
# add token name to element transfers
element_transfers = pd.merge(
    element_transfers,
    token_names.loc[:, ["token_address_raw", "token_name"]],
    how="left",
    left_on="contract_address",
    right_on="token_address_raw",
).rename(columns={"token_name": "token"})

# token mappings
contract_url = "https://raw.githubusercontent.com/element-fi/elf-deploy/main/addresses/mainnet.json"
element_token_mappings = requests.get(contract_url).json()
tokens_from_github = []
for token, data in element_token_mappings["tranches"].items():
    for tranche in data:
        for token_type in ("ptPool", "ytPool"):
            if token_type in tranche:
                tokens_from_github.append(
                    {
                        "expiry_timestamp": tranche["expiration"],
                        "expiry_datetime": datetime.fromtimestamp(
                            tranche["expiration"]
                        ).strftime("%Y-%m-%d %H:%M:%S"),
                        "token_address_raw": "\\"
                        + tranche[token_type]["address"][1:].lower(),
                        "token_type": "LPeP" if token_type == "ptPool" else "LPeY",
                        "poolId": "\\" + tranche[token_type]["poolId"][1:].lower(),
                    }
                )
tokens_from_github = pd.DataFrame(tokens_from_github)
token_expiry = pd.merge(tokens_from_github, token_names, on="token_address_raw")
token_expiry = token_expiry.loc[
    :, ["token_address_raw", "token_name", "expiry_timestamp", "expiry_datetime"]
]
pql.register(token_expiry, "token_expiry")

# rename liquidity data to match onchain token names
print("renaming liquidity data to match onchain token names")
old_names = [
    "LPePyvcrvSTETH-16SEP22",
    "LPePyvcrvSTETH-24FEB23",
    "LPePyvCurveLUSD-16SEP22",
    "LPePyvcrv3crypto-16SEP22",
]
new_names = [
    "LPePyvCurve-stETH-16SEP22",
    "LPePyvCurve-stETH-24FEB23",
    "LPePyvCurve-LUSD-16SEP22",
    "LPePyvCurve-3Crypto-16SEP22",
]
for idx in range(len(old_names)):
    rows_to_replace = element_liquidity.lp_token == old_names[idx]
    element_liquidity.loc[rows_to_replace, "lp_token"] = new_names[idx]
    print(
        f"replaced {sum(rows_to_replace):2g} rows for {old_names[idx]:24s} with {new_names[idx]:27s}"
    )

# add to pql
pql.register(element_liquidity, "liquidity")
pql.register(element_transfers, "transfers")

# the `lp_events` table contains all events related to Element LP tokens
# and contains expiry metadata for each related token

lp_events = pql.q(
    """
     
SELECT ROW_NUMBER() OVER (PARTITION BY address, token_expiry.token_name
                          ORDER BY datetime ASC) AS row_num,
       DATETIME_PARSE(datetime) AS datetime,
       tx_hash,
       address, -- signer: et."from"
       liquidityProvider,
       token_expiry.token_name AS token_name,
       token_change,
       usd_change,
       recipient_address,
       event_type,
       expiry_timestamp,
       DATETIME(expiry_timestamp, 'unixepoch') AS expiry_datetime,
       evt_index,
       tx_index

 FROM (

    SELECT evt_block_time AS datetime,
           evt_tx_hash AS tx_hash,
           liquidity_provider AS address,
           liquidityProvider,
           lp_token,
           lp_tokens_acquired AS token_change,
           deposit_size_base_usd AS usd_change,
           '' AS recipient_address,
           'deposit' AS event_type,
           "index" AS evt_index,
           tx_index
      FROM liquidity
     WHERE lp_tokens_acquired >= 0

     UNION ALL

    SELECT evt_block_time AS datetime,
           evt_tx_hash AS tx_hash,
           liquidity_provider AS address,
           liquidityProvider,
           lp_token,
           lp_tokens_acquired AS token_change,
           deposit_size_base_usd AS usd_change,
           '' AS recipient_address,
           'withdraw' AS event_type,
           "index" AS evt_index,
           tx_index
      FROM liquidity
     WHERE lp_tokens_acquired < 0

     UNION ALL

    SELECT block_time AS datetime,
           tx_hash AS tx_hash,
           sender AS address,
           '' AS liquidityProvider,
           token AS lp_token,
           -tokens_transferred AS token_change,
           0 AS usd_change,
           recipient AS recipient_address,
           'send' AS event_type,
           "index" AS evt_index,
           tx_index
      FROM transfers
     WHERE token LIKE 'LP%'

) a LEFT JOIN token_expiry
      ON a.lp_token = token_expiry.token_name

-- note that tie-breaking sorting using the `evt_index`
-- resolves cases where smart contracts may receive funds
-- and then disburse them in a single transaction
ORDER BY datetime ASC, tx_index ASC, evt_index ASC

"""
)
pql.register(lp_events, "lp_events")

# list all contracts where signer isn't same as sender
listOfDoubleAddresses = list(
    set(
        (
            lp_events.loc[
                lp_events.address != lp_events.liquidityProvider, ["liquidityProvider"]
            ]
        ).liquidityProvider.values
    )
)
listOfDoubleAddresses = listOfDoubleAddresses[1:]

# replace signer with sender for these contracts
print("replacing signer with sender for these contracts")
n = 0
for double_address in listOfDoubleAddresses:
    idxAffectedRows = lp_events.liquidityProvider == double_address
    different_rows = sum(
        lp_events.loc[idxAffectedRows, "address"]
        != lp_events.loc[idxAffectedRows, "liquidityProvider"]
    )
    n += different_rows
    print(
        f"overwriting liquidity_provider address for {double_address}"
        f"for {different_rows} rows"
    )
    lp_events.loc[idxAffectedRows, "address"] = lp_events.loc[
        idxAffectedRows, "liquidityProvider"
    ]
print(f"total affected rows replaced: {n}. nice!")

# what's the most recent timestamp in the LP data? used later
max_datetime = pql.q("SELECT MAX(datetime) FROM lp_events")["MAX(datetime)"][0]

# helper function to look up previous balance
def get_usd_balance_for_address(address_index, address):
    """
    Note that since this is based off of the 'rolling_usd_balance' it
    includes the EARLY_BIRD_BONUS.
    """
    usd_balance_total = 0
    for token_name in address_index[address].keys():
        usd_balance_total += address_index[address][token_name][-1][
            "rolling_usd_balance"
        ]
    return usd_balance_total


def build_token_index(lp_events):
    """
    USD credit for LPs is given at the time of deposit
    i.e., the ETH price at the time of deposit is how their "credit"
    for the airdrop is calculated

    when LP tokens are transferred between addresses, we must retain
    the "credit value" of the tokens even upon transfer
    """

    EARLY_BIRD_BONUS = 1.0  # 0%
    EARLY_BIRD_USD_THRESHOLD = 0 * 1e6  # 0 million
    EARLY_BIRD_TIME_DELTA = timedelta(days=0)  # 0 days

    # for storing full rows for a given address, token pair
    address_token_index = defaultdict(dict)
    # for storing the cumulative deposits (and not withdrawals) to a pool
    pool_index = {}

    # for storing full rows for a given address, token pair
    address_token_index = defaultdict(dict)
    # for storing the cumulative deposits (and not withdrawals) to a pool
    pool_index = {}
    # for storing bad addresses to be investigated later (withdraw before deposit)
    bad_address = []
    bad_address_zero_balance = []

    # iterate according to:
    # ORDER BY datetime ASC, tx_index ASC, evt_index ASC
    for _, row in lp_events.iterrows():
        new_row = dict(row)

        address = new_row["address"]
        event_type = new_row["event_type"]
        token_name = new_row["token_name"]
        if token_name is None:
            display("warning: none token name:")
            display(new_row)
        token_change = new_row["token_change"]
        usd_change = new_row["usd_change"]
        event_datetime = new_row["datetime"]
        recipient_address = new_row["recipient_address"]

        # membership in `address_token_index`
        if not token_name in address_token_index[address]:

            if event_type == "deposit":

                # update cumulative token deposits
                if token_name not in pool_index:
                    apply_early_bird_bonus = True
                    pool_index[token_name] = {}
                    pool_index[token_name]["usd_balance"] = usd_change
                    pool_index[token_name][
                        "first_deposit_datetime"
                    ] = datetime.strptime(event_datetime, "%Y-%m-%d %H:%M:%S")
                else:
                    elapsed_time = (
                        datetime.strptime(event_datetime, "%Y-%m-%d %H:%M:%S")
                        - pool_index[token_name]["first_deposit_datetime"]
                    )
                    if (
                        pool_index[token_name]["usd_balance"]
                        <= EARLY_BIRD_USD_THRESHOLD
                        and elapsed_time < EARLY_BIRD_TIME_DELTA
                    ):
                        apply_early_bird_bonus = True
                    else:
                        apply_early_bird_bonus = False
                    pool_index[token_name]["usd_balance"] += usd_change

                new_row["cumulative_deposits_for_pool"] = pool_index[token_name][
                    "usd_balance"
                ]
                new_row["first_deposit_for_pool_datetime"] = pool_index[token_name][
                    "first_deposit_datetime"
                ].strftime("%Y-%m-%d %H:%M:%S")

                new_row["rolling_token_balance"] = token_change
                new_row["rolling_usd_balance"] = (
                    usd_change * EARLY_BIRD_BONUS
                    if apply_early_bird_bonus
                    else usd_change
                )

                # need get_usd_balance lookup here: just because it's their first transaction for this token, doesn't mean it's their first transaction overall
                new_row[
                    "all_pool_usd_balance_for_address"
                ] = get_usd_balance_for_address(address_token_index, address) + (
                    usd_change * EARLY_BIRD_BONUS
                    if apply_early_bird_bonus
                    else usd_change
                )

            elif event_type == "withdraw":
                print("WARNING: withdraw before receive")
                bad_address.append(address)
                print(address)
                continue

            elif event_type == "send":
                # freak out only if it's not the zero address
                if address != "\\x0000000000000000000000000000000000000000":
                    print("WARNING: send before receive")
                    bad_address.append(address)
                    print(address)
                continue  # in all cases continue

            else:
                raise Exception("unknown event_type: {}".format(event_type))

            address_token_index[address][token_name] = [new_row]

        # token has already been traded
        else:

            last_row = address_token_index[address][token_name][-1]

            if event_type == "deposit":

                # update cumulative token deposits
                elapsed_time = (
                    datetime.strptime(event_datetime, "%Y-%m-%d %H:%M:%S")
                    - pool_index[token_name]["first_deposit_datetime"]
                )
                if (
                    pool_index[token_name]["usd_balance"] <= EARLY_BIRD_USD_THRESHOLD
                    and elapsed_time < EARLY_BIRD_TIME_DELTA
                ):
                    apply_early_bird_bonus = True
                else:
                    apply_early_bird_bonus = False
                pool_index[token_name]["usd_balance"] += usd_change

                new_row["rolling_token_balance"] = (
                    last_row["rolling_token_balance"] + token_change
                )
                new_row["rolling_usd_balance"] = last_row["rolling_usd_balance"] + (
                    usd_change * EARLY_BIRD_BONUS
                    if apply_early_bird_bonus
                    else usd_change
                )
                new_row[
                    "all_pool_usd_balance_for_address"
                ] = get_usd_balance_for_address(address_token_index, address) + (
                    usd_change * EARLY_BIRD_BONUS
                    if apply_early_bird_bonus
                    else usd_change
                )

            elif event_type == "withdraw":

                new_row["rolling_token_balance"] = (
                    last_row["rolling_token_balance"] + token_change
                )

                # token change is explicitly negative in a withdraw
                if last_row["rolling_token_balance"] == 0:
                    print("WARNING: token balance is zero")
                    print(last_row)
                    bad_address_zero_balance.append(address)
                    usd_balance_withdrawn = 0
                else:
                    usd_balance_withdrawn = (
                        token_change / last_row["rolling_token_balance"]
                    ) * last_row["rolling_usd_balance"]
                    new_row["usd_change"] = usd_balance_withdrawn  # for tracking only

                # so we add to decrement
                new_row["rolling_usd_balance"] = (
                    last_row["rolling_usd_balance"] + usd_balance_withdrawn
                )

                new_row["all_pool_usd_balance_for_address"] = (
                    get_usd_balance_for_address(address_token_index, address)
                    + usd_balance_withdrawn
                )

            elif event_type == "send":

                # calculate the new token balance since some tokens have been sent
                new_row["rolling_token_balance"] = (
                    last_row["rolling_token_balance"] + token_change
                )

                # calculate the dollar equivalency of the sent tokens
                if last_row["rolling_token_balance"] == 0:
                    print("WARNING: token balance is zero")
                    print(last_row)
                    bad_address_zero_balance.append(address)
                    usd_balance_sent = 0
                else:
                    usd_balance_sent = (
                        token_change / last_row["rolling_token_balance"]
                    ) * last_row["rolling_usd_balance"]
                    new_row["usd_change"] = usd_balance_sent  # for tracking only

                # calculate the new usd balance
                new_row["rolling_usd_balance"] = (
                    last_row["rolling_usd_balance"] + usd_balance_sent
                )
                new_row["all_pool_usd_balance_for_address"] = (
                    get_usd_balance_for_address(address_token_index, address)
                    + usd_balance_sent
                )

                # construct a new row for the 'receive' event
                receiver_row = dict(new_row)
                receiver_row["address"] = new_row["recipient_address"]
                receiver_row["recipient_address"] = ""
                receiver_row["token_change"] = -token_change
                receiver_row["usd_change"] = -usd_balance_sent
                receiver_row["event_type"] = "receive"

                receiver_address = receiver_row["address"]

                # if we've never seen this address receive this token type
                if token_name not in address_token_index[receiver_address]:

                    # set the relevant new balances
                    receiver_row[
                        "rolling_token_balance"
                    ] = -token_change  # TODO: check this sign
                    receiver_row["rolling_usd_balance"] = -usd_balance_sent

                    receiver_row["all_pool_usd_balance_for_address"] = -usd_balance_sent

                    # add the receiver row to its index
                    address_token_index[receiver_address][token_name] = [receiver_row]

                # if we've seen this address receive this token type
                else:

                    # then we have existing knowledge of its balances, which we must update
                    last_receiver_row = address_token_index[receiver_address][
                        token_name
                    ][-1]

                    # TODO: check this sign
                    receiver_row["rolling_token_balance"] = (
                        last_receiver_row["rolling_token_balance"] - token_change
                    )
                    receiver_row["rolling_usd_balance"] = (
                        last_receiver_row["rolling_usd_balance"] - usd_balance_sent
                    )

                    receiver_row["all_pool_usd_balance_for_address"] = (
                        get_usd_balance_for_address(address_token_index, address)
                        - usd_balance_sent
                    )

                    address_token_index[receiver_address][token_name].append(
                        receiver_row
                    )

            # we always track the cumulative deposits into the pool, the first deposit datetime, and append the new row to the index
            new_row["cumulative_deposits_for_pool"] = pool_index[token_name][
                "usd_balance"
            ]
            new_row["first_deposit_for_pool_datetime"] = pool_index[token_name][
                "first_deposit_datetime"
            ].strftime("%Y-%m-%d %H:%M:%S")

            address_token_index[address][token_name].append(new_row)
    return (address_token_index, bad_address, bad_address_zero_balance)


address_token_index, bad_address, bad_address_zero_balance = build_token_index(
    lp_events
)
if len(bad_address_zero_balance) > 0:
    print("bad addresses with zero balance: \n", bad_address_zero_balance)
    pd.DataFrame(bad_address_zero_balance).to_csv(
        "bad_address_zero_balance.csv", index=False, header=False
    )

# reconstitute events from the index into a table
lp_events_usd_credit = pd.DataFrame(
    [
        row
        for address in address_token_index.keys()
        for token_name in address_token_index[address].keys()
        for row in address_token_index[address][token_name]
    ]
).sort_values(by=["datetime", "tx_index", "evt_index"], ascending=True)
pql.register(lp_events_usd_credit, "lp_events_usd_credit")

# add an extra row at the end
actions_with_today_query = """
    SELECT datetime,
           tx_hash,
           tx_index,
           evt_index,
           address,
           token_name,
           token_change,
           usd_change,
           rolling_token_balance,
           rolling_usd_balance,
           recipient_address,
           event_type,
           all_pool_usd_balance_for_address,
           cumulative_deposits_for_pool,
           first_deposit_for_pool_datetime,
           expiry_timestamp,
           expiry_datetime
      FROM lp_events_usd_credit
      WHERE datetime < expiry_datetime
      
     UNION ALL

    SELECT DATE_PARSE('{}') AS datetime,
           tx_hash,
           tx_index,
           evt_index,
           address,
           'final_row' as token_name,
           token_change,
           usd_change,
           rolling_token_balance,
           rolling_usd_balance,
           recipient_address,
           event_type,
           'final_row' as all_pool_usd_balance_for_address,
           cumulative_deposits_for_pool,
           first_deposit_for_pool_datetime,
           expiry_timestamp,
           expiry_datetime
      FROM lp_events_usd_credit
     WHERE datetime < expiry_datetime
     GROUP BY address
     """

# piecemeal queries
before_delta_query = """
    SELECT *,
           LAG(datetime, 1) OVER (PARTITION BY address ORDER BY datetime,tx_index,evt_index ASC)
           AS prior_datetime,
           CASE WHEN expiry_datetime<datetime THEN expiry_datetime ELSE datetime END as effective_date
      FROM actions_with_today
     ORDER BY datetime,tx_index,evt_index ASC, address
"""
with_delta_query = """
    SELECT *,
           (JulianDay({text}) - JulianDay(prior_datetime))*24*60*60  AS seconds_credit,
           (JulianDay({text}) - JulianDay(prior_datetime))*24*60*60 * LAG(all_pool_usd_balance_for_address, 1)
              OVER (PARTITION BY address ORDER BY datetime,tx_index,evt_index ASC)
           AS usd_seconds_credit 
      FROM before_delta
"""
accumulating_query = """
    SELECT *,
           SUM(usd_seconds_credit) OVER (PARTITION BY address ORDER BY datetime,tx_index,evt_index ASC)
           AS acc_usd_seconds_credit,
           SUM(seconds_credit) OVER (PARTITION BY address ORDER BY datetime,tx_index,evt_index ASC)
           AS acc_seconds_credit
      FROM with_delta
"""
pre_normalization_query = """
    SELECT accumulating.address,
           MAX(lp_events_usd_credit.all_pool_usd_balance_for_address) AS all_pool_usd_balance_for_address,
           MAX(acc_usd_seconds_credit) AS lp_usd_seconds,
           MAX(acc_seconds_credit) AS lp_seconds,
           MIN(accumulating.datetime) AS first_datetime,
           MAX(accumulating.datetime) AS last_datetime
      FROM accumulating
 LEFT JOIN lp_events_usd_credit on lp_events_usd_credit.address=accumulating.address
     GROUP BY 1
     --HAVING MAX(acc_usd_seconds_credit) > 0 -- if you want a minimum, set it here
     --AND  MAX(acc_seconds_credit)/24/60/60 > 0 -- if you want a minimum, set it here
"""
final_query = """
SELECT address,
       "0" || trim(address, '\\') AS pastable_address,
       all_pool_usd_balance_for_address,
       lp_usd_seconds, -- total credit: USD * seconds
       lp_usd_seconds/(SELECT SUM(lp_usd_seconds) FROM pre_normalization) AS lp_usd_seconds_share,
       lp_usd_seconds/60/60/24/30 as lp_usd_per_30_days,
       lp_seconds,
       lp_seconds/60/60/24 as lp_days,
       first_datetime
       
  FROM pre_normalization
  ORDER BY lp_usd_seconds DESC
"""

# combine into one query to assign usd credit to each event
lp_usd_seconds_per_user_query = f"""
with before_delta AS (
{before_delta_query}
),
with_delta AS ( -- every row in time
{with_delta_query}
),
accumulating AS ( -- aggregating across every row in time
{accumulating_query}
),
pre_normalization AS (
{pre_normalization_query}
)
{final_query}
"""

actions_with_today = pql.q(actions_with_today_query.format(max_datetime))
pql.register(actions_with_today, "actions_with_today")
lp_usd_seconds_per_user = pql.q(
    lp_usd_seconds_per_user_query.format(text="effective_date")
)
pql.register(lp_usd_seconds_per_user, "lp_usd_seconds_per_user")

lp_usd_seconds_per_user.lp_usd_seconds_share = (
    lp_usd_seconds_per_user.lp_usd_seconds_share
    / sum(lp_usd_seconds_per_user.lp_usd_seconds_share)
)
print(
    f"total share of lp_usd_seconds adds up to {lp_usd_seconds_per_user.lp_usd_seconds_share.sum()}"
    f" (should be 1.0 but python floats are weird)"
)

lp_usd_seconds_per_user.to_csv("lp_usd_seconds_per_user.csv", index=False)
