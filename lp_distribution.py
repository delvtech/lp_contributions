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

# read from csv
element_liquidity = pd.read_csv("./element_liquidity.csv")
element_transfers = pd.read_csv("./element_transfers.csv")

# fix usd value
element_liquidity["price"] = (
    element_liquidity["deposit_size_base_usd"] / element_liquidity["deposit_size_base"]
)
element_liquidity["deposit_size_base_usd"] = (
    element_liquidity["lp_tokens_acquired"] * element_liquidity["price"]
)

# add to pql
pql.register(element_liquidity, "liquidity")
pql.register(element_transfers, "transfers")

# lp token names, used for yield token addresses later
lp_token_names = pd.DataFrame(
    re.findall(r"\'(.*)\'.*--(.*)", open("element_transfers.sql", "r").read()),
    columns=["token_address_raw", "token_name"],
)
lp_token_names["token_address_raw"] = lp_token_names["token_address_raw"].str.lower()
pql.register(lp_token_names, "lp_token_names")

# token mappings
contract_url = "https://raw.githubusercontent.com/element-fi/elf-deploy/main/addresses/mainnet.json"
element_token_mappings = requests.get(contract_url).json()
tokens = []
for token, data in element_token_mappings["tranches"].items():
    for tranche in data:
        tokens.append(
            {
                "expiry_timestamp": tranche["expiration"],
                "expiry_datetime": datetime.fromtimestamp(
                    tranche["expiration"]
                ).strftime("%Y-%m-%d %H:%M:%S"),
                "token_address_raw": "\\" + tranche["address"][1:].lower(),
                "token": token,
                "token_type": "pToken",
            }
        )
        for token_type in ("ptPool", "ytPool"):
            if token_type in tranche:
                tokens.append(
                    {
                        "expiry_timestamp": tranche["expiration"],
                        "expiry_datetime": datetime.fromtimestamp(
                            tranche["expiration"]
                        ).strftime("%Y-%m-%d %H:%M:%S"),
                        "token_address_raw": "\\"
                        + tranche[token_type]["address"][1:].lower(),
                        "token": token,
                        "token_type": token_type,
                        "poolId": "\\" + tranche[token_type]["poolId"][1:].lower(),
                    }
                )
tokens = pd.DataFrame(tokens)
pql.register(tokens, "token_expiry_stg")

# failed joins, which are mappings missing from the Element deployment file on Github
# so we bring them back in from `lp_token_names` (the `element_transfers.sql` file)
failed_token_name_joins = pql.q(
    """
WITH a AS (
 SELECT token_name,
        lp_token_names.token_address_raw AS token_address_raw,
        token_expiry_stg.token_address_raw AS github_token_address_raw
   FROM lp_token_names
   LEFT OUTER JOIN token_expiry_stg
     ON lp_token_names.token_address_raw = token_expiry_stg.token_address_raw
)

SELECT token_name,
       REPLACE(token_name, 'eY', 'eP') AS mirror_principal_token_name,
       token_address_raw
  FROM a
 WHERE github_token_address_raw IS NULL
 ORDER BY 1 DESC
"""
)
pql.register(failed_token_name_joins, "failed_token_name_joins")

# we lack proper addresses for the yield tokens so we join them in awkwardly here
# now we have a table that describes all Element tokens and their expiries
token_expiry = pql.q(
    """
WITH successful_token_expiry AS (

 SELECT lp_token_names.token_address_raw,
        token_name,
        expiry_timestamp,
        expiry_datetime
   FROM lp_token_names
  INNER JOIN token_expiry_stg
     ON lp_token_names.token_address_raw = token_expiry_stg.token_address_raw

),

failed_joins_repaired AS (

 SELECT failed_token_name_joins.token_address_raw,
        failed_token_name_joins.token_name,
        expiry_timestamp,
        expiry_datetime
   FROM failed_token_name_joins
  INNER JOIN successful_token_expiry
     ON failed_token_name_joins.mirror_principal_token_name = successful_token_expiry.token_name

)

SELECT *
  FROM successful_token_expiry
  
 UNION ALL
 
SELECT *
  FROM failed_joins_repaired
  
 ORDER BY expiry_datetime DESC
"""
)
pql.register(token_expiry, "token_expiry")

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

whereIn = "('" + pd.Series([i for i in listOfDoubleAddresses]).str.cat(sep="','") + "')"
sql = """
select namespace, name, abi::varchar, address::varchar, code::varchar, base, dynamic, updated_at, created_at, id, factory from ethereum."contracts" ec
where ec.address IN {}
""".format(
    whereIn
)
ethereum_contracts = run_query(sql)

for n, row in ethereum_contracts.iterrows():
    idxAffectedRows = lp_events.liquidityProvider == row.address
    r = lp_events.loc[idxAffectedRows, "address"]
    lp_events.loc[idxAffectedRows, "address"] = lp_events.loc[
        idxAffectedRows, "liquidityProvider"
    ]

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

    EARLY_BIRD_BONUS = 1.1  # 10%
    EARLY_BIRD_USD_THRESHOLD = 2000000  # 2 million
    EARLY_BIRD_TIME_DELTA = timedelta(days=10)  # 10 days

    # for storing full rows for a given address, token pair
    address_token_index = defaultdict(dict)
    # for storing the cumulative deposits (and not withdrawals) to a pool
    pool_index = {}

    # iterate according to:
    # ORDER BY datetime ASC, tx_index ASC, evt_index ASC

    # for storing full rows for a given address, token pair
    address_token_index = defaultdict(dict)
    # for storing the cumulative deposits (and not withdrawals) to a pool
    pool_index = {}
    # for storing bad addresses to be investigated later (withdraw before deposit)
    bad_address = []

    # iterate according to:
    # ORDER BY datetime ASC, tx_index ASC, evt_index ASC
    for _, row in lp_events.iterrows():
        new_row = dict(row)

        address = new_row["address"]
        event_type = new_row["event_type"]
        token_name = new_row["token_name"]
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
                print("WARNING: send before receive")
                bad_address.append(address)
                print(address)
                continue

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
    return (address_token_index, bad_address)


address_token_index, bad_address = build_token_index(lp_events)
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
     HAVING MAX(acc_usd_seconds_credit) > 500*90*60*60*24 
     --AND  MAX(acc_seconds_credit)/24/60/60 > 7
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

lp_usd_seconds_per_user.to_csv("lp_usd_seconds_per_user.csv", index=False)
