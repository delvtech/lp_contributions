## setup

set up your preferred virtual environment, or just do this:

```
python -m venv .venv
source ./.venv/bin/activate
```

install python dependencies with `pip install -r requirements.txt`

## running

if you only need data up to January 5th, use the included csv's

run `python lp_distribution.py`

once that's done, results are stored in `lp_usd_seconds_per_user.csv`, inspect them with something like:

```
results = pd.read_csv('lp_usd_seconds_per_user.csv')
display(results.head(10))
display(f"total share adds up to {results['lp_usd_seconds_share'].sum()}")
```

## updating data

if you need to update data

copy `.env.example` to `.env` and enter your details for the dune API

run `dune_queries.py` and pray :pray:

(new pool deployments require updates to these queries)

or give us a shout
