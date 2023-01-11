## setup

set up your preferred virtual environment, or just do this:

```
python -m venv .venv
source ./.venv/bin/activate
```

install python dependencies with `pip install -r requirements.txt`

## running

if you only need data up to January 5th, use the included csv's

if you want specific date cutoffs, adjust these lines at the top of `lp_contribution.py`:

```
CUTOFF_STARTING_DATE = "2021-01-01"
CUTOFF_ENDING_DATE = "2022-12-31"
```

run `python lp_contribution.py`

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
