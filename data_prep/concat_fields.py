from pathlib import Path
import pandas as pd

# list files with LPS in the name
p = Path(".")
files = p.glob("*LPS*")
print(files)

# read the files into a list
dfs = [pd.read_csv(f) for f in files]

# concatenate the list of dataframes
df = pd.concat(dfs)
print(df.head(10))
print(df.shape)

df.to_csv("element_liquidity_concatenated.csv", index=False)
