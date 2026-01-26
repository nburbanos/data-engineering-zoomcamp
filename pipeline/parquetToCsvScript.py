import pandas as pd
import sys

# Read the parquet file specified as a command line argument
df = pd.read_parquet(sys.argv[1])

# Print the data in CSV format to standard output
print(df.to_csv(index=False))
