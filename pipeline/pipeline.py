import sys
import pandas as pd
#Arguments get printed when you executed python command to run script
print('arguments', sys.argv)

month = int(sys.argv[1])

#Create data frame
df = pd.DataFrame({"day": [1, 2], "num_passengers": [3, 4]})

#Adding another column
df['month'] = month
print(df.head()) #To show what's in the data frame

df.to_parquet(f"output_{month}.parquet")

print(f'Hello pipeline, month={month}')