from pybaseball import statcast
from pybaseball import cache
import pandas as pd
import os

cache.enable()
cache.config.cache_type = "parquet"
cache.config.save()

#players = pd.read_csv("datasources/players.csv")
years = range(2007, 2024)
chunks = [('01-01', '05-18'), ('05-18', '06-18'), ('06-18', '07-18'), ('07-18', '08-18'), ('08-18', '12-31')]
for year in years:
    for first, second in chunks:
        file_name = f"{year}-{first}:{second}"
        print(f"Collecting data for the ranges: {file_name}")
        first_date = f"{year}-{first}"
        second_date = f"{year}-{second}"
        file_path = os.path.join("pbp_data", f"{file_name}.parquet")
        if not os.path.exists(file_path):
            data = statcast(first_date, second_date)
            data.to_parquet(file_path, index=False)
        else:
            print("File already exists.\n")