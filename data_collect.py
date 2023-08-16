from pybaseball import statcast
from pybaseball import cache
from datetime import datetime
import pandas as pd
import os

cache.enable()
cache.config.cache_type = "parquet"
cache.config.save()

def collect_pitch_by_pitch(dir:str = "pbp_data"):
    years = range(2007, datetime.now().year+1)
    chunks = [('01-01', '05-18'), ('05-18', '06-18'), ('06-18', '07-18'), ('07-18', '08-18'), ('08-18', '12-31')]
    for year in years:
        for first, second in chunks:
            file_name = f"{year}-{first}:{second}"
            first_date = f"{year}-{first}"
            second_date = f"{year}-{second}"
            file_path = os.path.join(dir, f"{file_name}.parquet")
            if not os.path.exists(file_path):
                print(f"Collecting data for the ranges: {file_name}")
                data = statcast(first_date, second_date)
                data.to_parquet(file_path, index=False)
    return None

def congregate_files_by_year(dir:str = 'pbp_data', out_dir:str = "pbp_data_by_year"):
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)

    pbp_file_paths = os.listdir(dir)
    
    # get file years
    file_years = sorted(set([p.split("-")[0] for p in pbp_file_paths]))
    for f in file_years:
        frame = pd.concat([pd.read_parquet(os.path.join(dir, p)) for p in pbp_file_paths if p.startswith(f)], axis = 0, ignore_index = True)
        print(frame)
        break

congregate_files_by_year()