from pybaseball import statcast
from pybaseball import chadwick_register
from pybaseball import  playerid_lookup
from pybaseball import  statcast_pitcher

import pandas as pd
import os


players = pd.read_csv("datasources/players.csv")