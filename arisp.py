"""
This script establishes a new metric for baseball: Advance Runner into Scoring Position (ARiSP).
This identifies which batter proportionally advances runners into scoring position. The scoring
is as follows:
- If a runner on first advances to second, that is 1 point
- If a runner on second advances to third, that is 2 points

The points will be proportioned against the number of opportunities the batter has had in 
scenarios that could advance runners. For example, suppose a batter had roughly 100 opportunities
to advance runners into scoring position. Of those 100 opportunities, he advanced a runner from
first to second 30 times and advanced a runner from second to third 30 times. Thus, the batter
has an ARiSP score of (30*1 + 30*2)/100 = 0.900.
"""

import os
import pyarrow.parquet as pq
from pybaseball import playerid_reverse_lookup
from pybaseball import playerid_lookup
import pandas as pd
from datamodels import ARiSPplayer


class ARiSP:
    def __init__(self, year: int, player: int | str):
        """Initializing of properties.

        Args:
            year (int): The year the stats would like to be pull from
            player (int | str): Either the mlbam id or name of the player. 
            Ex: "Jared Walsh" or 665120
        """
        self.year = self._check_year(year) 
        self.player_id, self.player_name = self._check_player(player)

    def __str__(self):
        return f"{self.year}: {self.player_name} ({self.player_id})"

    def _check_year(self, year: int) -> int:
        assert 2007 <= year <= 2023, "Year must be between 2007 and 2023 inclusive."
        return year
    
    def _check_player_against_year(self, player_frame: pd.DataFrame) -> pd.DataFrame:
        if self.year < player_frame.mlb_played_first[0]:
            raise AssertionError("The given year was before player started career. Please input another year.")
        elif self.year > player_frame.mlb_played_last[0]:
            raise AssertionError("The given year was after player ended career. Please input another year.")
        return player_frame

    def _check_player(self, player: int | str) -> tuple[int, str]:

        if isinstance(player, int):
            player_frame = playerid_reverse_lookup([player])
            if len(player_frame) == 0:
                raise AssertionError("The id for the player is wrong. Please check the number again.")
            player_frame = self._check_player_against_year(player_frame)
            player_name = f"{player_frame.name_first[0].capitalize()} {player_frame.name_last[0].capitalize()}"
            return player, player_name
        
        elif isinstance(player, str):
            first_name, *last_name = list(map(str.capitalize, player.split(' ')))
            last_name = ' '.join(last_name)
            player_frame = playerid_lookup(last_name, first_name)
            if len(player_frame) == 0:
                raise AssertionError("The name for the player is wrong. Please check the spelling again.")
            elif len(player_frame) > 1:
                raise AssertionError("There are multiple players with that name. Please input the mlbam key for greater specificity.")
            player_frame = self._check_player_against_year(player_frame)
            player_id = player_frame.key_mlbam[0]
            player_name = f"{first_name} {last_name}"

            return player_id, player_name
        
        raise AssertionError("Please input the players name or mlbam id.")
    
    def _filter_data(self, descriptions: list[list[str]]) -> list[str]:
        total_opportunities = len(descriptions)

        advance_second, advance_third = 0, 0

        nonevents = []
        for d in descriptions:
            nonevents += list(map(str.strip, d.split("   ")[1:])) 

        # remove empty lists
        filter_cond = lambda x: x and not x.lower().startswith(self.player_name.lower())
        nonevents = list(filter(filter_cond, nonevents))
        for ne in nonevents:
            if "to 3rd" in ne:
                advance_third += 1
            elif "to 2nd" in ne:
                advance_second += 1
                        
        return total_opportunities, (advance_second, advance_third)
    
    def _read_data(self):
        data_path = os.path.join("pbp_data_by_year", f"{self.year}.parquet")
        pbp_data = pq.read_table(data_path, columns=["batter", "events","description", "on_2b", "on_1b", "des"]).to_pandas()
        player_frame = pbp_data[pbp_data['batter'] == self.player_id]   \
            .dropna(subset = ["on_2b", "on_1b"], how = 'all')           \
            .dropna(subset = ["events"], how = 'any')
        
        return player_frame['des'].to_list()
    
    @property
    def profile(self):
        total_opportunities, (advance_second, advance_third) = self._filter_data(self._read_data())
        score = (advance_second + 2*advance_third)/ total_opportunities
        return ARiSPplayer(
            player_id = self.player_id,
            player_name=self.player_name,
            season_year=self.year,
            total_opportunities=total_opportunities,
            num_advance_to_2b=advance_second,
            num_advance_to_3b=advance_third,
            ARiSP_score=score
        )
    

print(ARiSP(2023, "Ke'Bryan Hayes").profile)