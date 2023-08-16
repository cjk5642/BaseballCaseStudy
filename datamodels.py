from dataclasses import dataclass

@dataclass
class ARiSPplayer:
    player_id: int
    player_name: str
    season_year: int
    total_opportunities: int
    num_advance_to_2b: int
    num_advance_to_3b: int
    ARiSP_score: float

    