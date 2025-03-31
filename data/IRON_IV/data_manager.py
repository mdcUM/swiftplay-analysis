import pandas as pd
from typing import Dict, List, Set
from pathlib import Path
import json

class DataManager:
    def __init__(self):
        self.swiftplay_player_match_file = "SwiftplayPlayerMatchData.csv"
        self.ranked_player_match_file = "RankedPlayerMatchData.csv"
        self.swiftplay_matches_file = "SwiftplayMatches.csv"
        self.ranked_matches_file = "RankedMatches.csv"
        self.players_file = "Players.csv"
        self.queue_state_file = "queue_state.json"
        self.match_id_cache = set()
        
        self._initialize_files()
        
    def _initialize_files(self):
        """Initialize CSV files if they don't exist"""
        file_columns = {
            self.swiftplay_player_match_file: [
                "matchId", "puuid", "teamId", "summonerLevel", "role", "teamPosition", "kills", 
                "deaths", "assists", "championId", "win", "teamEarlySurrendered",
                "totalTimeSpentDead", "timePlayed", "longestTimeSpentLiving",
                "tier", "rank", "championLevel", "championPoints"
            ],
            self.ranked_player_match_file: [
                "matchId", "puuid", "teamId", "summonerLevel", "role", "teamPosition", "kills", 
                "deaths", "assists", "championId", "win", "teamEarlySurrendered",
                "totalTimeSpentDead", "timePlayed", "longestTimeSpentLiving",
                "tier", "rank", "championLevel", "championPoints"
            ],
            self.swiftplay_matches_file: [
                "matchId", "gameDuration", "endOfGameResult", 
                "gameEndedInSurrender", "gameEndedInEarlySurrender"
            ],
            self.ranked_matches_file: [
                "matchId", "gameDuration", "endOfGameResult", 
                "gameEndedInSurrender", "gameEndedInEarlySurrender"
            ],
            self.players_file: [
                "puuid", "summonerLevel", "tier", "rank",
                "swiftplay_kills", "swiftplay_deaths", "swiftplay_assists",
                "swiftplay_kd", "swiftplay_ad", "swiftplay_kda", "swiftplay_kad",
                "swiftplay_win_loss_ratio", "ranked_kills", "ranked_deaths",
                "ranked_assists", "ranked_kd", "ranked_ad", "ranked_kda",
                "ranked_kad", "ranked_win_loss_ratio"
            ]
        }
        
        for file, columns in file_columns.items():
            if not Path(file).exists():
                pd.DataFrame(columns=columns).to_csv(file, index=False)
                
        # Load existing match IDs into cache
        for file in [self.swiftplay_matches_file, self.ranked_matches_file]:
            if Path(file).exists():
                df = pd.read_csv(file)
                self.match_id_cache.update(df["matchId"].values)
        
    def player_exists(self, puuid: str) -> bool:
        """Check if player exists in Players.csv"""
        if not Path(self.players_file).exists():
            return False
        df = pd.read_csv(self.players_file)
        return puuid in df["puuid"].values
        
    def match_exists(self, match_id: str, queue_type: str) -> bool:
        """Check if match exists in appropriate matches file with caching"""
        if match_id in self.match_id_cache:
            return True
            
        file = self.swiftplay_matches_file if queue_type == "swiftplay" else self.ranked_matches_file
        if not Path(file).exists():
            return False
            
        df = pd.read_csv(file)
        exists = match_id in df["matchId"].values
        if exists:
            self.match_id_cache.add(match_id)
        return exists
        
    def save_player(self, player_data: Dict):
        """Save player data to Players.csv"""
        df = pd.DataFrame([player_data])
        df.to_csv(self.players_file, mode='a', header=not Path(self.players_file).exists(), index=False)
        
    def save_match(self, match_data: Dict, queue_type: str):
        """Save match data to appropriate matches file"""
        file = self.swiftplay_matches_file if queue_type == "swiftplay" else self.ranked_matches_file
        df = pd.DataFrame([match_data])
        df.to_csv(file, mode='a', header=not Path(file).exists(), index=False)
        
    def save_player_matches(self, player_matches: List[Dict], queue_type: str):
        """Save player match data to appropriate player match file, skipping existing entries"""
        file = self.swiftplay_player_match_file if queue_type == "swiftplay" else self.ranked_player_match_file
        
        # Read existing data
        existing_df = pd.read_csv(file)
        
        # Filter out matches that already exist for each player
        new_matches = []
        for match in player_matches:
            exists = len(existing_df[
                (existing_df["matchId"] == match["matchId"]) & 
                (existing_df["puuid"] == match["puuid"])
            ]) > 0
            
            if not exists:
                new_matches.append(match)
        
        # Only save if we have new matches
        if new_matches:
            new_df = pd.DataFrame(new_matches)
            new_df.to_csv(file, mode='a', header=False, index=False)
        
    def save_queue_state(self, queue: List[str]):
        """Save current queue state to file"""
        with open(self.queue_state_file, 'w') as f:
            json.dump(queue, f)
            
    def load_queue_state(self) -> List[str]:
        """Load queue state from file"""
        if not Path(self.queue_state_file).exists():
            return []
        with open(self.queue_state_file, 'r') as f:
            return json.load(f)

    def players_file_exists(self) -> bool:
        """Check if Players.csv exists"""
        return Path(self.players_file).exists()

    def get_all_player_puuids(self) -> Set[str]:
        """Get set of all PUUIDs from Players.csv"""
        if not self.players_file_exists():
            return set()
        df = pd.read_csv(self.players_file)
        return set(df["puuid"].values)

    def get_player_match_data(self, match_id: str, puuid: str, queue_type: str) -> Dict | None:
        """Get player match data for a specific match and player"""
        file = self.swiftplay_player_match_file if queue_type == "swiftplay" else self.ranked_player_match_file
        if not Path(file).exists():
            return None
        
        df = pd.read_csv(file)
        match_data = df[(df["matchId"] == match_id) & (df["puuid"] == puuid)]
        
        if len(match_data) == 0:
            return None
        
        return match_data.iloc[0].to_dict() 