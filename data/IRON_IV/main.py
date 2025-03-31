import logging
from typing import Set, List, Dict
from collections import deque
import random
import argparse

from api_client import RiotAPIClient
from data_manager import DataManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class LeagueBFS:
    def __init__(self):
        self.api_client = RiotAPIClient()
        self.data_manager = DataManager()
        self.visited_puuids: Set[str] = set()
        self.queue = deque()
        
    def start_from_division(self, tier: str, division: str):
        """Start BFS from a specific division/tier combination"""
        logging.info(f"Sampling from {tier} {division}")
        self._sample_from_division(tier, division)
    
    def resume_from_queue(self):
        """Resume BFS from saved queue state"""
        saved_queue = self.data_manager.load_queue_state()
        if not saved_queue:
            logging.error("No saved queue state found")
            return
            
        logging.info(f"Resuming BFS with {len(saved_queue)} players in queue")
        self.queue = deque(saved_queue)
        
        # Load visited PUUIDs from Players.csv to avoid reprocessing
        if self.data_manager.players_file_exists():
            self.visited_puuids = self.data_manager.get_all_player_puuids()
        
        # Start BFS from current queue
        self.bfs(None)  # Pass None since we're not starting with a new PUUID

    def start_stratified_sampling(self):
        """Performs stratified random sampling across divisions and tiers"""
        tiers = ["DIAMOND", "EMERALD", "PLATINUM", "GOLD", "SILVER", "BRONZE", "IRON"]
        divisions = ["I", "II", "III", "IV"]
        
        for tier in tiers:
            for division in divisions:
                logging.info(f"Sampling from {tier} {division}")
                self._sample_from_division(tier, division)
    
    def _sample_from_division(self, tier: str, division: str):
        """Samples players from a specific division and initiates BFS if valid candidate found"""
        attempts = 0
        while attempts < 5:  # Try up to 5 different pages
            page = random.randint(1, 17)
            logging.info(f"Trying page {page} for {tier} {division}")
            
            entries = self.api_client.get_league_entries(tier, division, page)
            
            for entry in entries:
                puuid = entry["puuid"]
                
                if puuid in self.visited_puuids:
                    continue
                    
                # Check if player has both Swiftplay and Ranked games
                swiftplay_matches = self.api_client.get_match_ids(puuid, queue_type="swiftplay")
                ranked_matches = self.api_client.get_match_ids(puuid, queue_type="ranked")
                
                if swiftplay_matches and ranked_matches:
                    logging.info(f"Found valid candidate in {tier} {division}. Starting BFS...")
                    self.bfs(puuid)
                    return
                    
            attempts += 1
            
    def bfs(self, start_puuid: str | None):
        """Performs BFS starting from given PUUID or continues from existing queue"""
        if start_puuid is not None:
            # Only clear and initialize queue if starting new BFS
            self.queue.clear()
            self.queue.append(start_puuid)
            self.visited_puuids.add(start_puuid)
        
        iterations = 0
        add_to_queue = True
        
        try:
            while self.queue:
                iterations += 1
                logging.info(f"BFS Iteration {iterations}, Queue size: {len(self.queue)}")
                
                current_puuid = self.queue.popleft()
                # Save queue state before processing each player
                self.data_manager.save_queue_state(list(self.queue))
                
                if len(self.queue) > 1000:
                    add_to_queue = False
                
                try:
                    self._process_player(current_puuid, add_to_queue)
                except Exception as e:
                    logging.error(f"Error processing player {current_puuid}: {str(e)}")
                    # Put the failed PUUID back in queue
                    self.queue.appendleft(current_puuid)
                    self.data_manager.save_queue_state(list(self.queue))
                    raise
                
        except KeyboardInterrupt:
            logging.info("Received keyboard interrupt, saving state before exit...")
            # Put the current PUUID back in queue if we were interrupted
            if current_puuid:
                self.queue.appendleft(current_puuid)
            self.data_manager.save_queue_state(list(self.queue))
            raise
    
    def _process_player(self, puuid: str, add_to_queue: bool):
        """Process a single player's data"""
        if self.data_manager.player_exists(puuid):
            logging.info(f"Player {puuid} already processed")
            return
            
        # Collect Swiftplay matches
        swiftplay_matches = self.api_client.get_match_ids(puuid, queue_type="swiftplay")
        swiftplay_data = self._process_matches(puuid, swiftplay_matches, "swiftplay", add_to_queue)
        
        # Collect Ranked matches
        ranked_matches = self.api_client.get_match_ids(puuid, queue_type="ranked")
        ranked_data = self._process_matches(puuid, ranked_matches, "ranked", False)
        
        # Aggregate player data
        player_data = self._aggregate_player_data(puuid, swiftplay_data, ranked_data)
        self.data_manager.save_player(player_data)

    def _process_matches(self, puuid: str, match_ids: List[str], queue_type: str, add_to_queue: bool) -> List[Dict]:
        """Process matches for a given queue type"""
        player_match_data = []
        
        for match_id in match_ids:
            try:
                if self.data_manager.match_exists(match_id, queue_type):
                    # Get player match data from existing records
                    existing_match_data = self.data_manager.get_player_match_data(match_id, puuid, queue_type)
                    if existing_match_data is not None:
                        player_match_data.append(existing_match_data)
                    continue
                    
                match_details = self.api_client.get_match_details(match_id)
                
                # Process player match data first
                player_matches = self._extract_player_matches(match_details, match_id)
                self.data_manager.save_player_matches(player_matches, queue_type)
                
                # Only save match info if player data was saved successfully
                match_info = self._extract_match_info(match_details)
                self.data_manager.save_match(match_info, queue_type)
                
                # Find and append this player's match data
                player_match = next(pm for pm in player_matches if pm["puuid"] == puuid)
                player_match_data.append(player_match)
                
                # Add teammates to queue if needed
                if add_to_queue and queue_type == "swiftplay":
                    self._add_teammates_to_queue(match_details)
                
            except Exception as e:
                logging.error(f"Error processing match {match_id}: {str(e)}")
                raise
            
        return player_match_data

    def _add_teammates_to_queue(self, match_details: Dict):
        """Add teammates from a match to the BFS queue"""
        for participant_puuid in match_details["metadata"]["participants"]:
            if participant_puuid not in self.visited_puuids:
                self.queue.append(participant_puuid)
                self.visited_puuids.add(participant_puuid)

    def _extract_match_info(self, match_details: Dict) -> Dict:
        """Extract relevant match information from match details"""
        info = match_details["info"]
        participants = info["participants"]
        
        # Get surrender info from any participant (should be same for all)
        surrender_info = participants[0]
        
        return {
            "matchId": match_details["metadata"]["matchId"],
            "gameDuration": info["gameDuration"],
            "endOfGameResult": info.get("endOfGameResult", ""),  # May not always be present
            "gameEndedInSurrender": surrender_info["gameEndedInSurrender"],
            "gameEndedInEarlySurrender": surrender_info["gameEndedInEarlySurrender"]
        }

    def _extract_player_matches(self, match_details: Dict, match_id: str) -> List[Dict]:
        """Extract player match data for all participants in a match"""
        player_matches = []
        
        for participant in match_details["info"]["participants"]:
            puuid = participant["puuid"]
            
            # Get player rank info
            rank_info = self.api_client.get_player_rank(puuid)
            tier = "UNRANKED"
            rank = "UNRANKED"
            if rank_info:  # Player might be unranked
                # Get the RANKED_SOLO_5x5 entry if it exists
                solo_rank = next((entry for entry in rank_info if entry["queueType"] == "RANKED_SOLO_5x5"), None)
                if solo_rank:
                    tier = solo_rank["tier"]
                    rank = solo_rank["rank"]
            
            # Get champion mastery
            champion_mastery = self.api_client.get_champion_mastery(puuid, participant["championId"])
            champion_level = champion_mastery.get("championLevel", 0) if champion_mastery else 0
            champion_points = champion_mastery.get("championPoints", 0) if champion_mastery else 0
            
            player_match = {
                "matchId": match_id,
                "puuid": puuid,
                "teamId": participant["teamId"],
                "summonerLevel": participant["summonerLevel"],
                "role": participant["role"],
                "teamPosition": participant["teamPosition"],
                "kills": participant["kills"],
                "deaths": participant["deaths"],
                "assists": participant["assists"],
                "championId": participant["championId"],
                "win": participant["win"],
                "teamEarlySurrendered": participant["teamEarlySurrendered"],
                "totalTimeSpentDead": participant["totalTimeSpentDead"],
                "timePlayed": participant["timePlayed"],
                "longestTimeSpentLiving": participant["longestTimeSpentLiving"],
                "tier": tier,
                "rank": rank,
                "championLevel": champion_level,
                "championPoints": champion_points
            }
            
            player_matches.append(player_match)
        
        return player_matches

    def _aggregate_player_data(self, puuid: str, swiftplay_data: List[Dict], ranked_data: List[Dict]) -> Dict:
        """Aggregate player statistics from their match data"""
        # Initialize counters for Swiftplay
        swiftplay_stats = {
            "kills": 0,
            "deaths": 0,
            "assists": 0,
            "wins": 0,
            "total_games": len(swiftplay_data)
        }
        
        # Get the most recent rank info (from first match)
        tier = "UNRANKED"
        rank = "UNRANKED"
        summoner_level = 0
        
        # Aggregate Swiftplay stats
        for match in swiftplay_data:
            swiftplay_stats["kills"] += match["kills"]
            swiftplay_stats["deaths"] += match["deaths"]
            swiftplay_stats["assists"] += match["assists"]
            swiftplay_stats["wins"] += 1 if match["win"] else 0
            
            # Get rank info from most recent match
            if tier == "UNRANKED":
                tier = match["tier"]
                rank = match["rank"]
                summoner_level = match["summonerLevel"]
        
        # Calculate Swiftplay ratios (use 1 for deaths if 0 to avoid division by zero)
        deaths_for_ratio = max(1, swiftplay_stats["deaths"])
        swiftplay_kd = swiftplay_stats["kills"] / deaths_for_ratio
        swiftplay_ad = swiftplay_stats["assists"] / deaths_for_ratio
        swiftplay_kda = (swiftplay_stats["kills"] + (swiftplay_stats["assists"] / 2)) / deaths_for_ratio
        swiftplay_kad = (swiftplay_stats["kills"] + swiftplay_stats["assists"]) / deaths_for_ratio
        swiftplay_wl = swiftplay_stats["wins"] / max(1, (swiftplay_stats["total_games"] - swiftplay_stats["wins"]))
        
        # Initialize ranked stats
        ranked_stats = None
        if ranked_data:
            ranked_stats = {
                "kills": 0,
                "deaths": 0,
                "assists": 0,
                "wins": 0,
                "total_games": len(ranked_data)
            }
            
            # Aggregate Ranked stats
            for match in ranked_data:
                ranked_stats["kills"] += match["kills"]
                ranked_stats["deaths"] += match["deaths"]
                ranked_stats["assists"] += match["assists"]
                ranked_stats["wins"] += 1 if match["win"] else 0
        
        # Create player data dictionary
        player_data = {
            "puuid": puuid,
            "summonerLevel": summoner_level,
            "tier": tier,
            "rank": rank,
            "swiftplay_kills": swiftplay_stats["kills"],
            "swiftplay_deaths": swiftplay_stats["deaths"],
            "swiftplay_assists": swiftplay_stats["assists"],
            "swiftplay_kd": swiftplay_kd,
            "swiftplay_ad": swiftplay_ad,
            "swiftplay_kda": swiftplay_kda,
            "swiftplay_kad": swiftplay_kad,
            "swiftplay_win_loss_ratio": swiftplay_wl
        }
        
        # Add ranked stats if they exist
        if ranked_stats:
            deaths_for_ratio = max(1, ranked_stats["deaths"])
            player_data.update({
                "ranked_kills": ranked_stats["kills"],
                "ranked_deaths": ranked_stats["deaths"],
                "ranked_assists": ranked_stats["assists"],
                "ranked_kd": ranked_stats["kills"] / deaths_for_ratio,
                "ranked_ad": ranked_stats["assists"] / deaths_for_ratio,
                "ranked_kda": (ranked_stats["kills"] + (ranked_stats["assists"] / 2)) / deaths_for_ratio,
                "ranked_kad": (ranked_stats["kills"] + ranked_stats["assists"]) / deaths_for_ratio,
                "ranked_win_loss_ratio": ranked_stats["wins"] / max(1, (ranked_stats["total_games"] - ranked_stats["wins"]))
            })
        else:
            # If no ranked data, set all ranked stats to None
            player_data.update({
                "ranked_kills": None,
                "ranked_deaths": None,
                "ranked_assists": None,
                "ranked_kd": None,
                "ranked_ad": None,
                "ranked_kda": None,
                "ranked_kad": None,
                "ranked_win_loss_ratio": None
            })
        
        return player_data

def main():
    parser = argparse.ArgumentParser(description='League of Legends Data Collection')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--resume', action='store_true', 
                      help='Resume BFS from saved queue state')
    group.add_argument('--tier', type=str, choices=['DIAMOND', 'EMERALD', 'PLATINUM', 'GOLD', 'SILVER', 'BRONZE', 'IRON'],
                      help='Tier to sample from')
    parser.add_argument('--division', type=str, choices=['I', 'II', 'III', 'IV'],
                      help='Division to sample from (required if --tier is specified)')
    
    args = parser.parse_args()
    
    bfs = LeagueBFS()
    try:
        if args.resume:
            bfs.resume_from_queue()
        else:
            if not args.division:
                parser.error("--division is required when --tier is specified")
            bfs.start_from_division(args.tier, args.division)
    except KeyboardInterrupt:
        logging.info("Program terminated by user")
    except Exception as e:
        logging.error(f"Program terminated with error: {str(e)}")

if __name__ == "__main__":
    main() 