import time
import requests
import logging
from typing import List, Dict
from collections import deque
from config import RIOT_API_KEY

class RateLimit:
    def __init__(self, requests_per_second: int, requests_per_2min: int):
        self.requests_per_second = requests_per_second
        self.requests_per_2min = requests_per_2min
        self.request_times_short = deque()  # for per-second tracking
        self.request_times_long = deque()   # for 2-minute tracking
        
    def wait_if_needed(self):
        current_time = time.time()
        
        # Remove old requests from 1-second window
        while self.request_times_short and current_time - self.request_times_short[0] > 1:
            self.request_times_short.popleft()
            
        # Remove old requests from 2-minute window
        while self.request_times_long and current_time - self.request_times_long[0] > 120:
            self.request_times_long.popleft()
            
        # Check if we need to wait for 1-second limit
        if len(self.request_times_short) >= self.requests_per_second:
            wait_time = 1 - (current_time - self.request_times_short[0])
            if wait_time > 0:
                time.sleep(wait_time)
                
        # Check if we need to wait for 2-minute limit
        if len(self.request_times_long) >= self.requests_per_2min:
            wait_time = 120 - (current_time - self.request_times_long[0])
            if wait_time > 0:
                logging.info(f"Rate limit reached. Sleeping for {wait_time:.2f} seconds")
                time.sleep(wait_time)
                
        # Add current request to both windows
        current_time = time.time()  # Get new time after any sleeps
        self.request_times_short.append(current_time)
        self.request_times_long.append(current_time)

class RiotAPIClient:
    def __init__(self):
        self.api_key = RIOT_API_KEY
        self.rate_limit = RateLimit(20, 100)
        # Add caches for rank and champion mastery
        self.rank_cache = {}  # puuid -> rank info
        self.champion_mastery_cache = {}  # (puuid, championId) -> mastery info
        
    def _make_request(self, url: str, allow_404: bool = False) -> Dict | None:
        """
        Make API request with retry logic and rate limiting
        Args:
            url: API endpoint URL
            allow_404: If True, return None on 404 instead of retrying/raising
        """
        self.rate_limit.wait_if_needed()
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                response = requests.get(url, headers={"X-Riot-Token": self.api_key})
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 404 and allow_404:
                    return None
                elif response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logging.warning(f"Rate limit exceeded. Waiting {retry_after} seconds")
                    time.sleep(retry_after)
                else:
                    logging.error(f"Request failed with status {response.status_code}")
                    
            except Exception as e:
                logging.error(f"Request error: {str(e)}")
                
            retry_count += 1
            time.sleep(2 ** retry_count)  # Exponential backoff
            
        raise Exception(f"Failed to get response after {max_retries} retries")
    
    def get_league_entries(self, tier: str, division: str, page: int) -> List[Dict]:
        url = f"https://na1.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/{tier}/{division}?page={page}"
        return self._make_request(url)
    
    def get_match_ids(self, puuid: str, queue_type: str) -> List[str]:
        base_url = f"https://americas.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
        params = "count=100"
        
        if queue_type == "swiftplay":
            params += "&queue=480"
        elif queue_type == "ranked":
            params += "&type=ranked"
            
        url = f"{base_url}?{params}"
        return self._make_request(url)
    
    def get_match_details(self, match_id: str) -> Dict:
        url = f"https://americas.api.riotgames.com/lol/match/v5/matches/{match_id}"
        return self._make_request(url)
    
    def get_player_rank(self, puuid: str) -> Dict:
        """Get player rank with caching"""
        if puuid not in self.rank_cache:
            url = f"https://na1.api.riotgames.com/lol/league/v4/entries/by-puuid/{puuid}"
            self.rank_cache[puuid] = self._make_request(url)
        return self.rank_cache[puuid]
    
    def get_champion_mastery(self, puuid: str, champion_id: int) -> Dict | None:
        """Get champion mastery with caching"""
        cache_key = (puuid, champion_id)
        if cache_key not in self.champion_mastery_cache:
            url = f"https://na1.api.riotgames.com/lol/champion-mastery/v4/champion-masteries/by-puuid/{puuid}/by-champion/{champion_id}"
            self.champion_mastery_cache[cache_key] = self._make_request(url, allow_404=True)
        return self.champion_mastery_cache[cache_key] 