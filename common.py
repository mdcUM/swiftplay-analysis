import pandas as pd

# Define the custom order
rank_order = ['UNRANKED', 
                'IRON IV', 'IRON III', 'IRON II', 'IRON I',
                'BRONZE IV', 'BRONZE III', 'BRONZE II', 'BRONZE I',
                'SILVER IV', 'SILVER III', 'SILVER II', 'SILVER I',
                'GOLD IV', 'GOLD III', 'GOLD II', 'GOLD I',
                'PLATINUM IV', 'PLATINUM III', 'PLATINUM II', 'PLATINUM I',
                'EMERALD IV', 'EMERALD III', 'EMERALD II', 'EMERALD I',
                'DIAMOND IV', 'DIAMOND III', 'DIAMOND II', 'DIAMOND I',
                'MASTER I', 'GRANDMASTER I', 'CHALLENGER I']

# Create a CategoricalDtype with the specified order
rank_type = pd.api.types.CategoricalDtype(categories=rank_order, ordered=True)

def get_all_data():
    players = pd.read_csv('./data/Players.csv')
    ranked_matches = pd.read_csv('./data/RankedMatches.csv')
    ranked_player_match_data = pd.read_csv('./data/RankedPlayerMatchData.csv')
    swiftplay_matches = pd.read_csv('./data/SwiftplayMatches.csv')
    swiftplay_player_match_data = pd.read_csv('./data/SwiftplayPlayerMatchData.csv')

    return preprocess(players, ranked_matches, ranked_player_match_data, swiftplay_matches, swiftplay_player_match_data)


def preprocess(players, ranked_matches, ranked_player_match_data, swiftplay_matches, swiftplay_player_match_data):
    swiftplay_player_match_data["full_rank"] = swiftplay_player_match_data["tier"] + " " + swiftplay_player_match_data["rank"]
    swiftplay_player_match_data["full_rank"] = swiftplay_player_match_data["full_rank"].replace("UNRANKED UNRANKED", "UNRANKED")

    ranked_player_match_data["full_rank"] = ranked_player_match_data["tier"] + " " + ranked_player_match_data["rank"]
    ranked_player_match_data["full_rank"] = ranked_player_match_data["full_rank"].replace("UNRANKED UNRANKED", "UNRANKED")

    players["full_rank"] = players["tier"] + " " + players["rank"]
    players["full_rank"] = players["full_rank"].replace("UNRANKED UNRANKED", "UNRANKED")

    # Apply the CategoricalDtype to the column
    swiftplay_player_match_data["full_rank"] = swiftplay_player_match_data["full_rank"].astype(rank_type)
    ranked_player_match_data["full_rank"] = ranked_player_match_data["full_rank"].astype(rank_type)
    players["full_rank"] = players["full_rank"].astype(rank_type)

    return players, ranked_matches, ranked_player_match_data, swiftplay_matches, swiftplay_player_match_data