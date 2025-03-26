from pathlib import Path
import pandas as pd

tiers = ["DIAMOND", "EMERALD", "PLATINUM", "GOLD", "SILVER", "BRONZE", "IRON"]
divisions = ["I", "II", "III", "IV"]

files_to_merge = [
    "Players.csv", "SwiftplayMatches.csv", "SwiftplayPlayerMatchData.csv", 
    "RankedMatches.csv", "RankedPlayerMatchData.csv"
]

root_path = Path(".")

def merge_csv_files(file_name):
    all_data = []
    
    # Iterate through all tier/division subfolders
    for tier in tiers:
        for division in divisions:
            folder_path = root_path / f"{tier}_{division}"
            file_path = folder_path / file_name
            
            if file_path.exists():
                df = pd.read_csv(file_path)
                all_data.append(df)
    
    if all_data:
        merged_df = pd.concat(all_data).drop_duplicates()
        merged_df.to_csv(root_path / file_name, index=False)
        print(f"Merged {file_name} and saved to root folder.")
    else:
        print(f"No data found for {file_name}.")

if __name__ == "__main__":
    for file in files_to_merge:
        merge_csv_files(file)
