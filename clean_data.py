'''
Created By : Christian Merriman
Date : 3/4/26

Purpose : To clean the data we downloaded with our scraper app. Very simple, it will go through it and remove unwanted data and fix any empty data.
'''
import pandas as pd
import os
import glob
import time
import asyncio
import string

# --- CONFIGURATION ---
# Generates ['a', 'b', ..., 'k']
LETTERS_TO_CLEAN = [char for char in string.ascii_lowercase if char != 'x'] # list(string.ascii_lowercase[:11]) 
DEBUG_PRINTOUT = True 
pd.set_option('future.no_silent_downcasting', True)

# --- DIRECTORY PATHS ---
DIRECTORY_DATA = "data_files"
DIRECTORY_RAW = "raw_player_data"
DIRECTORY_CLEAN = "cleaned_data"
DIRECTORY_SUB_CLEAN = "original_cleaned_data"
CLEANED_OUTPUT_DIR = os.path.join(DIRECTORY_DATA, DIRECTORY_CLEAN, DIRECTORY_SUB_CLEAN)

os.makedirs(CLEANED_OUTPUT_DIR, exist_ok=True)

#makes sure we have our columns in the correct order
def get_ordered_master_columns(files):
    """Pass 1: Scans headers to build an ordered master list for the current batch."""
    master_cols = ['Player_ID', 'Season', 'Age', 'Team', 'Lg', 'Pos']
    seen = set(master_cols)
    for f in files:
        try:
            temp_df = pd.read_csv(f, nrows=0)
            for col in temp_df.columns:
                if col not in seen and not col.startswith('Unnamed:'):
                    master_cols.append(col)
                    seen.add(col)
        except Exception:
            continue
    for final_col in ['Awards', 'Type']:
        if final_col in master_cols:
            master_cols.remove(final_col)
        master_cols.append(final_col)
    return master_cols

#cleans the dataframe
def clean_basketball_df(file_path, player_id, is_playoff, master_cols):
    """Cleans individual player CSVs and aligns to the master schema."""
    try:
        df = pd.read_csv(file_path)
        
        # 1. Valid Season Format Filter
        df = df.dropna(subset=['Season'])
        df = df[df['Season'].str.contains(r'^\d{4}-\d{2}', na=False)]
        
        # 2. Filter out "Did not play" and non-NBA league rows
        if 'Lg' in df.columns:
            df = df[df['Lg'].str.len() <= 3] 
            df = df[~df['Lg'].str.contains("Did not play", na=False, case=False)]
        
        if df.empty:
            return None

        # 3. Identity & Schema Alignment
        df['Player_ID'] = player_id
        df['Type'] = 'Playoffs' if is_playoff else 'Regular'
        df = df.reindex(columns=master_cols)

        # 4. Numeric Hammer
        text_cols = ['Player_ID', 'Season', 'Team', 'Lg', 'Pos', 'Awards', 'Type']
        for col in df.columns:
            if col not in text_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 5. Final NaN cleanup
        df = df.infer_objects(copy=False)
        df = df.fillna(0)
        
        return df
    except Exception as e:
        if DEBUG_PRINTOUT:
            print(f"\n    ! CRITICAL ERROR: {player_id} failed: {e}")
        return None

#Goes through each letter to clean
async def process_letters(letter_list):
    total_start_time = time.time()
    
    # Containers for the final merge
    all_reg_dfs = []
    all_post_dfs = []
    
    #loop through letters
    for letter in letter_list:
        letter_start_time = time.time()
        reg_list, post_list = [], []
        source_dir = os.path.join(DIRECTORY_DATA, DIRECTORY_RAW, letter)
        
        if not os.path.exists(source_dir):
            if DEBUG_PRINTOUT: print(f"--- Skipping {letter.upper()}: Directory not found ---")
            continue

        files = sorted(glob.glob(os.path.join(source_dir, "*.csv")))
        if not files:
            if DEBUG_PRINTOUT: print(f"--- Skipping {letter.upper()}: No CSVs found ---")
            continue

        if DEBUG_PRINTOUT:
            print(f"\n[{letter.upper()}] PASS 1: Scanning {len(files)} files to align schemas...")
        
        ordered_master = get_ordered_master_columns(files)
        
        if DEBUG_PRINTOUT:
            print(f"[{letter.upper()}] Success: Found {len(ordered_master)} unique stat categories.")
            print(f"[{letter.upper()}] PASS 2: Processing players and filtering junk rows...")
        
        for index, file_path in enumerate(files, 1):
            filename = os.path.basename(file_path)
            is_playoff = "_playoffs.csv" in filename
            player_id = filename.replace("_playoffs.csv", "").replace(".csv", "")
            
            if DEBUG_PRINTOUT:
                p_type = "POST" if is_playoff else "REG "
                print(f"    ({index}/{len(files)}) {p_type} | {player_id: <18}", end="\r")
            
            cleaned_df = clean_basketball_df(file_path, player_id, is_playoff, ordered_master)
            
            if cleaned_df is not None:
                if is_playoff: post_list.append(cleaned_df)
                else: reg_list.append(cleaned_df)

        if DEBUG_PRINTOUT:
            print(f"\n[{letter.upper()}] PASS 3: Concatenating and saving letter results...")

        # Save individual letter files and keep in memory for final merge
        for data_list, suffix, master_accumulator in [(reg_list, "reg_season", all_reg_dfs), 
                                                       (post_list, "playoffs", all_post_dfs)]:
            if data_list:
                final_df = pd.concat(data_list, ignore_index=True)
                out_path = os.path.join(CLEANED_OUTPUT_DIR, f"nba_{suffix}_{letter}.csv")
                final_df.to_csv(out_path, index=False)
                master_accumulator.append(final_df) # Store for final combine
                if DEBUG_PRINTOUT:
                    print(f"    -> Created: {os.path.basename(out_path)} ({len(final_df)} rows)")

        if DEBUG_PRINTOUT:
            l_time = time.time() - letter_start_time
            print(f"[{letter.upper()}] Completed in {l_time:.2f}s")

    # --- FINAL PASS: COMBINE EVERYTHING ---
    if DEBUG_PRINTOUT:
        print(f"\n{'='*50}")
        print("PASS 4: MERGING ALL LETTERS INTO MASTER FILES...")
        print(f"{'='*50}")

    for master_list, suffix in [(all_reg_dfs, "reg_season_all_a_z"), (all_post_dfs, "playoffs_all_a_z")]:
        if master_list:
            # pd.concat handles the union of all columns across all letters
            master_df = pd.concat(master_list, ignore_index=True)
            
            # If letter 'a' had a stat letter 'k' didn't have, fill that gap with 0
            master_df = master_df.fillna(0)
            
            master_out_path = os.path.join(CLEANED_OUTPUT_DIR, f"nba_{suffix}.csv")
            master_df.to_csv(master_out_path, index=False)
            
            if DEBUG_PRINTOUT:
                print(f"MASTER SUCCESS: {len(master_df)} total rows saved to {os.path.basename(master_out_path)}")

    if DEBUG_PRINTOUT:
        total_time = time.time() - total_start_time
        print(f"\nJOB COMPLETE: Processed {len(letter_list)} letters in {total_time:.2f}s")
        print(f"OUTPUT DIR: {os.path.abspath(CLEANED_OUTPUT_DIR)}")

if __name__ == "__main__":
    asyncio.run(process_letters(LETTERS_TO_CLEAN))