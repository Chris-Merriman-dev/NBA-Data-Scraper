'''
Created By : Christian Merriman
Date : 3/4/26

Purpose : Update to project started in 2024. Uses https://www.basketball-reference.com/players to download every NBA player to ever play.
It will use playwrite and chromium browser (update HEADLESS to see or not see).
SCRAPE_ALPHABET can be set to True, if you wish to redownload all the intitial player names. False on default.
For the player stats, you can either do all of them at once with commenting out ALPHABET_TO_PROCESS (will take too long) or use ALPHABET_TO_PROCESS and comment out 
the letters you do not want to download.

It will download the initial player names, so it knows whose data to get. Once it has those, it will go through all the players for the letters you choose to get (ALPHABET_TO_PROCESS).
If you already have any of these files, it will skip and move onto the next UNLESS you set OVERWRITE_PLAYER_STATS = True. Then it will automatically redownload.
The data is left uncleaned, we want it raw and will clean it later.


'''
import asyncio
import io
import pandas as pd
from playwright.async_api import async_playwright
import os
import string
import random

# --- CONFIGURATION ---
SCRAPE_ALPHABET = False         # Force re-scrape of index files even if they exist
OVERWRITE_PLAYER_STATS = False  
HEADLESS = True                 
DIRECTORY_DATA = "data_files"
DIRECTORY_DATA_RAWPLAYERDATA = "raw_player_data"

# --- LETTER SELECTION ---
#comment out any letter you do not wish to download, it can be time consuming to get them all. (15 to 20 minutes or more sometimes)
ALPHABET_TO_PROCESS = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'y', 'z']

SAVE_PATH = os.path.join(DIRECTORY_DATA, DIRECTORY_DATA_RAWPLAYERDATA)
stats = {"scraped": 0, "skipped": 0, "failed": 0}
os.makedirs(SAVE_PATH, exist_ok=True)

#This scrapes the master list for each letter we send in. We will use this to get our individual players data later
async def scrape_nba_letter(letter):
    """Scrapes the master list of players for a specific letter."""
    letter = letter.lower().strip()
    max_retries = 3
    
    #this will loop through max_retries to get our master list of data
    for attempt in range(max_retries):
        async with async_playwright() as p:
            #setup our chromium browser and init variables
            browser = await p.chromium.launch(headless=HEADLESS)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            url = f"https://www.basketball-reference.com/players/{letter}/"
            
            #now attempt to scrape the master list data
            try:
                print(f" [*] Indexing Letter {letter.upper()} at {url} (Attempt {attempt + 1})...")
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)

                export_button = page.locator("#all_players .section_heading span:has-text('Share & Export')")
                await export_button.wait_for(state="attached", timeout=10000)
                await export_button.dispatch_event("click")

                csv_button = page.locator("button:has-text('Get table as CSV (for Excel)')")
                await csv_button.wait_for(state="attached", timeout=10000)
                await csv_button.dispatch_event("click")

                csv_locator = page.locator("pre#csv_players")
                await csv_locator.wait_for(state="visible", timeout=10000)
                
                await asyncio.sleep(1.5) 
                full_text = await csv_locator.inner_text()

                if not full_text.strip():
                    raise ValueError("CSV container was empty.")

                df = pd.read_csv(io.StringIO(full_text), skiprows=4, sep=',')
                if df.empty:
                    raise ValueError("DataFrame is empty after parsing.")

                filepath = os.path.join(SAVE_PATH, f"nba_players_{letter}.csv")
                df.to_csv(filepath, index=False)
                
                #success scraping, close browser and end function
                print(f" [OK] Saved {len(df)} players to index.")
                await browser.close()
                return True 

            #if we failed, check to see if we should retry again
            except Exception as e:
                print(f" [!] Error indexing {letter}: {e}")
                await browser.close()
                if attempt < max_retries - 1: await asyncio.sleep(10)
                else: return False

#Used to get hte csv section from the url.
#it will just gather all the data and we will clean it later
async def get_csv_from_section(heading_locator, page, csv_id):
    await heading_locator.wait_for(state="attached", timeout=15000)
    export_button = heading_locator.locator("span:has-text('Share & Export')")
    await export_button.dispatch_event("click")
    await asyncio.sleep(1.2)
    csv_btn = heading_locator.locator("button:has-text('Get table as CSV (for Excel)')")
    await csv_btn.dispatch_event("click")
    csv_locator = page.locator(f"pre#{csv_id}")
    await csv_locator.wait_for(state="attached", timeout=12000)
    await asyncio.sleep(0.5)
    raw_text = await csv_locator.inner_text()
    lines = raw_text.split('\n')
    clean_lines = [l.strip() for l in lines if l.strip() and "cite us" not in l and not l.startswith("---")]
    return "\n".join(clean_lines)

#Will scrape the data for this players id
#It will attempt to get the players regular season and playoff stats for every year they played.
#It will make sure if we want to overwrite and save no matter what or if it already exists and we dont want to overwrite, we do not save it.
#If it fails, it will retry it max_retries
async def scrape_player_stats(player_id, letter, base_path, progress_str):
    #setup vars to scrape
    url = f"https://www.basketball-reference.com/players/{letter}/{player_id}.html"
    reg_file = f"{base_path}.csv"
    post_file = f"{base_path}_playoffs.csv"
    no_post_marker = f"{base_path}.no_post"
    
    max_retries = 3
    user_agents = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"]

    #loop through and attempt to get the data
    for attempt in range(max_retries):
        #attempt to run playwrite
        async with async_playwright() as p:
            #setup our chromium browser for playwrite to scrape
            browser = await p.chromium.launch(headless=HEADLESS) 
            context = await browser.new_context(user_agent=random.choice(user_agents))
            page = await context.new_page()
            
            #try and scrape
            try:
                #the following will attempt to get the data we are looking for
                print(f" {progress_str} > Processing: {player_id}")
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)

                if OVERWRITE_PLAYER_STATS or not os.path.exists(reg_file):
                    reg_heading = page.locator("#per_game_stats_sh")
                    reg_csv = await get_csv_from_section(reg_heading, page, "csv_per_game_stats")
                    with open(reg_file, "w", encoding="utf-8") as f:
                        f.write(reg_csv)
                
                playoff_tab = page.locator("#all_per_game_stats .sr_preset:has-text('Playoffs')")
                if await playoff_tab.count() > 0:
                    if OVERWRITE_PLAYER_STATS or not os.path.exists(post_file):
                        await playoff_tab.dispatch_event("click")
                        await asyncio.sleep(1.5)
                        post_heading = page.locator("#per_game_stats_post_sh")
                        await post_heading.wait_for(state="attached", timeout=8000)
                        post_csv = await get_csv_from_section(post_heading, page, "csv_per_game_stats_post")
                        if len(post_csv) > 20: 
                            with open(post_file, "w", encoding="utf-8") as f:
                                f.write(post_csv)
                            if os.path.exists(no_post_marker): os.remove(no_post_marker)
                else:
                    if not os.path.exists(no_post_marker):
                        with open(no_post_marker, "w") as f: f.write("checked")
                
                #we got data, end function
                stats["scraped"] += 1
                await browser.close()
                return True
            
            #if we error out, print out the error and see if we need to retry it again
            except Exception as e:
                print(f"    ! Error on {player_id}: {e}")
                await browser.close()
                if attempt < max_retries - 1: await asyncio.sleep(random.uniform(10, 15))
                else: return False

#This will go through every last name of every NBA player ever.
#It will scape any data it can. It will also put in sleeps, so you do not get timed out from the site. Also it will
#make sure to retry if an error happens. It a file already exists, it may or may not redownload the data. It depends on OVERWRITE_PLAYER_STATS being True or False.
#True will overwrite the files and redownload it again.
async def run_full_pipeline():

    #get our letters we wish to download
    target_alphabet = ALPHABET_TO_PROCESS if ALPHABET_TO_PROCESS else [c for c in string.ascii_lowercase if c != 'x']
    
    #go through every starting letter for NBA players last names
    for letter in target_alphabet:

        #setup the files
        alphabet_file = os.path.join(SAVE_PATH, f"nba_players_{letter}.csv")
        letter_dir = os.path.join(SAVE_PATH, letter)
        os.makedirs(letter_dir, exist_ok=True)
        
        # --- AUTO-CHECK: DO WE NEED TO SCRAPE THE INDEX? ---
        if SCRAPE_ALPHABET or not os.path.exists(alphabet_file):
            print(f"\n--- Index Missing for {letter.upper()}: Scraping Now ---")
            success = await scrape_nba_letter(letter)
            if not success:
                print(f" [!] Failed to get index for {letter}. Skipping to next letter.")
                continue
            await asyncio.sleep(random.uniform(4, 7))
        else:
            print(f"--- Index for {letter.upper()} already exists. Proceeding to players. ---")

        #open and setup our dataframe for the letter
        df_players = pd.read_csv(alphabet_file)
        player_ids = df_players['Player-additional'].tolist()
        
        #loop through every player for this letter, to get their NBA regular season and playoff stats.
        print(f"\n--- Processing Stats for Letter: {letter.upper()} ---")
        for index, p_id in enumerate(player_ids, 1):
            actual_letter = p_id[0].lower()
            base_file_path = os.path.join(letter_dir, p_id)
            progress_str = f"[{index}/{len(player_ids)}]"
            
            #checks to see if we should overwrite or the file already exists, to know if we should skip it
            if not OVERWRITE_PLAYER_STATS and os.path.exists(f"{base_file_path}.csv") and \
               (os.path.exists(f"{base_file_path}_playoffs.csv") or os.path.exists(f"{base_file_path}.no_post")):
                print(f" {progress_str} > Skipping: {p_id} (Complete)")
                stats["skipped"] += 1
                continue
            
            #scrape the data and do a random sleep, so we dont get timed out from the url
            await scrape_player_stats(p_id, actual_letter, base_file_path, progress_str)
            await asyncio.sleep(random.uniform(3, 6))

    print("\n" + "="*40)
    print(f"Total Scraped: {stats['scraped']} | Skipped: {stats['skipped']} | Failed: {stats['failed']}")
    print("="*40)

if __name__ == "__main__":
    asyncio.run(run_full_pipeline())