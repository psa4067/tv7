import time
import re
import os
from datetime import datetime
from dotenv import load_dotenv
from botasaurus.browser import browser, Driver
from botasaurus.soupify import soupify
from supabase import create_client

# --- LOAD KONFIGURASI ---
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

@browser(
    headless=True,
    reuse_driver=True,
    block_images=True,
    window_size=(1366, 768),
    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
def run_full_season_github_action(driver: Driver, data=None):
    base_domain = "https://tv3.nontondrama.my"
    
    # --- CONFIG GITHUB ACTION ---
    TARGET_YEARS = [2014, 2013]
    START_PAGE = 1 # Set ke 1 untuk auto-run rutin, atau sesuaikan jika ingin catch-up data lama
    # ---------------------------

    print(f"[*] GITHUB ACTION START - YEARS: {TARGET_YEARS}")

    for target_year in TARGET_YEARS:
        year_url = f"{base_domain}/year/{target_year}"
        print(f"\n[!] SCANNING TAHUN: {target_year}")
        
        driver.google_get(year_url)
        time.sleep(10)
        
        soup = soupify(driver)
        total_pages = 1
        h3_el = soup.select_one('div.container h3')
        if h3_el and "dari" in h3_el.text:
            match = re.search(r'dari\s+(\d+)', h3_el.text)
            if match: total_pages = int(match.group(1))
        
        for page in range(START_PAGE, total_pages + 1):
            p_url = f"{year_url}/page/{page}" if page > 1 else year_url
            print(f"\n--- HALAMAN {page}/{total_pages} ---")
            
            if page > START_PAGE:
                driver.get(p_url)
                time.sleep(7)
            
            page_soup = soupify(driver)
            articles = page_soup.select('article')
            
            for art in articles:
                a_tag = art.select_one('figure a')
                if not a_tag: continue
                
                s_link = a_tag.get('href')
                full_s_link = s_link if s_link.startswith('http') else base_domain + s_link
                series_slug = full_s_link.strip('/').split('/')[-1]
                
                try:
                    print(f"[*] Checking Series: {series_slug}")
                    driver.get(full_s_link)
                    time.sleep(6)
                    
                    series_soup = soupify(driver)
                    seasons = [opt.get('value') for opt in series_soup.select('select.season-select option') if opt.get('value')]
                    
                    if not seasons: continue

                    for sea_num in seasons:
                        ep_elements = series_soup.select('ul.episode-list li a')
                        temp_ep_list = []
                        
                        for ep_el in ep_elements:
                            ep_href = ep_el.get('href')
                            clean_ep_num = re.sub(r'\D', '', ep_el.text.strip())
                            if not clean_ep_num: continue
                            
                            unique_id = f"{series_slug}-s{sea_num}-e{clean_ep_num}"
                            temp_ep_list.append({
                                "id": unique_id,
                                "url": ep_href if ep_href.startswith('http') else base_domain + ep_href,
                                "ep_num": int(clean_ep_num),
                                "sea_num": int(sea_num)
                            })

                        if not temp_ep_list: continue

                        # BATCH CHECK KE SUPABASE (Hemat API Call & Waktu)
                        ids_to_check = [item['id'] for item in temp_ep_list]
                        check_db = supabase.table("series_episodes").select("id_episode").in_("id_episode", ids_to_check).execute()
                        existing_ids = {item['id_episode'] for item in check_db.data}

                        for ep_data in temp_ep_list:
                            if ep_data['id'] in existing_ids:
                                continue 

                            print(f"        [+] NEW EPISODE: {ep_data['id']}")
                            driver.get(ep_data['url'])
                            time.sleep(8)
                            
                            video_soup = soupify(driver)
                            options = video_soup.select('select#player-select option')
                            
                            links = {}
                            for opt in options:
                                val, serv = opt.get('value', '').strip(), opt.get('data-server', '').lower()
                                if val and serv in ['cast', 'turbovip']:
                                    links[serv] = 'https:' + val if val.startswith('//') else val
                            
                            if links:
                                payload = {
                                    "id_episode": ep_data['id'],
                                    "series_title": series_slug,
                                    "season": ep_data['sea_num'],
                                    "episode": ep_data['ep_num'],
                                    "link_cast": links.get('cast'),
                                    "link_turbo": links.get('turbovip')
                                }
                                supabase.table("series_episodes").upsert(payload).execute()
                                print(f"            [OK] Saved to Supabase.")

                except Exception as e:
                    print(f"    [ERR] {series_slug}: {e}")
        
        START_PAGE = 1 # Reset untuk tahun berikutnya

if __name__ == "__main__":
    run_full_season_github_action()
