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
def run_series_scraper(driver: Driver, data=None):
    base_domain = "https://tv3.nontondrama.my"
    
    # --- CONFIG FILTER ---
    TARGET_YEARS = [2014, 2013]
    START_PAGE = 1 
    # ---------------------------

    print(f"[*] SCRAPER START - YEARS: {TARGET_YEARS}")

    for target_year in TARGET_YEARS:
        year_url = f"{base_domain}/year/{target_year}"
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
                    
                    # Ambil daftar season yang tersedia
                    initial_soup = soupify(driver)
                    seasons = [opt.get('value') for opt in initial_soup.select('select.season-select option') if opt.get('value')]
                    
                    if not seasons:
                        # Jika tidak ada dropdown, mungkin hanya ada 1 season (S1)
                        seasons = ['1']

                    for sea_num in seasons:
                        print(f"    [-] Processing Season {sea_num}...")
                        
                        # --- FIX: LOGIKA PINDAH SEASON ---
                        # Kita coba klik/pilih season di dropdown
                        try:
                            if len(seasons) > 1:
                                driver.select('select.season-select', sea_num)
                                time.sleep(5) # Tunggu AJAX update daftar episode
                        except:
                            print(f"        [!] Gagal pindah season via UI, mencoba stay di halaman saat ini.")

                        # Ambil ulang soup setelah pindah season
                        current_series_soup = soupify(driver)
                        ep_elements = current_series_soup.select('ul.episode-list li a')
                        
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

                        if not temp_ep_list:
                            print(f"        [!] No episodes found for S{sea_num}")
                            continue

                        # --- BATCH CHECK KE SUPABASE ---
                        ids_to_check = [item['id'] for item in temp_ep_list]
                        check_db = supabase.table("series_episodes").select("id_episode").in_("id_episode", ids_to_check).execute()
                        existing_ids = {item['id_episode'] for item in check_db.data}

                        for ep_data in temp_ep_list:
                            if ep_data['id'] in existing_ids:
                                continue 

                            print(f"        [+] NEW EP: {ep_data['id']}")
                            driver.get(ep_data['url'])
                            time.sleep(8)
                            
                            video_soup = soupify(driver)
                            options = video_soup.select('select#player-select option')
                            
                            links = {}
                            # Menambah server cadangan agar tidak banyak yang skip
                            allowed_servers = ['cast', 'turbovip', 'pro', 'direct']
                            
                            for opt in options:
                                val = opt.get('value', '').strip()
                                serv = opt.get('data-server', '').lower()
                                
                                if val and serv in allowed_servers:
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
                                print(f"            [OK] Saved.")
                            else:
                                print(f"            [SKIP] No premium servers found.")

                        # Setelah selesai satu season, balik ke halaman series 
                        # untuk reset state dropdown jika perlu
                        driver.get(full_s_link)
                        time.sleep(3)

                except Exception as e:
                    print(f"    [ERR] {series_slug}: {e}")
        
        START_PAGE = 1

if __name__ == "__main__":
    run_series_scraper()
