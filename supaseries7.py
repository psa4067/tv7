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

if not SUPABASE_URL or not SUPABASE_KEY:
    print("[ERROR] API Key Supabase tidak ditemukan!")
    exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- KONFIGURASI RENTANG ---
TAHUN_MULAI = 2000
TAHUN_SELESAI = 1999
HALAMAN_MULAI = 1

@browser(
    headless=True,
    reuse_driver=True,
    block_images=True,
    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
def run_series_supabase_scraper(driver: Driver, data=None):
    base_domain = "https://tv3.nontondrama.my"
    
    while True: 
        print(f"\n{'='*80}")
        print(f"[*] SIKLUS DIMULAI: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}")

        for target_year in range(TAHUN_MULAI, TAHUN_SELESAI - 1, -1):
            year_url = f"{base_domain}/year/{target_year}"
            driver.google_get(year_url)
            time.sleep(5)
            
            soup = soupify(driver)
            total_pages = 1
            h3_el = soup.select_one('div.container h3')
            if h3_el and "dari" in h3_el.text:
                match = re.search(r'dari\s+(\d+)', h3_el.text)
                if match: total_pages = int(match.group(1))

            current_start_page = HALAMAN_MULAI if target_year == TAHUN_MULAI else 1
            
            for page in range(current_start_page, total_pages + 1):
                p_url = f"{year_url}/page/{page}" if page > 1 else year_url
                print(f"\n--- HALAMAN {page}/{total_pages} (TAHUN {target_year}) ---")
                
                driver.get(p_url)
                time.sleep(5)
                page_soup = soupify(driver)
                articles = page_soup.select('article figure a')
                
                for a_tag in articles:
                    s_link = a_tag.get('href')
                    if not s_link: continue
                    if not s_link.startswith('http'): s_link = base_domain + s_link
                    
                    # Ambil slug dasar dari URL
                    series_slug_raw = s_link.strip('/').split('/')[-1]
                    # Bersihkan tahun lama jika ada di slug URL (misal: judul-2016 -> judul)
                    base_title = re.sub(r'-\d{4}$', '', series_slug_raw)

                    try:
                        print(f"\n[*] Menganalisa Series: {base_title}")
                        driver.get(s_link)
                        time.sleep(4)
                        series_soup = soupify(driver)
                        
                        seasons = [opt.get('value') for opt in series_soup.select('select.season-select option') if opt.get('value')]
                        
                        for sea_num in seasons:
                            # 1. Buka Episode 1 untuk mendapatkan Tahun yang Akurat dari movie-info
                            entry_url = f"{base_domain}/{base_title}-season-{sea_num}-episode-1-{target_year}"
                            driver.get(entry_url)
                            time.sleep(5)
                            
                            if "404" in driver.title or "Not Found" in driver.title:
                                entry_url = f"{base_domain}/{base_title}-episode-1-{target_year}"
                                driver.get(entry_url)
                                time.sleep(5)

                            watch_soup = soupify(driver)
                            
                            # --- EKSTRAKSI TAHUN DARI H1 (movie-info) ---
                            # Format: "Nonton Bloodhounds - Season 1 Episode 1 (2023) Streaming"
                            h1_el = watch_soup.select_one('div.movie-info h1')
                            extracted_year = str(target_year) # Fallback ke tahun folder jika gagal
                            
                            if h1_el:
                                year_match = re.search(r'\((\d{4})\)', h1_el.text)
                                if year_match:
                                    extracted_year = year_match.group(1)
                            
                            # Format series_title: judul-tahun
                            series_title_db = f"{base_title}-{extracted_year}"
                            
                            episode_links = watch_soup.select('ul.episode-list li a')
                            if not episode_links:
                                continue

                            print(f"    [>] Season {sea_num} ({extracted_year}): {len(episode_links)} Episode ditemukan.")

                            for ep_tag in episode_links:
                                ep_href = ep_tag.get('href')
                                ep_text = ep_tag.text.strip()
                                clean_ep_num = re.sub(r'\D', '', ep_text)
                                if not clean_ep_num: continue

                                # Format id_episode: judul-tahun-sX-eX
                                unique_id = f"{series_title_db}-s{sea_num}-e{clean_ep_num}"
                                
                                ep_final_url = ep_href if ep_href.startswith('http') else base_domain + ep_href

                                # Cek DB
                                check_db = supabase.table("series_episodes").select("id_episode").eq("id_episode", unique_id).execute()
                                if check_db.data:
                                    print(f"        [-] Skip {unique_id} (Exist)")
                                    continue

                                print(f"        [*] Scraping: {unique_id}")
                                driver.get(ep_final_url)
                                time.sleep(7)
                                
                                video_soup = soupify(driver)
                                options = video_soup.select('select#player-select option')
                                
                                links_only = {}
                                for opt in options:
                                    val = opt.get('value', '').strip()
                                    serv = opt.get('data-server', '').lower()
                                    if val and serv in ['cast', 'turbovip']:
                                        clean_v = 'https:' + val if val.startswith('//') else val
                                        links_only[serv] = clean_v
                                
                                if links_only:
                                    payload = {
                                        "id_episode": unique_id,
                                        "series_title": series_title_db, # Format: judul-tahun
                                        "season": int(sea_num),
                                        "episode": int(clean_ep_num),
                                        "link_cast": links_only.get('cast'),
                                        "link_turbo": links_only.get('turbovip')
                                    }
                                    supabase.table("series_episodes").upsert(payload).execute()
                                    print(f"            [OK] Saved to DB.")

                    except Exception as e:
                        print(f"    [ERROR] Gagal pada {base_title}: {e}")

        print("\n[DONE] Siklus selesai. Menunggu restart...")
        time.sleep(900)

if __name__ == "__main__":
    run_series_supabase_scraper()
