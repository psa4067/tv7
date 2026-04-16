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

# --- KONFIGURASI RENTANG SCRAPING ---
# Silakan ubah variabel di bawah ini sesuai kebutuhan
TAHUN_MULAI = 2014
TAHUN_SELESAI = 2013  # Berhenti di tahun ini (inklusif)
HALAMAN_MULAI = 1     # Mulai dari halaman ini (hanya untuk TAHUN_MULAI)

@browser(
    headless=True,
    reuse_driver=True,
    block_images=True,
    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
def run_series_supabase_scraper(driver: Driver, data=None):
    base_domain = "https://tv3.nontondrama.my"
    
    # Loop Abadi jika ingin terus berulang, atau hapus 'while True' jika hanya ingin sekali jalan
    while True: 
        print(f"\n{'='*80}")
        print(f"[*] SIKLUS SERIES DIMULAI: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[*] RANGE: {TAHUN_MULAI} ke {TAHUN_SELESAI} | START PAGE: {HALAMAN_MULAI}")
        print(f"{'='*80}")

        # Loop dari Tahun Mulai turun ke Tahun Selesai
        for target_year in range(TAHUN_MULAI, TAHUN_SELESAI - 1, -1):
            year_url = f"{base_domain}/year/{target_year}"
            print(f"\n[!] TARGET TAHUN: {target_year}")
            
            # 1. Kunjungi halaman pertama tahun tersebut untuk mendapatkan TOTAL HALAMAN
            driver.google_get(year_url)
            time.sleep(5)
            
            soup = soupify(driver)
            total_pages = 1
            h3_el = soup.select_one('div.container h3')
            if h3_el and "dari" in h3_el.text:
                match = re.search(r'dari\s+(\d+)', h3_el.text)
                if match: 
                    total_pages = int(match.group(1))

            # Tentukan dari halaman mana kita mulai
            # Jika tahun saat ini adalah TAHUN_MULAI, gunakan HALAMAN_MULAI. 
            # Jika sudah ganti tahun, mulai dari 1.
            current_start_page = HALAMAN_MULAI if target_year == TAHUN_MULAI else 1
            
            if current_start_page > total_pages:
                print(f"[!] Start Page {current_start_page} melampaui total halaman ({total_pages}). Skip tahun ini.")
                continue

            for page in range(current_start_page, total_pages + 1):
                p_url = f"{year_url}/page/{page}" if page > 1 else year_url
                print(f"\n--- HALAMAN {page}/{total_pages} --- URL: {p_url}")
                
                driver.get(p_url)
                time.sleep(5)
                
                page_soup = soupify(driver)
                raw_links = [a.get('href') for a in page_soup.select('article figure a') if a.get('href')]
                
                if not raw_links:
                    print(f"[?] Tidak ada series ditemukan di halaman {page}")
                    continue

                for link in raw_links:
                    s_link = link if link.startswith('http') else base_domain + (link if link.startswith('/') else '/' + link)
                    series_slug = s_link.strip('/').split('/')[-1]
                    
                    try:
                        print(f"[*] Checking Series: {series_slug}")
                        driver.get(s_link)
                        time.sleep(5)
                        
                        series_soup = soupify(driver)
                        seasons = [opt.get('value') for opt in series_soup.select('select.season-select option') if opt.get('value')]
                        
                        if not seasons:
                            continue

                        for sea_num in seasons:
                            ep_elements = series_soup.select('ul.episode-list li a')
                            temp_ep_list = []
                            
                            for ep_el in ep_elements:
                                ep_href = ep_el.get('href')
                                ep_num_text = ep_el.text.strip()
                                clean_ep_num = re.sub(r'\D', '', ep_num_text)
                                if not clean_ep_num: continue
                                
                                unique_id = f"{series_slug}-s{sea_num}-e{clean_ep_num}"
                                full_ep_url = ep_href if ep_href.startswith('http') else base_domain + (ep_href if ep_href.startswith('/') else '/' + ep_href)
                                
                                temp_ep_list.append({
                                    "id": unique_id,
                                    "url": full_ep_url,
                                    "ep_num": int(clean_ep_num),
                                    "sea_num": int(sea_num)
                                })

                            if not temp_ep_list: continue

                            # --- BATCH CHECK (SUPABASE) ---
                            ids_to_check = [item['id'] for item in temp_ep_list]
                            check_db = supabase.table("series_episodes").select("id_episode").in_("id_episode", ids_to_check).execute()
                            existing_ids = {item['id_episode'] for item in check_db.data}

                            for ep_data in temp_ep_list:
                                if ep_data['id'] in existing_ids:
                                    print(f"    [-] Skip (Exist): {ep_data['id']}")
                                    continue

                                print(f"    [*] Scraping New Episode: {ep_data['id']}")
                                driver.get(ep_data['url'])
                                time.sleep(8)
                                
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
                                        "id_episode": ep_data['id'],
                                        "series_title": series_slug,
                                        "season": ep_data['sea_num'],
                                        "episode": ep_data['ep_num'],
                                        "link_cast": links_only.get('cast'),
                                        "link_turbo": links_only.get('turbovip')
                                    }
                                    supabase.table("series_episodes").upsert(payload).execute()
                                    print(f"        [OK] Saved to DB.")
                                else:
                                    print(f"        [!] No links found.")

                    except Exception as e:
                        print(f"    [ERROR] Gagal pada {series_slug}: {e}")

        print("\n[DONE] Semua range tahun selesai. Menunggu 15 menit sebelum restart...")
        time.sleep(900)

if __name__ == "__main__":
    run_series_supabase_scraper()
