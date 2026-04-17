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
TAHUN_MULAI = 2014
TAHUN_SELESAI = 2013
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
                    
                    series_slug_full = s_link.strip('/').split('/')[-1]
                    
                    # Memisahkan Judul dan Tahun (Contoh: stranger-things-2016)
                    match_slug = re.match(r"(.+)-(\d{4})$", series_slug_full)
                    if match_slug:
                        base_title = match_slug.group(1)
                        series_year = match_slug.group(2)
                    else:
                        base_title = series_slug_full
                        series_year = str(target_year)

                    try:
                        print(f"\n[*] Menganalisa Series: {base_title}")
                        driver.get(s_link)
                        time.sleep(4)
                        series_soup = soupify(driver)
                        
                        # Dapatkan semua Season yang tersedia
                        seasons = [opt.get('value') for opt in series_soup.select('select.season-select option') if opt.get('value')]
                        
                        for sea_num in seasons:
                            print(f"    [>] Memproses Season {sea_num}...")
                            
                            # STRATEGI: Buka Episode 1 untuk setiap season guna memicu list episode yang benar
                            # Format: /judul-season-X-episode-1-tahun
                            entry_url = f"{base_domain}/{base_title}-season-{sea_num}-episode-1-{series_year}"
                            
                            driver.get(entry_url)
                            time.sleep(5)
                            
                            # Jika 404 pada format standar (terutama Season 1), coba format alternatif
                            if "404" in driver.title or "Not Found" in driver.title:
                                entry_url = f"{base_domain}/{base_title}-episode-1-{series_year}"
                                driver.get(entry_url)
                                time.sleep(5)

                            # Sekarang kita berada di watchpage season tersebut. 
                            # Ambil list episode dari 'ul.episode-list.fade-in' sesuai screenshot Anda
                            watch_soup = soupify(driver)
                            episode_links = watch_soup.select('ul.episode-list li a')
                            
                            if not episode_links:
                                print(f"    [!] Tidak ditemukan list episode di {entry_url}")
                                continue

                            print(f"    [+] Ditemukan {len(episode_links)} episode untuk Season {sea_num}")

                            for ep_tag in episode_links:
                                ep_href = ep_tag.get('href')
                                ep_text = ep_tag.text.strip()
                                # Ambil angka episode saja
                                clean_ep_num = re.sub(r'\D', '', ep_text)
                                if not clean_ep_num: continue

                                ep_final_url = ep_href if ep_href.startswith('http') else base_domain + ep_href
                                unique_id = f"{base_title}-s{sea_num}-e{clean_ep_num}"

                                # Cek Database
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
                                        "series_title": base_title,
                                        "season": int(sea_num),
                                        "episode": int(clean_ep_num),
                                        "link_cast": links_only.get('cast'),
                                        "link_turbo": links_only.get('turbovip')
                                    }
                                    supabase.table("series_episodes").upsert(payload).execute()
                                    print(f"            [OK] Saved.")
                                else:
                                    print(f"            [!] Link video tidak ditemukan.")

                    except Exception as e:
                        print(f"    [ERROR] Gagal pada {base_title}: {e}")

        print("\n[DONE] Siklus selesai. Istirahat 15 menit...")
        time.sleep(900)

if __name__ == "__main__":
    run_series_supabase_scraper()
