# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from urllib.parse import urljoin
from tqdm import tqdm

# ==================== CẤU HÌNH ====================
BASE_URLS = {
    'Nhi':      'https://www.vinmec.com/vie/trung-tam-nhi',
    'Phụ nữ':   'https://www.vinmec.com/vie/trung-tam-suc-khoe-phu-nu'
}

MAX_PAGES = {
    'Nhi':      250,
    'Phụ nữ':   200
}

HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/130.0.0.0 Safari/537.36')
}

all_links = []

# ==================== HÀM CRAWL MỘT TRANG (CỐ GẮNG LẠI NẾU LỖI) ====================
def crawl_page_with_retry(base_url: str, category: str, page: int, pbar: tqdm, max_retries=3) -> int:
    url = f"{base_url}/page_{page}" if page > 1 else base_url
    added = 0

    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)  # Tăng timeout lên 30s
            if resp.status_code != 200:
                pbar.write(f" [Cảnh báo] HTTP {resp.status_code} tại trang {page} (lần {attempt+1})")
                if attempt == max_retries - 1:
                    return 0
                time.sleep(2)
                continue

            soup = BeautifulSoup(resp.text, 'html.parser')
            candidates = (
                soup.find_all('a', class_='title-link') or
                soup.find_all('a', class_='post-title') or
                soup.find_all('a', href=lambda h: h and ('/bai-viet/' in h or '/tin-tuc/' in h))
            )

            # Không dừng dù không có link → vẫn tiếp tục trang sau
            for a in candidates:
                href = a.get('href')
                if not href:
                    continue
                full_url = urljoin('https://www.vinmec.com', href)
                if '/bai-viet/' not in full_url and '/tin-tuc/' not in full_url:
                    continue

                title_tag = a.find('h3')
                title = title_tag.get_text(strip=True) if title_tag else a.get_text(strip=True).strip()

                if full_url not in [item['URL'] for item in all_links]:
                    all_links.append({
                        'Chuyên khoa': category,
                        'URL': full_url,
                        'Title': title
                    })
                    added += 1

            break  # Thành công → thoát vòng retry

        except requests.exceptions.RequestException as e:
            pbar.write(f" [Lỗi] Trang {page} (lần {attempt+1}): {e}")
            if attempt == max_retries - 1:
                pbar.write(f" [Bỏ qua] Trang {page} sau {max_retries} lần thử")
            else:
                time.sleep(3)

    return added

# ==================== CHẠY CRAWL – BẮT BUỘC ĐỦ TRANG ====================
print("BẮT ĐẦU CRAWL VINMEC - CỐ ĐỊNH SỐ TRANG\n")

with tqdm(
    total=sum(MAX_PAGES.values()),
    desc="Tổng số trang",
    bar_format='{l_bar}{bar}| {n}/{total} trang | {postfix}'
) as pbar:

    for cat, base in BASE_URLS.items():
        max_pages = MAX_PAGES[cat]
        pbar.set_postfix_str(f"Đang xử lý: {cat} (0/{max_pages} trang)")

        for page in range(1, max_pages + 1):
            added = crawl_page_with_retry(base, cat, page, pbar)
            current_links = sum(1 for x in all_links if x['Chuyên khoa'] == cat)

            pbar.set_postfix({
                'Chuyên khoa': cat,
                'Trang': f"{page}/{max_pages}",
                'Link': f"{current_links}",
                '+trang này': f"+{added}"
            })

            #if added > 0:
            #    pbar.write(f" [Success] Trang {page}: +{added} bài → Tổng {cat}: {current_links}")

            pbar.update(1)
            time.sleep(0.2)  # Vẫn giữ lịch sự

        total_links_cat = sum(1 for x in all_links if x['Chuyên khoa'] == cat)
        pbar.set_postfix_str(f"HOÀN TẤT: {cat} | {total_links_cat} link từ {page} trang")

# ==================== XUẤT KẾT QUẢ ====================
if all_links:
    df = pd.DataFrame(all_links)
    df = df.drop_duplicates(subset='URL')
    df = df.sort_values('Chuyên khoa').reset_index(drop=True)
    stats = df['Chuyên khoa'].value_counts()

    out_file = 'vinmec_nhi_phunu_full_pages.csv'
    df.to_csv(out_file, index=False, encoding='utf-8-sig')

    print(f"\nHOÀN TẤT – ĐÃ CRAWL ĐỦ SỐ TRANG!")
    print(f"   • Nhi:      {stats.get('Nhi', 0):,} link từ 500 trang")
    print(f"   • Phụ nữ:   {stats.get('Phụ nữ', 0):,} link từ 200 trang")
    print(f"   • Tổng link: {len(df):,}")
    print(f"   • File: {out_file}")
    print("\n10 LINK ĐẦU:")
    print(df.head(10)[['Chuyên khoa', 'URL']].to_string(index=False))
else:
    print("\nKHÔNG CRAWL ĐƯỢC LINK NÀO!")