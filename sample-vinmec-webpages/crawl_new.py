# === YÊU CẦU ===
# Cài đặt các thư viện này trước khi chạy:
# pip install pandas requests beautifulsoup4 lxml tqdm

import re
import time
import os
import pandas as pd
from typing import List, Dict, Any
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup

# === PHẦN 1: CẤU HÌNH ===

# Tên file CSV đầu vào (file bạn đã gửi)
# Đảm bảo file này nằm cùng thư mục với script .py
INPUT_CSV_PATH = "vinmec_nhi_phunu_full_pages.csv"

# Tên file CSV đầu ra (chứa nội dung đã crawl)
OUTPUT_CSV_PATH = "crawled_content_from_csv.csv"

# Số lượng URL tối đa để crawl (lấy từ Cell 7)
# Đặt là None để crawl tất cả (4945 URL)
# CẢNH BÁO: Crawl tất cả 4945 URL sẽ mất RẤT LÂU (khoảng 50-60 phút)
CRAWL_LIMIT = 4945

# Cấu hình crawl (từ Cell 2)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
}
REQ_TIMEOUT = 15
SLEEP_BETWEEN_REQ = 0.6

# === PHẦN 2: CÁC HÀM CRAWL (Từ Cell 4) ===

def clean_text(txt: str) -> str:
    """Xóa các khoảng trắng thừa."""
    if not txt:
        return ""
    txt = re.sub(r"\\s+", " ", txt, flags=re.MULTILINE).strip()
    return txt

def extract_main_text_vinmec(html: str) -> str:
    """Hàm trích xuất nội dung chính từ HTML của Vinmec (từ Cell 4)."""
    soup = BeautifulSoup(html, 'lxml')
    # Xóa các thẻ không cần thiết
    for tag in soup(['script', 'style', 'noscript', 'header', 'footer', 'nav', 'aside', 'form', 'iframe']):
        tag.decompose()
    
    candidate = None
    possible_classes = ['body-content', 'container_body', 'article-body', 'post-content', 'main-content']
    for cls in possible_classes:
        found = soup.find('div', class_=re.compile(cls, re.I))
        if found and found.get_text(strip=True):
            candidate = found
            break
            
    if candidate is None: candidate = soup.find('article')
    if candidate is None: candidate = soup.body
    if candidate is None: return ""
    
    # Xóa các từ khóa không mong muốn
    unwanted_keywords = ['Xem thêm', 'Bài viết liên quan', 'Có thể bạn quan tâm', 'Đặt lịch khám']
    for kw in unwanted_keywords:
        for tag in candidate.find_all(string=re.compile(kw, re.I)):
            try: tag.parent.decompose()
            except: pass
            
    # Lấy nội dung từ các thẻ
    content_parts = []
    valid_tags = ['h1', 'h2', 'h3', 'h4', 'p', 'li']
    for elem in candidate.find_all(valid_tags):
        text = elem.get_text(separator=" ", strip=True)
        if not text: continue
        if len(text) < 20: continue
        text = re.sub(r'\\s+', ' ', text)
        content_parts.append(text)
        
    final_text = "\\n\\n".join(content_parts)
    return clean_text(final_text)

def extract_main_text(html: str) -> str:
    """Hàm bao bọc, có thể thay đổi logic nếu có nhiều trang khác nhau."""
    return extract_main_text_vinmec(html)

def fetch_url(url: str) -> str:
    """Tải nội dung HTML từ một URL."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQ_TIMEOUT)
        if r.status_code == 200 and r.text:
            return r.text
        return ""
    except Exception as e:
        print(f"Lỗi khi tải {url}: {e}")
        return ""

def crawl_urls(urls: List[str], n: int = None) -> List[Dict[str, Any]]:
    """Thực thi crawl danh sách URL."""
    out = []
    
    # Quyết định số lượng URL để crawl
    if n is None:
        urls_to_crawl = urls
        desc = f"Crawling tất cả {len(urls)} URL"
    else:
        urls_to_crawl = urls[:n]
        desc = f"Crawling {n} URL đầu tiên"
        
    for u in tqdm(urls_to_crawl, desc=desc):
        html = fetch_url(u)
        time.sleep(SLEEP_BETWEEN_REQ) # Nghỉ giữa các request
        
        if not html:
            out.append({"url": u, "content_text": "", "ok": False})
            continue
            
        text = extract_main_text(html)
        out.append({"url": u, "content_text": text, "ok": bool(text)})
    return out

# === PHẦN 3: HÀM MAIN THỰC THI ===

def main():
    print(f"Bắt đầu quá trình crawl...")
    
    # === BƯỚC 1: TẢI CSV ===
    # (Đây là phần thay đổi chính)
    if not os.path.exists(INPUT_CSV_PATH):
        print(f"LỖI: Không tìm thấy file '{INPUT_CSV_PATH}'.")
        print("Hãy đảm bảo file CSV nằm cùng thư mục với script này.")
        return

    try:
        df = pd.read_csv(INPUT_CSV_PATH)
        print(f"Tải thành công file '{INPUT_CSV_PATH}'. Tìm thấy {len(df)} hàng.")
    except Exception as e:
        print(f"LỖI khi đọc CSV: {e}")
        return
        
    # === BƯỚC 2: ĐỔI TÊN CỘT ===
    # (Quan trọng) Đổi tên 'URL' -> 'url'
    if 'URL' in df.columns:
        df.rename(columns={
            'URL': 'url',
            'Title': 'title',
            'Chuyên khoa': 'chuyen_khoa'
        }, inplace=True)
        print("Đã đổi tên cột 'URL' -> 'url' và 'Title' -> 'title'.")
    
    if 'url' not in df.columns:
        print("LỖI: Không tìm thấy cột 'url' hoặc 'URL' trong file CSV.")
        return
        
    # === BƯỚC 3: CHẠY CRAWL ===
    # (Logic từ Cell 7)
    urls = df["url"].dropna().astype(str).tolist()
    
    num_to_crawl = len(urls) if CRAWL_LIMIT is None else min(CRAWL_LIMIT, len(urls))
    estimated_time_sec = num_to_crawl * (SLEEP_BETWEEN_REQ + 0.3) # (ước tính thêm 0.3s xử lý/request)
    print(f"Chuẩn bị crawl {num_to_crawl} URL. Thời gian ước tính: {estimated_time_sec / 60:.1f} phút.")
    
    crawled_data = crawl_urls(urls, n=CRAWL_LIMIT)
    print(f"\nĐã crawl xong {len(crawled_data)} URL.")
    
    # === BƯỚC 4: LƯU KẾT QUẢ ===
    # (Logic từ Cell 12)
    crawl_df = pd.DataFrame(crawled_data)
    
    # Bổ sung: Gộp thêm 'title' và 'chuyen_khoa' vào file output
    # Lấy title/chuyen_khoa từ df gốc
    merged_df = crawl_df.merge(df[['url', 'title', 'chuyen_khoa']], on='url', how='left')
    
    merged_df.to_csv(OUTPUT_CSV_PATH, index=False)
    print(f"✅ Dữ liệu đã crawl được lưu tại: {OUTPUT_CSV_PATH}")

# === ĐIỂM BẮT ĐẦU CHẠY SCRIPT ===
if __name__ == "__main__":
    main()