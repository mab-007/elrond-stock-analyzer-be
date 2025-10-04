import os
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.bseindia.com/",  # <- *critical*
}

def _download_pdf(url: str, pdf_dir: str, scrip_cd: str) -> str:
    """Stream one PDF; return local file path or error string."""
    original_fname = url.split("/")[-1]
    new_fname = f"({scrip_cd})_{original_fname}"
    try:
        fname = os.path.join(pdf_dir, new_fname)
        with requests.get(url, headers=HEADERS, stream=True, timeout=300, verify=False) as r:
            r.raise_for_status()  # 4xx/5xx → exception
            with open(fname, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
        return fname
    except Exception as exc:
        return f"FAIL: {url} ({exc})"

def download_announcement_pdfs(input_csv_path: str, output_pdf_dir: str):
    """
    Downloads all PDF announcements listed in a CSV file.
    """
    try:
        df_announcements = pd.read_csv(input_csv_path)
    except FileNotFoundError:
        print(f"Error: Input file not found at {input_csv_path}")
        return

    os.makedirs(output_pdf_dir, exist_ok=True)

    # Create a list of tuples (url, scrip_cd) for valid rows
    download_tasks = []
    for _, row in df_announcements.iterrows():
        url = row.get("ATTACHMENTNAME")
        scrip_cd = row.get("SCRIP_CD")
        is_new_entry = row.get("is_new_entry", True)
        if is_new_entry and pd.notnull(url) and pd.notnull(scrip_cd) and str(url).startswith("http"):
            download_tasks.append((url, str(scrip_cd)))

    if not download_tasks:
        print("No valid URLs with corresponding SCRIP_CD found in the input file.")
        return

    num_workers = min(20, max(5, len(download_tasks) // 4))
    print(f"Starting PDF download with {num_workers} workers for {len(download_tasks)} URLs.")

    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        futures = {pool.submit(_download_pdf, url, output_pdf_dir, scrip_cd): url for url, scrip_cd in download_tasks}
        for i, fut in enumerate(as_completed(futures), 1):
            res = fut.result()
            print(f"{i}/{len(download_tasks)} → {res}")

if __name__ == "__main__":
    date_str = datetime.now().strftime('%Y-%m-%d')
    input_csv = f"./bse_announcements/filtered_announcements_{date_str}.csv"
    output_dir = "./reports"
    download_announcement_pdfs(input_csv, output_dir)
