# basic_screen.py
# Quick impact screen -> basic_output.xlsx
import os
import re
import warnings
import openai
import pdfplumber
import pandas as pd
from httpx import ReadTimeout, ConnectError
from datetime import datetime
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore", message=r"Cannot set gray non-stroke color")

load_dotenv()  # Load environment variables from .env file

API_KEY = os.environ.get("OPENAI_API_KEY")
if not API_KEY:
    raise ValueError("OPENAI_API_KEY not found. Please set it in your .env file.")

MODEL_SCREEN = "gpt-4.1-mini"
BASE_URL = "https://www.bseindia.com/xml-data/corpfiling/AttachLive/"  # for PDF_Link
client = openai.OpenAI(api_key=API_KEY)
PROMPT_SCREEN = (
    "Return ONE tab‑separated line:\n"
    "Company<TAB>Impact tag<TAB>≤30‑word summary<TAB>Price‑move range<TAB>≤20‑word rationale\n"
    "(Impact tag = STRONGLY POSITIVE / POSITIVE / NEUTRAL / NEGATIVE / STRONGLY NEGATIVE "
    "use 'N/A' if immaterial.)"
)

split_line = lambda l: l.split("\t") if l.count("\t") == 4 else re.split(r"\s*\|\s*", l)

def _extract_text(path, max_pages=5, max_chars=12_000):
    txt = ""
    try:
        with pdfplumber.open(path) as pdf:
            for i, p in enumerate(pdf.pages):
                if i >= max_pages or len(txt) > max_chars:
                    break
                txt += (p.extract_text() or "") + "\n"
    except Exception as e:
        print(f"Error extracting text from {path}: {e}")
        return ""  # Return empty string if extraction fails
    return txt[:max_chars]

def _call_llm(prompt, user, retries=3):
    for a in range(retries):
        try:
            return client.chat.completions.create(
                model=MODEL_SCREEN,
                messages=[{"role": "system", "content": prompt},
                          {"role": "user",   "content": user}],
                max_tokens=400,
                # max_completion_tokens=1200, # for o3
                temperature=0.3,
            ).choices[0].message.content.strip()
        except (openai.APIConnectionError, ReadTimeout, ConnectError) as e:
            if a == retries - 1:
                raise
            print(f"Error during OpenAI API call: {e}")
            return None # Return None if API call fails
        except Exception as e:
            print(f"Unexpected error during llm call: {e}")
            return None

def _process_pdf(file_path: str) -> dict | None:
    """
    Processes a single PDF file: extracts text, calls LLM for analysis,
    and returns a dictionary with the results.
    """
    text = _extract_text(file_path)
    if not text or len(text) < 300:
        return None

    resp = _call_llm(PROMPT_SCREEN, text + "\nReturn one line only.")
    if not resp:
        return None

    parts = split_line(resp)
    if len(parts) != 5:
        return None

    company, imp_tag, summ, prng, rat = [p.strip() for p in parts]
    file_name = os.path.basename(file_path)

    # Extract SCRIP_CD from filename like "(123456)_filename.pdf"
    scrip_cd_match = re.search(r'^\((\d+)\)', file_name)
    scrip_cd = scrip_cd_match.group(1) if scrip_cd_match else "N/A"

    return {"File": file_name, "PDF_Link": BASE_URL + file_name, "Company": company,
            "SCRIP_CD": scrip_cd, "Impact": imp_tag, "Summary": summ, "Price_Range": prng, "Rationale": rat}


def analyze_and_rank_pdfs(pdf_folder_path: str, output_file_path: str):
    """
    Analyzes all PDFs in a folder, ranks them based on AI-driven impact
    assessment, saves the result to an Excel file, and returns the DataFrame.
    """
    price_mid = lambda s: (lambda n: [float(x) for x in re.findall(r"-?\d+\.?\d*", s)])(s)
    impact_map = {"STRONGLY POSITIVE": 5, "BEAT": 5, "POSITIVE": 4, "NEUTRAL": 3, "MATCHED": 3,
                  "NEGATIVE": 2, "STRONGLY NEGATIVE": 1, "MISSED": 1}
    impact = lambda t: impact_map.get(t.upper(), 0)

    rows = []
    if not os.path.isdir(pdf_folder_path):
        print(f"Error: PDF folder not found at {pdf_folder_path}")
        return None

    pdf_files = [
        os.path.join(pdf_folder_path, f)
        for f in os.listdir(pdf_folder_path)
        if f.lower().endswith(".pdf")
    ]

    if not pdf_files:
        print("No PDF files found in the specified folder.")
        return pd.DataFrame()

    print(f"Starting analysis of {len(pdf_files)} PDFs with 5 workers...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_file = {executor.submit(_process_pdf, pdf_path): pdf_path for pdf_path in pdf_files}
        for i, future in enumerate(as_completed(future_to_file), 1):
            result = future.result()
            print(f"Processed {i}/{len(pdf_files)}: {os.path.basename(future_to_file[future])}")
            if result:
                rows.append(result)

    if not rows:
        print("No valid PDFs were processed.")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Filter for positive impact tags only, as requested.
    # positive_impact_tags = ["STRONGLY POSITIVE", "BEAT", "POSITIVE"]
    # df = df[df["Impact"].str.upper().isin(positive_impact_tags)]

    df["Impact_Score"] = df["Impact"].apply(impact)
    df["Mid_%"] = df["Price_Range"].apply(lambda r: sum(price_mid(r)) / len(price_mid(r)) if price_mid(r) else 0)
    # Extract the numeric SCRIP_CD from the filename and overwrite the 'File' column.
    df['File'] = df['File'].str.extract(r'^\((\d+)\)').fillna('N/A')

    # Filter for rows where the mid-point percentage is greater than 0.
    df = df[df["Mid_%"] > 0].copy()
    
    df.sort_values(["Impact_Score", "Mid_%"], ascending=[False, False], inplace=True)
    # df = df.drop_duplicates(subset="Company", keep="first")
    df.reset_index(drop=True, inplace=True)
    df.insert(0, "Rank", df.index + 1)
    df.to_excel(output_file_path, index=False, sheet_name="Screened_Ranked")
    print(f":white_check_mark: Analysis saved to: {output_file_path}")
    return df

if __name__ == "__main__":
    date_str = datetime.now().strftime('%Y-%m-%d')
    pdf_folder = f"./reports"
    output_file = f"./output/summary_price_jump_{date_str}.xlsx"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    analyze_and_rank_pdfs(pdf_folder, output_file)