# basic_screen.py
# Quick impact screen -> basic_output.xlsx
import os
import re
import warnings
import openai
import pdfplumber
import io
import pandas as pd
from httpx import ReadTimeout, ConnectError
from datetime import datetime
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Union, Optional # Import to fix the type hint error


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
    "Company<TAB>Impact tag<TAB>≤500‑word summary<TAB>Price‑move range<TAB>≤20‑word rationale\n"
    "(Impact tag = STRONGLY POSITIVE / POSITIVE / NEUTRAL / NEGATIVE / STRONGLY NEGATIVE "
    "use 'N/A' if immaterial.)"
)

split_line = lambda l: l.split("\t") if l.count("\t") == 4 else re.split(r"\s*\|\s*", l)

def _extract_text(pdf_bytes: bytes, max_pages=5, max_chars=12_000) -> str:
    txt = ""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for i, p in enumerate(pdf.pages):
                if i >= max_pages or len(txt) > max_chars:
                    break
                txt += (p.extract_text() or "") + "\n"
    except Exception as e:
        print(f"Error extracting text from PDF bytes: {e}")
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

def _process_pdf_from_memory(pdf_row: pd.Series) ->  Optional[dict]:
    """
    Processes a single PDF from memory: extracts text, calls LLM for analysis,
    and returns a dictionary with the results.
    """
    text = _extract_text(pdf_row['pdf_content'])
    if not text or len(text) < 300:
        return None

    resp = _call_llm(PROMPT_SCREEN, text + "\nReturn one line only.")
    if not resp:
        return None

    parts = split_line(resp)
    if len(parts) != 5:
        return None

    company, imp_tag, summ, prng, rat = [p.strip() for p in parts]
    
    pdf_link = pdf_row['ATTACHMENTNAME']
    scrip_cd = str(pdf_row['SCRIP_CD'])

    return {"File": scrip_cd, "PDF_Link": pdf_link, "Company": company,
            "SCRIP_CD": scrip_cd, "Impact": imp_tag, "Summary": summ, "Price_Range": prng, "Rationale": rat}


def analyze_pdfs_from_dataframe(pdf_df: pd.DataFrame):
    """
    Analyzes all PDFs from a DataFrame, ranks them, and returns the result.
    """
    price_mid = lambda s: (lambda n: [float(x) for x in re.findall(r"-?\d+\.?\d*", s)])(s)
    impact_map = {"STRONGLY POSITIVE": 5, "BEAT": 5, "POSITIVE": 4, "NEUTRAL": 3, "MATCHED": 3,
                  "NEGATIVE": 2, "STRONGLY NEGATIVE": 1, "MISSED": 1}
    impact = lambda t: impact_map.get(t.upper(), 0)

    rows = []
    if pdf_df.empty:
        print("No PDFs to analyze in the DataFrame.")
        return pd.DataFrame()

    pdf_tasks = [row for _, row in pdf_df.iterrows()]

    print(f"Starting analysis of {len(pdf_tasks)} PDFs with 5 workers...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_file = {executor.submit(_process_pdf_from_memory, task): task['SCRIP_CD'] for task in pdf_tasks}
        for i, future in enumerate(as_completed(future_to_file), 1):
            result = future.result()
            print(f"Processed {i}/{len(pdf_tasks)}: SCRIP_CD {future_to_file[future]}")
            if result:
                rows.append(result)

    if not rows:
        print("No valid PDFs were processed.")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    df["Impact_Score"] = df["Impact"].apply(impact)
    df["Mid_%"] = df["Price_Range"].apply(lambda r: sum(price_mid(r)) / len(price_mid(r)) if price_mid(r) else 0)

    # Filter for rows where the mid-point percentage is greater than 0.
    df = df[df["Mid_%"] > 0].copy()
    
    df.sort_values(["Impact_Score", "Mid_%"], ascending=[False, False], inplace=True)
    # The 'SCRIP_CD' column is already populated from _process_pdf. Let's ensure it's a string for merging.
    df['SCRIP_CD'] = df['SCRIP_CD'].astype(str)
    # df = df.drop_duplicates(subset="Company", keep="first")
    df.reset_index(drop=True, inplace=True)
    df.insert(0, "Rank", df.index + 1)
    print(f"Analysis complete. {len(df)} results ranked.")
    return df