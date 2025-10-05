import os
import pandas as pd
from datetime import datetime
from fastapi import FastAPI, Query, HTTPException

from urllib.parse import unquote
# Import the refactored functions
from announcements import fetch_and_filter_announcements
from data_to_pdf import download_announcement_pdfs
from results import analyze_and_rank_pdfs

app = FastAPI(
    title="BSE Announcements Analyzer API",
    description="Triggers a pipeline to fetch, filter, download, and analyze BSE announcements.",
)

@app.post("/analyze-announcements/", summary="Run the full analysis pipeline")
def run_analysis_pipeline(
    date: str = Query(..., description="Target date in YYYY-MM-DD format.", regex=r"^\d{4}-\d{2}-\d{2}$"),
    cut_off_time: str = Query("20:30:00", description="Cut-off time in HH:MM:SS format."),
    market_cap_st: int = Query(2500, description="Start of market cap range (in Crores)."),
    market_cap_end: int = Query(25000, description="End of market cap range (in Crores).")
):
    """
    Exposes the main workflow from index.py as an API endpoint.
    """
    try:
        target_date = datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Please use YYYY-MM-DD.")

    # Decode the cut_off_time string to handle URL-encoded characters like '%3A' for colons.
    decoded_cut_off_time = unquote(cut_off_time)

    # Define file paths based on the input date
    filtered_csv_path = f"./bse_announcements/filtered_announcements_{date}.csv"
    pdf_folder = f"./reports/reports_{date}"
    output_excel_file = f"./output/summary_price_jump_{date}.xlsx"

    # # --- Step 1: Fetch and Filter Announcements ---
    filtered_df = fetch_and_filter_announcements(
        target_date=target_date,
        market_cap_start=market_cap_st,
        market_cap_end=market_cap_end,
        cut_off_time_str=decoded_cut_off_time
    )
    if filtered_df.empty:
        return {"message": "No announcements found matching the criteria."}

    # --- Step 2: Download PDFs ---
    download_announcement_pdfs(filtered_df=filtered_df, output_pdf_dir=pdf_folder)

    # --- Step 3: Analyze PDFs and Rank ---
    os.makedirs(os.path.dirname(output_excel_file), exist_ok=True)
    # The function now needs to return the dataframe for the API response
    ranked_df = analyze_and_rank_pdfs(filtered_df=filtered_df, pdf_folder_path=pdf_folder, output_file_path=output_excel_file)

    if ranked_df is None or ranked_df.empty:
        return {"message": "PDFs were downloaded, but analysis yielded no results."}

    # --- Step 4: Return the result ---
    return ranked_df.to_dict(orient="records")