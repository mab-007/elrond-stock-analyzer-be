import os
import pandas as pd
from datetime import datetime
from fastapi import FastAPI, Query, Path, HTTPException
from contextlib import asynccontextmanager

from urllib.parse import unquote
# Import the refactored functions
from announcements import fetch_and_filter_announcements
from data_to_pdf import download_pdfs_to_dataframe
from results import analyze_pdfs_from_dataframe
from database import connect_to_mongo, close_mongo_connection
from service.announcement_service import AnnouncementService

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Code to run on startup
    connect_to_mongo()
    yield
    # Code to run on shutdown
    close_mongo_connection()

app = FastAPI(
    title="BSE Announcements Analyzer API",
    description="Triggers a pipeline to fetch, filter, download, and analyze BSE announcements.",
    lifespan=lifespan,
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
    
    filtered_df = fetch_and_filter_announcements(
        target_date=target_date,
        market_cap_start=market_cap_st,
        market_cap_end=market_cap_end,
        cut_off_time_str=decoded_cut_off_time
    )
    if filtered_df.empty:
        return {"message": "No announcements found matching the criteria."}

    # --- Step 2: Download PDFs ---
    pdf_df = download_pdfs_to_dataframe(filtered_df)
    if pdf_df.empty:
        return {"message": "Announcements were found, but no PDFs could be downloaded."}

    # --- Step 3: Analyze PDFs and Rank ---
    # The function now reads from the dataframe with PDF content
    ranked_df = analyze_pdfs_from_dataframe(pdf_df)

    if ranked_df is None or ranked_df.empty:
        return {"message": "PDFs were downloaded, but analysis yielded no results."}

    # --- Step 4: Merge with original data to add News_submission_dt ---
    # Ensure SCRIP_CD in filtered_df is a string for a clean merge.
    # The BSE API can return it as a number.
    pdf_df['SCRIP_CD'] = pdf_df['SCRIP_CD'].astype(str)

    # Select only the columns needed for the merge to avoid duplicates and keep the merge clean.
    merge_cols = pdf_df[['SCRIP_CD', 'News_submission_dt']].drop_duplicates(subset=['SCRIP_CD'])

    # Merge the ranked analysis with the submission date using a left join.
    final_df = pd.merge(ranked_df, merge_cols, on='SCRIP_CD', how='left')

    # --- Step 5: Store predictions in MongoDB ---
    announcement_service = AnnouncementService()
    collection_name = f"predictions"
    db_result = announcement_service.create_predictions(final_df, collection_name)
    print(f"MongoDB insertion result for predictions: {db_result}")

    # --- Step 6: Return the enriched result ---
    return final_df.to_dict(orient="records")


@app.get("/predictions/{date}", summary="Fetch predictions by date")
def get_predictions(
    date: str = Path(..., description="Target date in YYYY-MM-DD format.", regex=r"^\d{4}-\d{2}-\d{2}$")
):
    """
    Retrieves the stored prediction results for a given date from MongoDB.
    """
    try:
        target_date = datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Please use YYYY-MM-DD.")

    announcement_service = AnnouncementService()
    try:
        predictions_df = announcement_service.get_predictions_by_date(target_date)
    except ConnectionError as e:
        raise HTTPException(status_code=500, detail=str(e))

    if predictions_df.empty:
        return {"message": f"No predictions found for date {date}."}

    # Convert DataFrame to a list of dictionaries (JSON array) and return
    return predictions_df.to_dict(orient="records")


@app.get("/announcements/latest", summary="Fetch the latest raw announcement")
def get_latest_announcement():
    """
    Retrieves the single most recent raw announcement from the 'raw_bse_announcements'
    collection in MongoDB, sorted by submission time.
    """
    announcement_service = AnnouncementService()
    try:
        latest_announcement = announcement_service.get_latest_announcements()
    except ConnectionError as e:
        raise HTTPException(status_code=500, detail=str(e))

    if not latest_announcement:
        return {"message": "No announcements found in the database."}

    return latest_announcement