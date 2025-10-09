import pandas as pd
from typing import List
from datetime import datetime, timedelta
from pydantic import ValidationError

from database import db_mongo
from entity.filtered_announcements import FilteredAnnouncement
from entity.prediction import Prediction

class AnnouncementService:
    """
    Service class to handle all database operations related to announcements.
    """

    def get_predictions_by_date(self, target_date: datetime) -> pd.DataFrame:
        """
        Fetches prediction records from MongoDB for a specific date.

        Args:
            target_date: The date for which to fetch predictions.

        Returns:
            A pandas DataFrame containing the fetched predictions.
            Returns an empty DataFrame if no records are found.
        """
        if db_mongo.db is None:
            raise ConnectionError("Database connection is not established.")

        collection_name = "predictions"
        collection = db_mongo.db[collection_name]

        # Define the date range for the query.
        # target_date is already at the beginning of the day (00:00:00).
        start_of_day = target_date
        end_of_day = start_of_day + timedelta(days=1)

        # Construct the query to find documents where News_submission_dt is within the target day.
        query = {"News_submission_dt": {"$gte": start_of_day, "$lt": end_of_day}}
        
        predictions_cursor = collection.find(query, {'_id': 0})
        
        # Convert cursor to a list of dictionaries, then to a DataFrame
        return pd.DataFrame(list(predictions_cursor))

    def create_predictions(self, predictions_df: pd.DataFrame, collection_name: str) -> dict:
        """
        Validates a DataFrame of predictions against the Prediction Pydantic model
        and inserts them into the specified MongoDB collection.

        Args:
            predictions_df: A pandas DataFrame containing the final analysis results.
            collection_name: The name of the MongoDB collection to insert data into.

        Returns:
            A dictionary with the count of successful insertions and a list of validation errors.
        """
        if db_mongo.db is None:
            raise ConnectionError("Database connection is not established.")

        collection = db_mongo.db[collection_name]
        
        valid_records = []
        errors = []
        for _, row in predictions_df.iterrows():
            try:
                # Convert row to dict and handle NaN values for Pydantic validation
                record_data = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
                validated_prediction = Prediction.model_validate(record_data)
                valid_records.append(validated_prediction.model_dump(by_alias=True))
            except ValidationError as e:
                errors.append({"record": row.to_dict(), "error": str(e)})

        if not valid_records:
            return {"inserted_count": 0, "errors": errors}
            
        result = collection.insert_many(valid_records)
        return {"inserted_count": len(result.inserted_ids), "errors": errors}

    def create_announcements(self, announcements_df: pd.DataFrame, collection_name: str) -> dict:
        """
        Validates a DataFrame of announcements against the FilteredAnnouncement Pydantic
        model and inserts them into the specified MongoDB collection.

        Args:
            announcements_df: A pandas DataFrame containing the announcements to be saved.
            collection_name: The name of the MongoDB collection to insert data into.

        Returns:
            A dictionary containing the count of successful insertions and a list of validation errors.
        """
        if db_mongo.db is None:
            raise ConnectionError("Database connection is not established.")

        collection = db_mongo.db[collection_name]
        
        valid_records = []
        errors = []
        for _, row in announcements_df.iterrows():
            try:
                announcement_data = row.to_dict()
                # Pydantic expects None for Optional fields, not NaN
                for key, value in announcement_data.items():
                    if pd.isna(value):
                        announcement_data[key] = None
                validated_announcement = FilteredAnnouncement(**announcement_data)
                valid_records.append(validated_announcement.model_dump(by_alias=True)) # Use model_dump for Pydantic v2
            except ValidationError as e:
                errors.append({"record": row.to_dict(), "error": str(e)})
            except Exception as e:
                errors.append({"record": row.to_dict(), "error": f"Unexpected error: {str(e)}"})

        if not valid_records:
            return {"inserted_count": 0, "errors": errors}
            
        result = collection.insert_many(valid_records)
        
        return {"inserted_count": len(result.inserted_ids), "errors": errors}

    def get_latest_announcements(self) -> dict | None:
        """
        Fetches the single most recent prediction from the 'predictions' collection
        based on the 'News_submission_dt' field.

        Returns:
            A dictionary representing the latest prediction, or None if no predictions are found.
        """
        if db_mongo.db is None:
            raise ConnectionError("Database connection is not established.")

        collection = db_mongo.db["raw_bse_announcements"]

        # Sort by 'News_submission_dt' in descending order and get the first document.
        # The -1 indicates descending order.
        latest_prediction = collection.find_one(
            {},
            sort=[("News_submission_dt", -1)],
            projection={'_id': 0}  # Exclude the MongoDB internal '_id'
        )

        return latest_prediction