from typing import List, Dict, Any
from pydantic import ValidationError, TypeAdapter

from datetime import datetime, timedelta
from database import db_mongo
from entity.ui_data import UIDataDocument, UIDataItem
from typing import Dict, Any, Optional

class UIDataService:
    """
    Service class to handle database operations for UI-ready data.
    """

    def create_ui_data_document(self, data_items: List[dict], collection_name: str = "ui_data") -> dict:
        """
        Validates a list of UI data items, wraps them in a UIDataDocument,
        and inserts the single document into the specified MongoDB collection.

        Args:
            data_items: A list of dictionaries, where each dictionary represents a company's UI data.
            collection_name: The name of the MongoDB collection to insert the document into.

        Returns:
            A dictionary with the inserted document's ID or a list of validation errors.
        """
        if db_mongo.db is None:
            raise ConnectionError("Database connection is not established.")

        collection = db_mongo.db[collection_name]

        try:
            # Create the main document which validates the nested items internally
            # Use TypeAdapter for robust list validation
            validated_items = TypeAdapter(List[UIDataItem]).validate_python(data_items)
            ui_document = UIDataDocument(data=validated_items)
            
            # Use the custom dumper to prepare the nested structure for MongoDB
            document_to_insert = ui_document.model_dump_for_db()
            
            result = collection.insert_one(document_to_insert)
            return {"inserted_id": str(result.inserted_id), "errors": []}
        except ValidationError as e:
            # Return a more standard error structure
            return {"inserted_id": None, "errors": e.json()}

    def get_latest_ui_data(self, target_date: datetime = None, collection_name: str = "ui_data") -> Optional[Dict[str, Any]]:
        """
        Fetches the most recent UI data document. If a date is provided, it fetches
        the latest document containing data for that specific date.

        Args:
            target_date: The specific date to filter the data items by. If None, gets the absolute latest document.
            collection_name: The name of the MongoDB collection to fetch from.

        Returns:
            A dictionary representing the latest UI data document, or None if not found.
        """
        if db_mongo.db is None:
            raise ConnectionError("Database connection is not established.")

        collection = db_mongo.db[collection_name]
        
        # The query to find matching documents.
        find_query = {}

        if target_date:
            # If a date is provided, build a query to find documents where at least one
            # item in the 'data' array has a 'news_time' within that day.
            start_of_day = target_date
            end_of_day = start_of_day + timedelta(days=1)
            find_query = {
                "data": {
                    "$elemMatch": {"news_time": {"$gte": start_of_day, "$lt": end_of_day}}
                }
            }

        # Find all matching documents, sort them by the parent timestamp, and return the latest one.
        cursor = collection.find(find_query, projection={'_id': 0}).sort("timestamp", -1).limit(1)
        
        # Return the first document from the cursor, or None if no documents were found.
        return next(cursor, None)