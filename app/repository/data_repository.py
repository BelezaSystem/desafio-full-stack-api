from ..infra.database import get_database
import pandas as pd
from bson.json_util import ObjectId


class DataRepository:
    @staticmethod
    def insert_data(data: dict) -> str:
        db = get_database()["file"]
        inserted_id = db.insert_one(data).inserted_id
        return str(inserted_id)

    @staticmethod
    def insert_batch_data(data_list: list) -> list:
        db = get_database()["file"]
        result = db.insert_many(data_list)
        return result.inserted_ids

    @staticmethod
    def fetch_document_by_id(data_id: str) -> dict:
        db = get_database()
        collection = db["file"]
        document = collection.find_one({"_id": ObjectId(data_id)})

        if document:
            for key, value in document.items():
                if isinstance(value, float):
                    if value == float('inf') or value == float('-inf') or pd.isna(value):
                        document[key] = None
            document.pop('_id', None)
            return document

    @staticmethod
    def fetch_all_documents() -> list:
        db = get_database()
        collection = db["file"]
        documents = collection.find({})
        documents_list = []
        for document in documents:
            for key, value in document.items():
                if isinstance(value, float):
                    if value == float('inf') or value == float('-inf') or pd.isna(value):
                        document[key] = None
            document.pop('_id', None)
            documents_list.append(document)
        return documents_list

    @staticmethod
    def delete_document(data_id: str) -> int:
        db = get_database()
        collection = db["file"]
        delete_result = collection.delete_one({"_id": ObjectId(data_id)})
        return delete_result.deleted_count

    @staticmethod
    def update_document(data_id: str, update_data: dict) -> int:
        db = get_database()
        collection = db["file"]
        update_result = collection.update_one({"_id": ObjectId(data_id)}, {'$set': update_data})
        return update_result.modified_count
