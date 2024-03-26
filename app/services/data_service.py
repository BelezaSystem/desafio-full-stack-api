from fastapi import HTTPException
from ..utils.split_df import split_df
from ..utils.logger import get_logger
from ..repository.data_repository import DataRepository
from ..utils.create_dict import csv_to_dict
import pandas as pd
from io import BytesIO

logger = get_logger(__name__)


class DataService:

    @staticmethod
    async def add_data(file: bytes) -> dict:
        print(f'file::: {file}')
        try:
            df_csv = pd.read_csv(BytesIO(file), sep=";", encoding='latin-1')
            df_csv.fillna('', inplace=True)
            batch_size = 15000

            if len(df_csv) > batch_size:
                list_ids = []
                df_list = split_df(df_csv, batch_size)
                for df in df_list:
                    dictionary = csv_to_dict(df)
                    batch_data_ids = DataRepository.insert_batch_data(dictionary)
                    if not batch_data_ids:
                        logger.error(f"Error adding batch files")
                        raise HTTPException(status_code=400,
                                            detail={"message": "Error adding batch files", "status_code": 400})
                    list_ids.extend(batch_data_ids)
                    logger.info(f"Batch files added successfully with data_ids: {batch_data_ids}")

                return {
                    "detail": {
                        "message": "Batch files added successfully",
                        "data_ids": list_ids,
                        "status_code": 201
                    }
                }

            dictionary = csv_to_dict(df_csv)
            data_id = DataRepository.insert_data(dictionary)
            if data_id is None:
                logger.error(f"Error adding file")
                raise HTTPException(status_code=400, detail={"message": "Error adding file", "status_code": 400})
            logger.info(f"File added successfully with data_id: {data_id}")

            return {
                "detail": {
                    "message": "File added successfully",
                    "data_id": data_id,
                    "status_code": 201
                }
            }

        except HTTPException as http_exc:
            raise http_exc

    @staticmethod
    def get_document_by_id(data_id: str) -> dict:
        try:
            logger.info(f"Fetching document with data_id: {data_id}")
            document = DataRepository.fetch_document_by_id(data_id)
            if document is None:
                logger.error(f"Error fetching document with data_id: {data_id}")
                raise HTTPException(status_code=404, detail={"message": "File not found", "status_code": 404})
            return {
                "detail": {
                    "message": "Document fetched successfully",
                    "data": document,
                    "status_code": 200
                }
            }
        except HTTPException as http_exc:
            raise http_exc

    @staticmethod
    def get_documents() -> dict:
        try:
            logger.info(f"Fetching all documents")
            documents = DataRepository.fetch_all_documents()
            if not documents:
                logger.error(f"Error fetching documents")
                raise HTTPException(status_code=404, detail={"message": "Files not found", "status_code": 404})
            return {
                "detail": {
                    "message": "Documents fetched successfully",
                    "data": documents,
                    "status_code": 200
                }
            }
        except HTTPException as http_exc:
            raise http_exc

    @staticmethod
    def delete_data(data_id: str) -> dict:
        try:
            deleted_count = DataRepository.delete_document(data_id)
            if deleted_count == 0:
                logger.error(f"Error deleting file: File not found with data_id: {data_id}")
                raise HTTPException(status_code=404, detail={"message": "File not found", "status_code": 404})
            logger.info(f"File deleted successfully with data_id: {data_id}")
            return {
                "detail": {
                    "message": "File deleted successfully",
                    "data_id": data_id,
                    "status_code": 200
                }
            }
        except HTTPException as http_exc:
            raise http_exc

    @staticmethod
    async def update_data(data_id: str, file: bytes) -> dict:
        try:
            if isinstance(file, pd.DataFrame):
                df_csv = file
            else:
                df_csv = pd.read_csv(BytesIO(file), sep=";", encoding='latin-1')
            if df_csv.empty:
                logger.error(f"Error updating file: Empty file")
                raise HTTPException(status_code=400,
                                    detail={"message": "Error updating file: Empty file", "status_code": 400})
            df_csv.fillna('', inplace=True)
            old_file_data = DataRepository.fetch_document_by_id(data_id)
            if old_file_data is None:
                logger.error(f"Error updating file: File not found with data_id: {data_id}")
                raise HTTPException(status_code=404, detail={"message": "File not found", "status_code": 404})
            old_file_df = pd.DataFrame(old_file_data)
            new_file_df = pd.concat([old_file_df, df_csv], axis=0, ignore_index=True)
            new_file_data = csv_to_dict(new_file_df)
            updated_count = DataRepository.update_document(data_id, new_file_data)
            if updated_count == 0:
                logger.error(f"Error updating file: File not found with data_id: {data_id}")
                raise HTTPException(status_code=404, detail={"message": "File not found", "status_code": 404})
            logger.info(f"File updated successfully with data_id: {data_id}")
            return {
                "detail": {
                    "message": "File updated successfully",
                    "data_id": data_id,
                    "status_code": 200
                }
            }
        except HTTPException as http_exc:
            raise http_exc
