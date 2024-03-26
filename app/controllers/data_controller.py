from typing import Dict

from ..services.data_service import DataService
from fastapi import UploadFile, File


class DataController:
    @staticmethod
    async def add_data(file: UploadFile = File(...)) -> dict:
        content = await file.read()
        print(f'FILE CONTROLLER::{content}')
        return await DataService.add_data(content)

    @staticmethod
    def get_document_by_id(data_id: str) -> dict:
        return DataService.get_document_by_id(data_id)

    @staticmethod
    def get_documents() -> Dict[str, dict]:
        return DataService.get_documents()

    @staticmethod
    def delete_data(data_id: str) -> dict:
        return DataService.delete_data(data_id)

    @staticmethod
    async def update_data(data_id: str, file: UploadFile = File(...)) -> dict:
        content = await file.read()
        return await DataService.update_data(data_id, content)
