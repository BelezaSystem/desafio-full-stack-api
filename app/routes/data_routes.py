from fastapi import APIRouter
from ..controllers.data_controller import DataController
from fastapi import File, UploadFile

router = APIRouter()


@router.post("/add-file")
async def add_data(file: UploadFile = File(...)):
    return await DataController.add_data(file)


@router.get("/get-file/{data_id}")
async def get_document(data_id: str):
    return DataController.get_document_by_id(data_id)


@router.get("/get-files")
async def get_documents():
    return DataController.get_documents()


@router.put("/update-file/{data_id}")
async def update_data(data_id: str, file: UploadFile = File(...)):
    return await DataController.update_data(data_id, file)


@router.delete("/delete-file/{data_id}")
async def delete_data(data_id: str):
    return DataController.delete_data(data_id)
