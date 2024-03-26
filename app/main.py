from dotenv import load_dotenv
from fastapi import FastAPI
from app.routes.data_routes import router as data_router

load_dotenv()

app = FastAPI()

app.include_router(data_router, prefix="/data", tags=["Desafio Full Stack - API"])

