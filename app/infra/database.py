from pymongo import MongoClient
from ..config.settings import Settings

settings = Settings()


def get_database():
    client = MongoClient(settings.mongodb_url)
    return client.get_default_database()
