from fastapi import FastAPI
from pymongo import MongoClient

app = FastAPI()
client = MongoClient("mongodb://localhost:27017/")
db = client["mydatabase"]

@app.get("/")
async def root():
    return {"message": "Hello World"}