import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

def get_mongo_client():
    return MongoClient(
        host="mongodb",
        port=27017,
        username=os.getenv("MONGO_USER"),
        password=os.getenv("MONGO_PASSWORD"),
        authSource="admin" 
    )

def drop_database():
    client = get_mongo_client()
    db_name = 'big_data_db'
    
    print(f"Dropping db: {db_name}...")
    client.drop_database(db_name)
    print("Database dropped successfully.")

if __name__ == "__main__":
    drop_database()