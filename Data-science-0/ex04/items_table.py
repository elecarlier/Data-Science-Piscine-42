import pandas as pd
from sqlalchemy import create_engine, MetaData
import sqlalchemy
from dotenv import load_dotenv
import os
import os.path

load_dotenv()

CSV_PATH = '../subject/item/item.csv'
DB_NAME = 'piscineds'
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = 'localhost'
DB_PORT = '5332'
TABLE_NAME = os.path.splitext(os.path.basename(CSV_PATH))[0]

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def table_exists(engine, table_name):
    metadata = MetaData()
    metadata.reflect(bind=engine)
    return table_name in metadata.tables

def load(path, tableName):
    try:
        engine = create_engine(DATABASE_URL)
        print("Connection successful!")

        if not table_exists(engine, tableName):
            print(f"Table {tableName} doesn't exist, creating...")
            data = pd.read_csv(path)
            data = data.replace({"" : None})

            data_types = {
                "id": sqlalchemy.types.INTEGER(),
                "big_number": sqlalchemy.types.BIGINT(),
                "category_path": sqlalchemy.types.VARCHAR(length=255),
                "name": sqlalchemy.types.VARCHAR(length=100)
            }

            data.head(0).to_sql(tableName, engine, index=False, if_exists='replace', dtype=data_types)
            data.to_sql(tableName, engine, index=False, if_exists='append')
            print(f"Data inserted into table {tableName}")
        else:
            print(f"Table {tableName} already exists")

        engine.dispose()
    except Exception as e:
        print("An error occurred while connecting to the database:", e)

if __name__ == "__main__":
    load(CSV_PATH, TABLE_NAME)
