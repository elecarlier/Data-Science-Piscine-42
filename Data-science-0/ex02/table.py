import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
import sqlalchemy
from dotenv import load_dotenv
import os

load_dotenv()

CSV_PATH = '../subject/customer/data_2022_dec.csv'
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

			data_types = {
				"event_time": sqlalchemy.types.TIMESTAMP(),
				"event_type": sqlalchemy.types.VARCHAR(255),
				"product_id": sqlalchemy.types.INTEGER(),
				"price": sqlalchemy.types.FLOAT(),
				"user_id": sqlalchemy.types.BIGINT(),
				"user_session": sqlalchemy.types.UUID()
				}

			data.head(0).to_sql(tableName, engine, index=False, dtype=data_types)
			print(f"Table {tableName} created with succes")
		else:
			print(f"Table {tableName} already exists")

		engine.dispose()
	except Exception as e:
		print("An error occured while connecting the database: ", e)


if __name__ == "__main__":
    load(CSV_PATH, TABLE_NAME)
