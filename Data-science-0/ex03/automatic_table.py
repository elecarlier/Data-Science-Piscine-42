import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, text
import sqlalchemy
from dotenv import load_dotenv
import os

load_dotenv()


DB_NAME = 'piscineds'
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = 'localhost'
DB_PORT = '5332'

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

CSV_FOLDER = '../subject/customer'

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
			data.head(0).to_sql(tableName, engine, index=False, if_exists='replace', dtype=data_types)

			data.to_sql(tableName, engine, index=False, if_exists='append')
			print(f"Table {tableName} created with succes")
		else:
			print(f"Table {tableName} already exists")

		engine.dispose()
	except Exception as e:
		print("An error occured while connecting the database: ", e)


if __name__ == "__main__":
    for file in os.listdir(CSV_FOLDER):
        if file.endswith(".csv"):
            path = os.path.join(CSV_FOLDER, file)
            table_name = os.path.splitext(file)[0]
            load(path, table_name)
