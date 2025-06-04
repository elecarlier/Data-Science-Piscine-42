import pandas as pd
from sqlalchemy import create_engine, MetaData, text
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


def load_item_table(path, tableName):
    try:
        engine = create_engine(DATABASE_URL)
        print("Connection successful!")

        if not table_exists(engine, tableName):
            print(f"Table {tableName} doesn't exist, creating...")
            data = pd.read_csv(path)
            data = data.replace({"" : None})

            data_types = {
                "product_id": sqlalchemy.types.INTEGER(),
                "category_id": sqlalchemy.types.BIGINT(),
                "category_code": sqlalchemy.types.VARCHAR(length=255),
                "brand": sqlalchemy.types.VARCHAR(length=100)
            }

            data.head(0).to_sql(tableName, engine, index=False, if_exists='replace', dtype=data_types)
            data.to_sql(tableName, engine, index=False, if_exists='append')
            print(f"Data inserted into table {tableName}")
        else:
            print(f"Table {tableName} already exists")

        engine.dispose()
    except Exception as e:
        print("An error occurred while connecting to the database:", e)



def clean_item_table():
    try:

        engine = create_engine(DATABASE_URL)
        with engine.begin() as conn:
            print("Creating clean_items table (deduplicated)...")
            conn.execute(text("DROP TABLE IF EXISTS clean_items;"))

            conn.execute(text("""
                CREATE TABLE clean_items AS
                SELECT
                    product_id,
                    MAX(category_id) AS category_id,
                    MAX(category_code) AS category_code,
                    MAX(brand) AS brand
                FROM item
                GROUP BY product_id;
            """))

            print("Table clean_items created.")
    except Exception as e:
        print("Error while cleaning item table:", e)


def fusion():
    try:
        engine = create_engine(DATABASE_URL)
        with engine.begin() as conn:
            print("Adding enrichment columns to customers...")
            conn.execute(text("""
                ALTER TABLE customers
                ADD COLUMN IF NOT EXISTS category_id BIGINT,
                ADD COLUMN IF NOT EXISTS category_code VARCHAR(255),
                ADD COLUMN IF NOT EXISTS brand VARCHAR(255);
            """))

            print("Updating customer table with item metadata...")
            conn.execute(text("""
                UPDATE customers c
                SET
                    category_id = i.category_id,
                    category_code = i.category_code,
                    brand = i.brand
                FROM clean_items i
                WHERE c.product_id = i.product_id;
            """))

            print("Fusion successful.")

    except Exception as e:
        print("Error while fusioning tables:", e)



if __name__ == "__main__":
    load_item_table(CSV_PATH, TABLE_NAME)
    clean_item_table()
    fusion()
