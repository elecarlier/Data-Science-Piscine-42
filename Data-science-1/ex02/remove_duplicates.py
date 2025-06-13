from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

DB_NAME = 'piscineds'
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = 'localhost'
DB_PORT = '5332'

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def remove_duplicates():
    try:
        engine = create_engine(DATABASE_URL)
        with engine.begin() as conn:
            print("Connected to DB.")
            print("Removing duplicates within 1 second interval...")

            conn.execute(text("""
                WITH marked AS (
                    SELECT
                      ctid,
                      event_time,
                      event_type,
                      product_id,
                      price,
                      user_id,
                      user_session,
                      LAG(event_time) OVER (
                        PARTITION BY event_type, product_id, price, user_id, user_session
                        ORDER BY event_time
                      ) AS prev_time
                    FROM customers
                ),
                duplicates AS (
                    SELECT ctid
                    FROM marked
                    WHERE prev_time IS NOT NULL
                      AND EXTRACT(EPOCH FROM (event_time - prev_time)) <= 1
                )
                DELETE FROM customers
                WHERE ctid IN (
                    SELECT ctid FROM duplicates
                );
            """))

            print("Duplicates removed based on 1-second threshold.")

            count = conn.execute(text("SELECT COUNT(*) FROM customers")).scalar()
            print(f"Duplicates removed successfully. Table now has {count} rows.")

    except Exception as e:
        print("Error while removing duplicates:", e)

if __name__ == "__main__":
    remove_duplicates()

