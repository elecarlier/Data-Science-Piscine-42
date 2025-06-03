from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Charger les variables d'environnement
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
        with engine.connect() as conn:
            print("✅ Connected to DB.")

            # Créer une table temporaire sans doublons
            print("🔄 Removing duplicates...")
            conn.execute(text("""
                CREATE TABLE temp_customers AS
                SELECT DISTINCT ON (
                    user_id,
                    event_type,
                    product_id,
                    user_session,
                    date_trunc('second', event_time)
                ) *
                FROM customers
                ORDER BY user_id, event_type, product_id, user_session, event_time;
            """))

            # Remplacer l’ancienne table
            conn.execute(text("DROP TABLE customers;"))
            conn.execute(text("ALTER TABLE temp_customers RENAME TO customers;"))

            print("✅ Duplicate rows removed and 'customers' table updated.")

    except Exception as e:
        print("❌ Error while removing duplicates:", e)

if __name__ == "__main__":
    remove_duplicates()
