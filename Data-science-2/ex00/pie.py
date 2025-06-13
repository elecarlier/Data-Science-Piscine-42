import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

DB_NAME = 'piscineds'
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = 'localhost'
DB_PORT = '5332'

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def create_piechart():
    try:
        engine = create_engine(DATABASE_URL)
        with engine.begin() as conn:
            print("Connected to DB.")
            df = pd.read_sql("SELECT event_type FROM customers", conn)
            event_counts = df['event_type'].value_counts()
            plt.figure(figsize=(8, 8))
            plt.pie(event_counts, labels=event_counts.index, autopct='%1.1f%%', startangle=180)
            plt.title("Distribution of Event Types")
            plt.axis('equal')
            plt.show()

    except Exception as e:
        print("Error while creating the chart:", e)

if __name__ == "__main__":
    create_piechart()
