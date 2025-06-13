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

x_labels = ["Oct", "Nov", "Dec", "Jan", "Feb"]

#plot
def create_chart_clients():
    try:
        engine = create_engine(DATABASE_URL)
        with engine.begin() as conn:
            print("Connected to DB.")
			#plt.title("")
			plt.figure(figsize=(10, 6))
			plt.ylabel('Number of customers')
			plt.xticks(x, x_labels)
			plt.grid()
            plt.show()

    except Exception as e:
        print("Error while creating the clients chart:", e)

#bar
def create_chart_sales():
    try:
        engine = create_engine(DATABASE_URL)
        with engine.begin() as conn:
            print("Connected to DB.")
			plt.figure(figsize=(10, 6))
			#plt.title("")
			plt.ylabel('total sales in million of Altairian Dollars')
			#plt.xticks(x, x_labels)
			plt.grid(axis='y')
            plt.show()

    except Exception as e:
        print("Error while creating the sales chart:", e)

#plot
def create_chart_averageSales():
    try:
        engine = create_engine(DATABASE_URL)
        with engine.begin() as conn:
            print("Connected to DB.")
			plt.figure(figsize=(10, 6))
			#plt.title("")
			plt.ylabel('Average spend/customers in Altairian Dollars')
			#plt.xticks(x, x_labels)
			plt.grid()
            plt.show()

    except Exception as e:
        print("Error while creating the average sales chart:", e)


if __name__ == "__main__":

