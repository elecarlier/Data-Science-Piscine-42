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

def fetch_data():
    engine = create_engine(DATABASE_URL)
    query = """
        SELECT event_time, price, user_id
        FROM customers
        WHERE event_type = 'purchase'
          AND event_time >= '2022-10-01'
          AND event_time < '2023-03-01'
    """
    df = pd.read_sql(query, engine)
    df["event_time"] = pd.to_datetime(df["event_time"])
    df["day"] = df["event_time"].dt.date
    df["month"] = df["event_time"].dt.to_period("M").dt.to_timestamp()
    return df

def compute_daily_stats(df):
    daily_stats = df.groupby("day").agg(
        customers=("user_id", "nunique"),
        total_sales=("price", "sum")
    )
    daily_stats["avg_spend"] = daily_stats["total_sales"] / daily_stats["customers"]
    return daily_stats


def compute_monthly_stats(df):
    monthly_stats = df.groupby("month").agg(
        customers=("user_id", "nunique"),
        total_sales=("price", "sum")
    )
    monthly_stats["avg_spend"] = monthly_stats["total_sales"] / monthly_stats["customers"]
    return monthly_stats


def create_chart_clients(daily_stats):
    plt.figure(figsize=(10, 6))
    plt.plot(daily_stats.index, daily_stats["customers"], marker='o')
    plt.ylabel('Number of customers')
    xticks = [d for d in daily_stats.index if d.day == 1]
    xticklabels = [d.strftime("%b") for d in xticks]
    plt.xticks(xticks, xticklabels)
    plt.grid(True)
    plt.title("Number of Customers per Month")
    plt.show()

def create_chart_sales(monthly_stats):
    plt.figure(figsize=(10, 6))
    plt.bar(monthly_stats.index, monthly_stats["total_sales"] / 1_000_000, width=15)
    plt.ylabel('Total sales (millions of Altairian Dollars)')
    xticks = monthly_stats.index
    plt.xticks(xticks, x_labels)
    plt.grid(axis='y')
    plt.title("Total Sales per Month")
    plt.show()

def create_chart_averageSales(daily_stats):
    plt.figure(figsize=(10, 6))
    plt.fill_between(daily_stats.index, daily_stats["avg_spend"], alpha=0.4)
    plt.plot(daily_stats.index, daily_stats["avg_spend"], marker='o')
    plt.ylabel('Average spend per customer (Altairian Dollars)')
    xticks = [d for d in daily_stats.index if d.day == 1]
    xticklabels = [d.strftime("%b") for d in xticks]
    plt.xticks(xticks, xticklabels)
    plt.grid(True)
    plt.title("Average Spend per Customer per Month")
    plt.show()


if __name__ == "__main__":
    df = fetch_data()
    monthly_stats = compute_monthly_stats(df)
    daily_stats = compute_daily_stats(df)
    create_chart_clients(daily_stats)
    create_chart_sales(monthly_stats)
    create_chart_averageSales(daily_stats)
