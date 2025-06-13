import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import numpy as np

load_dotenv()

DB_NAME = 'piscineds'
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = 'localhost'
DB_PORT = '5332'

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def fetch_data():
    engine = create_engine(DATABASE_URL)
    query = """
        SELECT price, event_type
        FROM customers
    """
    df = pd.read_sql(query, engine)

    return df[df["event_type"] == "purchase"]


def print_price_stats(df):
    prices = df["price"]
    count = len(prices)
    mean_price = np.mean(prices)
    std_price = np.std(prices)
    min_price = np.min(prices)
    quartiles = np.percentile(prices, [25, 50, 75])
    max_price = np.max(prices)

    print(f"count {count:.6f}")
    print(f"mean {mean_price:.6f}")
    print(f"std {std_price:.6f}")
    print(f"min {min_price:.6f}")
    print(f"25% {quartiles[0]:.6f}")
    print(f"50% {quartiles[1]:.6f}")
    print(f"75% {quartiles[2]:.6f}")
    print(f"max {max_price:.6f}")

def plot_price_boxplot(df):
    prices = df["price"]

    fig, (ax1, ax2) = plt.subplots(nrows=1, ncols=2, figsize=(14, 4), sharey=True)

    # Boxplot complet avec outliers
    ax1.boxplot(prices, vert=False, notch=True,
                patch_artist=True,
                boxprops=dict(facecolor='lightgray'),
                flierprops=dict(marker='D', color='red', markersize=4),
				widths=0.4)
    ax1.set_title("Full Boxplot (with outliers)")
    ax1.set_xlabel("Price (₳)")
    ax1.grid(True)


    # Boxplot zoomé, sans outliers
    ax2.boxplot(prices, vert=False, notch=True,
                patch_artist=True,
                showfliers=False,
                boxprops=dict(facecolor='lightgreen'),
                medianprops=dict(color='black'),
				widths=0.4)
    ax2.set_title("Zoomed Boxplot (no outliers)")
    ax2.set_xlabel("Price")
    ax2.set_xlim(0, np.percentile(prices, 95))  # zoom jusqu'au 95e percentile
    ax2.grid(True)

    plt.suptitle("Price Distribution of Purchased Items")
    plt.tight_layout()
    plt.show()



if __name__ == "__main__":
	df = fetch_data()
	print_price_stats(df)
	plot_price_boxplot(df)

