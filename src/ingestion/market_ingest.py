import pandas as pd
import os
from datetime import datetime

# Path to sample data
RAW_DATA_DIR = "../../sample_data/"
RAW_OUTPUT_DIR = "../../sample_data/raw/"

def read_sample_data(filename):
    df = pd.read_csv(os.path.join(RAW_DATA_DIR, filename))
    return df

def save_raw_data(df, filename):
    if not os.path.exists(RAW_OUTPUT_DIR):
        os.makedirs(RAW_OUTPUT_DIR)
    filepath = os.path.join(RAW_OUTPUT_DIR, filename)
    df.to_csv(filepath, index=False)
    print(f"Saved raw data to {filepath}")

if __name__ == "__main__":
    df = read_sample_data("stocks_sample.csv")
    today = datetime.today().strftime("%Y-%m-%d")
    save_raw_data(df, f"stocks_raw_{today}.csv")  
