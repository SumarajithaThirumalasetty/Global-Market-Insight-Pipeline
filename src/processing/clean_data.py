import pandas as pd
import os

RAW_OUTPUT_DIR = "../../sample_data/raw/"
CLEANED_OUTPUT_DIR = "../../sample_data/processed/"

def clean_data(filename):
    df = pd.read_csv(os.path.join(RAW_OUTPUT_DIR, filename))
    
    # Example cleaning
    df.drop_duplicates(inplace=True)
    df.fillna(0, inplace=True)
    
    # Save cleaned data
    if not os.path.exists(CLEANED_OUTPUT_DIR):
        os.makedirs(CLEANED_OUTPUT_DIR)
    cleaned_file = os.path.join(CLEANED_OUTPUT_DIR, "cleaned_" + filename)
    df.to_csv(cleaned_file, index=False)
    print(f"Saved cleaned data to {cleaned_file}")

if __name__ == "__main__":
    # Replace with your raw file name
    clean_data("stocks_raw_2025-11-24.csv")
