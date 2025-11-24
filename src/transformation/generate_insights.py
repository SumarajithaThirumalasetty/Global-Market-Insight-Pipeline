import pandas as pd
import os

CLEANED_OUTPUT_DIR = "../../sample_data/processed/"
INSIGHTS_OUTPUT_DIR = "../../sample_data/insights/"

def generate_insights(filename):
    df = pd.read_csv(os.path.join(CLEANED_OUTPUT_DIR, filename))
    
    # Example transformation: calculate daily change
    df['daily_change'] = df['close'] - df['open']
    
    # Save insights
    if not os.path.exists(INSIGHTS_OUTPUT_DIR):
        os.makedirs(INSIGHTS_OUTPUT_DIR)
    insights_file = os.path.join(INSIGHTS_OUTPUT_DIR, "insights_" + filename)
    df.to_csv(insights_file, index=False)
    print(f"Saved insights to {insights_file}")

if __name__ == "__main__":
    # Replace with your cleaned file name
    generate_insights("cleaned_stocks_raw_2025-11-24.csv")
