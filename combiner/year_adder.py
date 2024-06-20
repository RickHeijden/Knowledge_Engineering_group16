import pandas as pd


def add_year_from_processed():
    """
    Add the 'year' column from the 'processed.csv' file to the 'combined_filtered_enriched.csv' file.
    """
    # Step 1: Read the CSV files
    processed_df = pd.read_csv('../datasets/processed.csv')
    combined_filtered_enriched_df = pd.read_csv('../datasets/combined_filtered_enriched.csv')

    # Step 2: Merge the DataFrames on the 'title' column
    result_df = pd.merge(combined_filtered_enriched_df, processed_df[['title', 'year']], on='title', how='left')

    # Step 3: Save the resulting DataFrame to a new CSV file
    result_df.to_csv('processed_combined_filtered_enriched.csv', index=False)


if __name__ == "__main__":
    add_year_from_processed()
