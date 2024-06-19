import os
from cleaner import clean_authors
from combiner import *


def integrate_dataframes():
    """
    Integrate the dataframes into a single dataframe.

    @return: The integrated dataframe.
    """
    generation = GenerateDataframes(directory)
    df_file1, df_file2, df_file3, df_file4 = generation.generate_dataframes()

    dataframe = combine_dataframes(df_file1, df_file2, df_file3, df_file4)

    print(len(dataframe))
    # Join on isbn10
    dataframe = combine_on_isbn10(dataframe)
    print(len(dataframe))

    # Join on isbn13
    print(len(dataframe))
    dataframe = combine_on_isbn13(dataframe)

    # Lowercase the title
    dataframe['title'] = dataframe['title'].str.lower()

    # Join on title and author
    dataframe = combine_on_title_author(dataframe)
    print(len(dataframe))

    dataframe.to_csv(directory + 'combined.csv', index=False)

    return dataframe


# 1) Convert the fourth dataset from JSON file to CSV
# 2) Explore the csv headers
# 3) Combine the 4 CSV files into a single CSV file
# Note: Country of publication has to be engineered
# 4) Check how many duplicate titles there are in the combined dataset
if __name__ == '__main__':
    # Dataset files
    directory = 'datasets/'

    # Check if combined.csv exists using os
    if not os.path.exists(directory + 'combined.csv'):
        combined_dataframe = integrate_dataframes()
    else:
        combined_dataframe = pd.read_csv('datasets/combined.csv')

    # Apply the function to the 'author' column
    combined_dataframe['author'] = clean_authors(combined_dataframe['author'])

    # Reapplying combining title and author matches
    combined_dataframe = combine_on_title_author(combined_dataframe)

    combined_dataframe.to_csv(directory + 'combined_filtered.csv', index=False)
