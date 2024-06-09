import os
from cleaner import clean_authors
from combiner import *

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
        generateDataframes = GenerateDataframes(directory)
        df_file1, df_file2, df_file3, df_file4 = generateDataframes.generate_dataframes()

        combined_dataframe = combine_dataframes(df_file1, df_file2, df_file3, df_file4)

        print(len(combined_dataframe))
        # Join on isbn10
        combined_dataframe = combine_on_isbn10(combined_dataframe)
        print(len(combined_dataframe))

        # Join on isbn13
        print(len(combined_dataframe))
        combined_dataframe = combine_on_isbn13(combined_dataframe)

        # Lowercase the title
        combined_dataframe['title'] = combined_dataframe['title'].str.lower()

        # Join on title and author
        combined_dataframe = combine_on_title_author(combined_dataframe)
        print(len(combined_dataframe))

        combined_dataframe.to_csv(directory + 'combined.csv', index=False)
    else:
        combined_dataframe = pd.read_csv('datasets/combined.csv')

    # Apply the function to the 'author' column
    combined_dataframe['author'] = clean_authors(combined_dataframe['author'])

    # Reapplying combining title and author matches
    combined_dataframe = combine_on_title_author(combined_dataframe)

    combined_dataframe.to_csv(directory + 'combined_filtered.csv', index=False)
