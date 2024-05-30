import pandas as pd
import json

# 1) Convert the fourth dataset from JSON file to CSV
# 2) Explore the csv headers
# 3) Combine the 4 CSV files into a single CSV file
# Note: Country of publication has to be engineered
# 4) Check how many duplicate titles there are in the combined dataset
if __name__ == '__main__':
    # Dataset files
    directory = 'datasets/'
    file1 = directory + 'amazon_bs_20102020.csv'
    file2 = directory + 'Amazon_popular_books_dataset.csv'
    file3 = directory + 'bestsellers.csv'
    file4 = directory + 'nyt2.json'

    df_file1 = pd.read_csv(file1)
    df_file1.columns = df_file1.columns.str.lower()
    df_file1.rename(columns={'book_title': 'title'}, inplace=True)
    df_file1 = df_file1[['title', 'author', 'rank', 'rating']]

    df_file2 = pd.read_csv(file2)
    df_file2.columns = df_file2.columns.str.lower()
    df_file2.rename(columns={'brand': 'author', 'best_sellers_rank': 'rank'}, inplace=True)
    df_file2 = df_file2[['title', 'author', 'isbn10', 'rating', 'categories', 'rank']]
    df_file2['rank'] = df_file2['rank'].apply(
        lambda x:
        min([int(z['rank']) for z in [y for y in json.loads(x)]]) if not pd.isnull(x) else None
        if x else None
    )
    df_file2['rating'] = df_file2['rating'].apply(lambda x: x.split(' ')[0] if not pd.isnull(x) else None)

    df_file3 = pd.read_csv(file3)
    df_file3.columns = df_file3.columns.str.lower()
    df_file3 = df_file3[['title', 'author', 'isbn10', 'isbn13', 'description', 'rank', 'amazon_product_url']]

    desired_file4 = directory + 'nyt2.csv'
    df_file4 = pd.read_json(file4, lines=True, dtype_backend='pyarrow', engine='pyarrow')
    df_file4.columns = df_file4.columns.str.lower()
    df_file4 = df_file4[['title', 'author', 'rank', 'description', 'publisher', 'amazon_product_url']]
    df_file4['rank'] = df_file4['rank'].apply(lambda x: x.get('$numberInt', None) if x else None)
    df_file4.to_csv(desired_file4, index=False)

    columns = set(df_file1.columns) | set(df_file2.columns) | set(df_file3.columns) | set(df_file4.columns)
    dataframes = [df_file1, df_file2, df_file3, df_file4]

    for df in dataframes:
        for column in columns:
            if column not in df.columns:
                df[column] = None

    combined_dataframe = pd.concat(dataframes, ignore_index=True)

    def combine_rows(series: pd.Series):
        return series.dropna().iloc[0] if not series.dropna().empty else None

    print(len(combined_dataframe))
    # Join on isbn10, isbn13, (title and author)
    combined_dataframe_without_isbn10 = combined_dataframe[combined_dataframe['isbn10'].isnull()]
    combined_dataframe = combined_dataframe.groupby('isbn10', as_index=False).agg(combine_rows)
    combined_dataframe = pd.concat([combined_dataframe, combined_dataframe_without_isbn10], ignore_index=True)
    print(len(combined_dataframe))

    # Same for isbn13
    combined_dataframe_without_isbn13 = combined_dataframe[combined_dataframe['isbn13'].isnull()]
    combined_dataframe = combined_dataframe.groupby('isbn13', as_index=False).agg(combine_rows)
    combined_dataframe = pd.concat([combined_dataframe, combined_dataframe_without_isbn13], ignore_index=True)
    print(len(combined_dataframe))

    # Same for title and author
    combined_dataframe_without_title_author = \
        combined_dataframe[combined_dataframe['title'].isnull() | combined_dataframe['author'].isnull()]
    combined_dataframe = combined_dataframe.groupby(['title', 'author'], as_index=False).agg(combine_rows)
    combined_dataframe = pd.concat([combined_dataframe, combined_dataframe_without_title_author], ignore_index=True)
    print(len(combined_dataframe))

    words_to_split_author = [' and ', ',', '&', ' with ']

    def split_authors(author):
        if pd.isnull(author):
            return author
        # Start with the original author string
        split_authors = [author]
        # Split the author string by each word in words_to_split_author
        for word in words_to_split_author:
            split_authors = [a for auth in split_authors for a in auth.split(word)]
        # Join the resulting list with ';'
        return ';'.join(a.strip() for a in split_authors)
    # Apply the function to the 'author' column
    combined_dataframe['author'] = combined_dataframe['author'].apply(split_authors)

    combined_dataframe.to_csv(directory + 'combined.csv', index=False)
