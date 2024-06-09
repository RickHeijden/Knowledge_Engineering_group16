import pandas as pd
import json


class GenerateDataframes:

    def __init__(self, directory: str):
        self.__directory = directory

    def generate_dataframes(self):
        return (
            self.generate_dataframe1(),
            self.generate_dataframe2(),
            self.generate_dataframe3(),
            self.generate_dataframe4()
        )

    def generate_dataframe1(self):
        file = self.__directory + 'amazon_bs_20102020.csv'
        df_file = pd.read_csv(file)

        df_file.columns = df_file.columns.str.lower()
        df_file.rename(columns={'book_title': 'title'}, inplace=True)
        df_file = df_file[['title', 'author', 'rank', 'rating']]

        return df_file

    def generate_dataframe2(self):
        file = self.__directory + 'Amazon_popular_books_dataset.csv'
        df_file = pd.read_csv(file)
        df_file.columns = df_file.columns.str.lower()
        df_file.rename(columns={'brand': 'author', 'best_sellers_rank': 'rank'}, inplace=True)
        df_file = df_file[['title', 'author', 'isbn10', 'rating', 'categories', 'rank']]
        df_file['rank'] = df_file['rank'].apply(
            lambda x:
            min([int(z['rank']) for z in [y for y in json.loads(x)]]) if not pd.isnull(x) else None
            if x else None
        )
        df_file['rating'] = df_file['rating'].apply(lambda x: x.split(' ')[0] if not pd.isnull(x) else None)

        return df_file

    def generate_dataframe3(self):
        file = self.__directory + 'bestsellers.csv'

        df_file = pd.read_csv(file)
        df_file.columns = df_file.columns.str.lower()
        df_file = df_file[['title', 'author', 'isbn10', 'isbn13', 'description', 'rank', 'amazon_product_url']]

        return df_file

    def generate_dataframe4(self):
        file = self.__directory + 'nyt2.json'

        desired_file4 = self.__directory + 'nyt2.csv'
        df_file = pd.read_json(file, lines=True, dtype_backend='pyarrow', engine='pyarrow')
        df_file.columns = df_file.columns.str.lower()
        df_file = df_file[['title', 'author', 'rank', 'description', 'publisher', 'amazon_product_url']]
        df_file['rank'] = df_file['rank'].apply(lambda x: x.get('$numberInt', None) if x else None)
        df_file.to_csv(desired_file4, index=False)

        return df_file
