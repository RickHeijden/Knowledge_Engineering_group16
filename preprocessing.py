import pandas as pd
from data_retriever import DataRetriever


class Preprocessing:
    __COLUMNS__ = ['title', 'author', 'year', 'publisher', 'rating', 'rank', 'categories', 'description']
    __df: pd.DataFrame

    def __init__(self, df: pd.DataFrame) -> None:
        self.__df = df

    def process(self) -> None:
        self.__df = self.__df.apply(self.__process_row)

    def get_df(self) -> pd.DataFrame:
        return self.__df

    def create_author_info(self) -> pd.DataFrame:
        author_info_df = pd.DataFrame(columns=['author', 'birthDate', 'deathDate'])
        authors = self.get_authors()

        for author in authors:
            author_info = DataRetriever.get_author_info_from_dbpedia(author)

            if author_info:
                author_info = author_info['results']['bindings']
                author_info_row = {
                    'author': author,
                    'birthDate': author_info['birthDate']['value'] if 'birthDate' in author_info else None,
                    'deathDate': author_info['deathDate']['value'] if 'deathDate' in author_info else None
                }
                author_info_df = author_info_df.append(author_info_row, ignore_index=True)

        return author_info_df

    def save_author_info(self, file_path: str) -> None:
        author_info_df = self.create_author_info()
        author_info_df.to_csv(file_path)

    def get_authors(self):
        return self.__df['author'].unique()

    def __process_row(self, row: pd.Series) -> pd.Series:
        if row['isbn13'] is None or row['isbn10'] is None:
            data = DataRetriever.get_json_from_title_and_author(row['title'], row['author'])
            industryIdentifiers: list[dict[str, str]] = data[0]['volumeInfo']['industryIdentifiers']

            row['isbn13'] = [identifier['isbn13'] for identifier in industryIdentifiers if identifier['type'] == 'isbn13'][0]
            row['isbn10'] = [identifier['isbn10'] for identifier in industryIdentifiers if identifier['type'] == 'isbn10'][0]

        data = None

        # Check if any of the __COLUMNS__ are NONE in te row
        for column in self.__COLUMNS__:
            if row[column] is None:
                search = column
                if search == 'author':
                    search = 'authors'
                elif search == 'year':
                    search = 'publishedDate'
                elif search == 'rating' or search == 'rank':
                    continue

                if data is None:
                    isbn = row['isbn13']
                    if isbn is None:
                        isbn = row['isbn10']

                    data = DataRetriever.get_json_from_isbn(isbn)

                volume_info = data[0]['volumeInfo']

                if search == 'author':
                    row[column] = volume_info[search][0]
                else:
                    row[column] = volume_info[search]

        return row
