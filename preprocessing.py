import pandas as pd
import numpy as np
from data_retriever import DataRetriever


class Preprocessing:
    __COLUMNS__: list[str] = ['title', 'author', 'year', 'publisher', 'rating', 'rank', 'categories', 'description']
    __df: pd.DataFrame

    def __init__(self, df: pd.DataFrame) -> None:
        self.__df = df

    def process(self) -> None:
        for column in self.__COLUMNS__:
            if column not in self.__df.columns:
                self.__df[column] = None

        self.__df = self.__df.apply(self.__process_row, axis=1)

    def get_df(self) -> pd.DataFrame:
        return self.__df

    def create_author_info(self) -> pd.DataFrame:
        author_info_df: pd.DataFrame = pd.DataFrame(columns=[
            'author',
            'birthDate',
            'birthPlace',
            'birthCountries',
            'deathDate',
            'genres',
            'influenced',
            'influencedBy',
        ])
        authors: list[str] = self.get_authors()

        for author in authors:
            author_info: dict = DataRetriever.get_author_info_from_dbpedia(author)

            author_info_row: dict[str, str | None] = {
                'author': author,
                'birthDate': None,
                'birthPlace': None,
                'birthCountries': None,
                'deathDate': None,
                'genres': None,
                'influenced': None,
                'influencedBy': None,
            }

            if author_info:
                author_info = author_info['results']['bindings']
                if author_info['birthDate']:
                    author_info_row['birthDate'] = author_info['birthDate']['value']
                if author_info['birthPlace']:
                    author_info_row['birthPlace'] = author_info['birthPlace']['value']
                if author_info['birthCountries']:
                    author_info_row['birthCountries'] = author_info['birthCountries']['value']
                if author_info['deathDate']:
                    author_info_row['deathDate'] = author_info['deathDate']['value']
                if author_info['genres']:
                    author_info_row['genres'] = author_info['genres']['value']
                if author_info['influenced']:
                    author_info_row['influenced'] = author_info['influenced']['value']
                if author_info['influencedBy']:
                    author_info_row['influencedBy'] = author_info['influencedBy']['value']

            author_info_df = author_info_df.append(author_info_row, ignore_index=True)

        return author_info_df

    def save_author_info(self, file_path: str) -> None:
        author_info_df: pd.DataFrame = self.create_author_info()
        author_info_df.to_csv(file_path)

    def get_authors(self) -> list[str]:
        return self.__df['author'].unique()

    def __process_row(self, row: pd.Series) -> pd.Series:
        if str(row['isbn13']) == 'nan' or str(row['isbn10']) == 'nan':
            author = row['author']
            if str(author) == 'nan':
                author = None

            data: dict | None = DataRetriever.get_json_from_title_and_author(row['title'], author)
            if isinstance(data, dict):
                industry_identifiers: list[dict[str, str]] = (
                    data.get('items', [{}])[0].get('volumeInfo', {}).get('industryIdentifiers', [])
                )
            else:
                industry_identifiers: list[dict[str, str]] = []

            row['isbn13'] = next(
                (identifier['identifier'] for identifier in industry_identifiers if
                 identifier['type'].lower() == 'isbn_13'),
                None
            )

            row['isbn10'] = next(
                (identifier['identifier'] for identifier in industry_identifiers if
                 identifier['type'].lower() == 'isbn_10'),
                None
            )

        data: dict | None = None

        # Check if any of the __COLUMNS__ are NONE in te row
        for column in self.__COLUMNS__:
            if row[column] is None:
                search: str = column
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

                print(data)
                # Check if 'data' is a dictionary
                if isinstance(data, dict):
                    # Check if we have 'items' attribute in the data
                    if 'items' in data and isinstance(data['items'], list) and len(data['items']) > 0:
                        # Safely get volumeInfo
                        volume_info: dict = data['items'][0].get('volumeInfo', {})
                    else:
                        continue
                else:
                    continue

                if search == 'author':
                    row[column] = volume_info[search][0]
                else:
                    if search in volume_info:
                        row[column] = volume_info[search]

        return row


if __name__ == '__main__':
    df: pd.DataFrame = pd.read_csv('datasets/combined.csv')
    preprocessing: Preprocessing = Preprocessing(df)
    preprocessing.process()
    df = preprocessing.get_df()
    df.to_csv('datasets/processed.csv')
    preprocessing.save_author_info('datasets/author_info.csv')
    print(df)
