import pandas as pd
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
        authors: list[str] = self.get_authors()

        row_list: list = []
        for index, author in enumerate(authors):
            author_info: dict = DataRetriever.get_author_info_from_dbpedia(author)

            author_info_row: dict[str, str | bool | None] = {
                'author': author,
                'birthDate': None,
                'birthPlace': None,
                'birthCountries': None,
                'deathDate': None,
                'genres': None,
                'influenced': None,
                'influencedBy': None,
                'properlyProcessed': False,
            }

            if author_info:
                author_info = author_info['results']['bindings']

                if len(author_info) > 0:
                    author_info = author_info[0]

                    if 'birthDate' in author_info and author_info['birthDate']:
                        author_info_row['birthDate'] = author_info['birthDate']['value']

                    if 'birthPlace' in author_info and author_info['birthPlace']:
                        author_info_row['birthPlace'] = author_info['birthPlace']['value']

                    if 'birthCountries' in author_info and author_info['birthCountries']:
                        author_info_row['birthCountries'] = author_info['birthCountries']['value']

                    if 'deathDate' in author_info and author_info['deathDate']:
                        author_info_row['deathDate'] = author_info['deathDate']['value']

                    if 'genres' in author_info and author_info['genres']:
                        author_info_row['genres'] = author_info['genres']['value']

                    if 'influenced' in author_info and author_info['influenced']:
                        author_info_row['influenced'] = author_info['influenced']['value']

                    if 'influencedBy' in author_info and author_info['influencedBy']:
                        author_info_row['influencedBy'] = author_info['influencedBy']['value']

                    author_info_row['properlyProcessed'] = True

            row_list.append(author_info_row)

        author_info_df = pd.DataFrame(row_list)
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

            if data is None or data['totalItems'] == 0:
                return row

            industry_identifiers: list[dict[str, str]] = data['items'][0]['volumeInfo']['industryIdentifiers']

            isbn13 = [
                identifier['identifier'] for identifier in industry_identifiers
                if identifier['type'].lower() == 'isbn_13'
            ]

            if len(isbn13) > 0:
                row['isbn13'] = isbn13[0]

            isbn10 = [
                identifier['identifier'] for identifier in industry_identifiers
                if identifier['type'].lower() == 'isbn_10'
            ]

            if len(isbn10) > 0:
                row['isbn10'] = isbn10[0]

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

                if data is None:
                    continue

                if data['totalItems'] > 0:
                    volume_info: dict = data['items'][0]['volumeInfo']
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
