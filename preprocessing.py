import pandas as pd
import os
from data_retriever import DataRetriever


class Preprocessing:
    __COLUMNS__: list[str] = ['title', 'author', 'year', 'publisher', 'rating', 'rank', 'categories', 'description']
    __dataframe: pd.DataFrame
    __data_retriever: DataRetriever

    def __init__(self, dataframe: pd.DataFrame) -> None:
        self.__dataframe = dataframe
        self.__data_retriever = DataRetriever()

    def process(self) -> None:
        for column in self.__COLUMNS__:
            if column not in self.__dataframe.columns:
                self.__dataframe[column] = None

        self.__dataframe = self.__dataframe.apply(self.__process_row, axis=1)

    def get_df(self) -> pd.DataFrame:
        return self.__dataframe

    def create_author_info(self, path: str) -> None:
        # If file does not exist, create it and add CSV column headers.
        if not os.path.exists(path):
            with open(path, "w") as file:
                file.write(
                    ",".join(
                        [
                            "author",
                            "birth_date",
                            "birth_place",
                            "birth_countries",
                            "death_date",
                            "genres",
                            "influenced",
                            "influenced_by",
                            "properly_processed",
                        ]
                    )
                    + "\n"
                )

        with open(path, "a", buffering=1, encoding="utf-8") as file:
            def write_line(
                    author_name: str,
                    birth_date: str | None,
                    birth_place: str | None,
                    birth_countries: str | None,
                    death_date: str | None,
                    genres: str | None,
                    influenced: str | None,
                    influenced_by: str | None,
                    properly_processed: bool,
            ):
                """Write a line to the CSV file."""
                file.write(
                    ",".join(
                        map(
                            str,
                            [
                                author_name,
                                birth_date,
                                birth_place,
                                birth_countries,
                                death_date,
                                genres,
                                influenced,
                                influenced_by,
                                properly_processed,
                            ],
                        )
                    )
                    + "\n"
                )

            authors: list[str] = self.get_authors()

            for index, author in enumerate(authors):
                author = str(author)
                author_info: dict = self.__data_retriever.get_author_info_from_dbpedia(author)

                author_info_row: dict[str, str | bool | None] = {
                    'author': author.replace(',', ';'),
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
                            author_info_row['birthDate'] = author_info['birthDate']['value'].replace(',', ';')

                        if 'birthPlace' in author_info and author_info['birthPlace']:
                            author_info_row['birthPlace'] = author_info['birthPlace']['value'].replace(',', ';')

                        if 'birthCountries' in author_info and author_info['birthCountries']:
                            author_info_row['birthCountries'] = author_info['birthCountries']['value'].replace(',', ';')

                        if 'deathDate' in author_info and author_info['deathDate']:
                            author_info_row['deathDate'] = author_info['deathDate']['value'].replace(',', ';')

                        if 'genres' in author_info and author_info['genres']:
                            author_info_row['genres'] = author_info['genres']['value'].replace(',', ';')

                        if 'influenced' in author_info and author_info['influenced']:
                            author_info_row['influenced'] = author_info['influenced']['value'].replace(',', ';')

                        if 'influencedBy' in author_info and author_info['influencedBy']:
                            author_info_row['influencedBy'] = author_info['influencedBy']['value'].replace(',', ';')

                        author_info_row['properlyProcessed'] = True

                write_line(
                    author_info_row['author'],
                    author_info_row['birthDate'],
                    author_info_row['birthPlace'],
                    author_info_row['birthCountries'],
                    author_info_row['deathDate'],
                    author_info_row['genres'],
                    author_info_row['influenced'],
                    author_info_row['influencedBy'],
                    author_info_row['properlyProcessed'],
                )

    def _clean_author(self, author: str) -> str:
        return author.replace('(Author)', '').replace('(Narrator)', '').replace('(Author;Narrator)', '').strip()

    def get_authors(self) -> list[str]:
        return self.__dataframe['author'].map(self._clean_author).str.split(';').explode().unique().tolist()

    def __process_row(self, row: pd.Series) -> pd.Series:
        if str(row['isbn13']) == 'nan' or str(row['isbn10']) == 'nan':
            author = row['author']
            if str(author) == 'nan':
                author = None

            data: dict | False = self.__data_retriever.get_json_from_title_and_author(row['title'], author)

            if not data or data['totalItems'] == 0:
                return row

            try:
                industry_identifiers: list[dict[str, str]] = data['items'][0]['volumeInfo']['industryIdentifiers']
            except KeyError:
                return row
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

        data: dict | False = False

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

                if not data:
                    isbn = row['isbn13']
                    if isbn is None:
                        isbn = row['isbn10']

                    data = self.__data_retriever.get_json_from_isbn(isbn)

                if not data:
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
    preprocessing.create_author_info('datasets/author_info.csv')
    print(df)
