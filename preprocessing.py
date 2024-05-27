import pandas as pd
from data_retriever import DataRetriever


class Preprocessing:
    __COLUMNS__: list[str] = ['title', 'author', 'year', 'publisher', 'rating', 'rank', 'categories', 'description']
    __df: pd.DataFrame

    def __init__(self, df: pd.DataFrame) -> None:
        self.__df = df

    def process(self) -> None:
        self.__df = self.__df.apply(self.__process_row)

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
        if row['isbn13'] is None or row['isbn10'] is None:
            data: dict | None = DataRetriever.get_json_from_title_and_author(row['title'], row['author'])
            industry_identifiers: list[dict[str, str]] = data[0]['volumeInfo']['industryIdentifiers']

            row['isbn13'] = [
                identifier['isbn13'] for identifier in industry_identifiers if identifier['type'] == 'isbn13'
            ][0]
            row['isbn10'] = [
                identifier['isbn10'] for identifier in industry_identifiers if identifier['type'] == 'isbn10'
            ][0]

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

                volume_info: dict = data[0]['volumeInfo']

                if search == 'author':
                    row[column] = volume_info[search][0]
                else:
                    row[column] = volume_info[search]

        return row
