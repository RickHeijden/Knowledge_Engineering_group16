import pandas as pd
import os
from cleaner import clean_authors
from data_retriever import DataRetriever


def _stringify_author(author: str) -> str:
    """
    Convert the author to a string.

    @param author: The author to convert.
    @return: The author as a string.
    """
    return author if type(author) is str else ''


class Preprocessing:
    """
    Class that preprocesses the data.
    """

    # The columns to keep in the dataframe
    __COLUMNS__: list[str] = ['title', 'author', 'publisher', 'rating', 'rank', 'categories', 'description', 'year']

    # The dataframe to process
    __dataframe: pd.DataFrame

    # The data retriever
    __data_retriever: DataRetriever

    def __init__(self, dataframe: pd.DataFrame) -> None:
        self.__dataframe = dataframe
        self.__data_retriever = DataRetriever()

    def process(self, processed_filename: str) -> None:
        """
        Process the dataframe and save it to a CSV file.

        @param processed_filename: The name of the file to save the processed dataframe to.
        """
        # Add the columns that are not in the dataframe
        for column in self.__COLUMNS__:
            if column not in self.__dataframe.columns:
                self.__dataframe[column] = None

        # Process the rows and save them in batches of 1000 to the CSV file
        rows = []
        idx = 1
        for index, row in self.__dataframe.iterrows():
            rows.append(self.__process_row(row))
            if idx % 1000 == 0:
                pd.DataFrame(rows).to_csv(processed_filename, index=False)
            idx += 1

        pd.DataFrame(rows).to_csv(processed_filename, index=False)

    def get_df(self) -> pd.DataFrame:
        """
        Get the dataframe.

        @return: The dataframe.
        """
        return self.__dataframe

    def create_author_info(self, path: str) -> None:
        """
        Create a CSV file with the author information.

        This method retrieves the authors from the books csv and adds the author information from DBpedia and saves it to a CSV file.

        @param path: The path to the CSV file to save.
        """

        # If file does not exist, create it and add CSV column headers.
        if not os.path.exists(path):
            with open(path, "w") as file:
                file.write(
                    ",".join(
                        [
                            "author",
                            "birth_date",
                            "birth_country",
                            "death_date",
                            "genres",
                            "properly_processed",
                        ]
                    )
                    + "\n"
                )

        with open(path, "a", buffering=1, encoding="utf-8") as file:
            def write_line(
                    author_name: str,
                    birth_date: str,
                    birth_country: str,
                    death_date: str,
                    genres: str,
                    properly_processed: bool,
            ):
                """
                Write a line to the CSV file.
                """
                file.write(
                    ",".join(
                        map(
                            str,
                            [
                                author_name,
                                birth_date,
                                birth_country,
                                death_date,
                                genres,
                                properly_processed,
                            ],
                        )
                    )
                    + "\n"
                )

            authors: list[str] = self.get_authors()

            for author in authors:
                author_info: dict = self.__data_retriever.get_author_info_from_dbpedia(author)

                author_info_row: dict[str, str | bool] = {
                    'author': author,
                    'birthDate': '',
                    'birthCountry': '',
                    'deathDate': '',
                    'genres': '',
                    'properlyProcessed': False,
                }

                if author_info:
                    author_info = author_info['results']['bindings']

                    if len(author_info) > 0:
                        author_info = author_info[0]

                        if 'birthDate' in author_info and author_info['birthDate']:
                            author_info_row['birthDate'] = author_info['birthDate']['value'].replace(',', ';')

                        if 'countryName' in author_info and author_info['countryName']:
                            author_info_row['birthCountry'] = author_info['countryName']['value']

                        if 'deathDate' in author_info and author_info['deathDate']:
                            author_info_row['deathDate'] = author_info['deathDate']['value'].replace(',', ';')

                        if 'genres' in author_info and author_info['genres']:
                            author_info_row['genres'] = author_info['genres']['value'].replace(
                                'http://dbpedia.org/resource/', '').replace(',', ';')

                        author_info_row['properlyProcessed'] = True

                write_line(
                    author_info_row['author'],
                    author_info_row['birthDate'],
                    author_info_row['birthCountry'],
                    author_info_row['deathDate'],
                    author_info_row['genres'],
                    author_info_row['properlyProcessed']
                )

    def get_authors(self) -> list[str]:
        """
        Get the authors from the dataframe.

        @return: The authors.
        """
        authors = clean_authors(self.__dataframe['author'])

        return authors.map(_stringify_author).str.split(';').explode().str.strip().unique()

    def __process_row(self, row: pd.Series) -> pd.Series:
        """
        Process a row by adding all the missing information.

        @param row: The row to process.
        @return: The processed row.
        """
        author = row['author']
        if not author:
            author = None

        data: dict | False = self.__data_retriever.get_json_from_title_and_author(row['title'], author)
        if not data:
            return row

        # Check if any of the __COLUMNS__ are NONE in te row
        for column in self.__COLUMNS__:
            if column not in row or not row[column]:
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
    processed_filename = 'datasets/processed_nonbestsellers.csv'
    author_info_filename = 'datasets/author_info2.csv'
    if not os.path.exists(processed_filename):
        df: pd.DataFrame = pd.read_csv('datasets/non_best_selling_books_filtered.csv')
        preprocessing: Preprocessing = Preprocessing(df)
        preprocessing.process(processed_filename)
    else:
        df: pd.DataFrame = pd.read_csv(processed_filename)
        preprocessing: Preprocessing = Preprocessing(df)
    preprocessing.create_author_info(author_info_filename)
