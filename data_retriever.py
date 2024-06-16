import requests
import time
from requests.exceptions import RequestException
from requests import Response


class DataRetriever:
    """
    Class that retrieves data from the Google Books API and the DBpedia SPARQL endpoint.
    """

    # Number of requests made to the API
    __requests: int

    def __init__(self):
        self.__requests = 0

    def get_json_from_isbn(self, isbn: str) -> dict | bool:
        """
        Get the JSON response from the Google Books API using the ISBN.

        @param isbn: The ISBN of the book.

        @return: The JSON response from the Google Books API.
        """
        self.__requests += 1
        # Rate limit the requests
        if self.__requests % 10 == 0:
            time.sleep(1)

        try:
            base_api_link: str = "https://www.googleapis.com/books/v1/volumes"
            params: dict[str, str] = {
                'q': f"isbn:{isbn}",
                'langRestrict': 'en',
                'projection': 'lite'
            }

            response: Response = requests.get(base_api_link, params=params)

            if response.status_code != 200:
                return False

            return response.json()
        except RequestException as e:
            print(f"An error occurred: {e}")

            return False

    def get_json_from_title_and_author(self, title: str, author: str | None) -> dict | bool:
        """
        Get the JSON response from the Google Books API using the title and author.

        @param title: The title of the book.
        @param author: The author of the book.

        @return: The JSON response from the Google Books API.
        """
        self.__requests += 1
        # Rate limit the requests
        if self.__requests % 10 == 0:
            time.sleep(1)

        try:
            base_api_link: str = "https://www.googleapis.com/books/v1/volumes"
            if author is None:
                params: dict[str, str] = {
                    'q': f"intitle:{title}",
                    'langRestrict': 'en',
                    'projection': 'lite'
                }
            else:
                params: dict[str, str] = {
                    'q': f"intitle:{title}+inauthor:{author}",
                    'langRestrict': 'en',
                    'projection': 'lite'
                }

            response: Response = requests.get(base_api_link, params=params)

            if response.status_code != 200:
                return False

            return response.json()
        except RequestException as e:
            print(f"An error occurred: {e}")

            return False

    def get_books_from_author(self, author_name: str) -> list:
        """
        Get the books written by the author from the Google Books API.

        @param author_name: The name of the author to retrieve the books for.

        @return: The list of books written by the author.
        """
        self.__requests += 1
        # Rate limit the requests
        if self.__requests % 10 == 0:
            time.sleep(1)

        start_index = 0
        total_items = None
        books = []

        try:
            # Looping over the pagination
            while start_index == 0 or start_index < total_items:
                base_api_link = "https://www.googleapis.com/books/v1/volumes"
                params = {
                    'q': f"inauthor:{author_name}",
                    'langRestrict': 'en',
                    'startIndex': start_index,
                    'projection': 'lite'
                }

                response = requests.get(base_api_link, params=params)

                if response.status_code != 200:
                    return []

                data = response.json()

                if 'totalItems' not in data or 'items' not in data:
                    return books

                if not total_items:
                    total_items = data['totalItems']

                books.extend(data['items'])
                start_index += 40

            return books
        except RequestException as e:
            print(f"An error occurred: {e}")

            return []

    def get_author_info_from_dbpedia(self, author_name: str) -> dict | bool:
        """
        Get the author information from DBpedia using the author name.

        We retrieve the following information about the author if available:
        - Abstract
        - Birth date
        - Country name
        - Death date
        - Genres the author writes in

        @param author_name: The name of the author to retrieve the information for.
        @return: The JSON response from the DBpedia SPARQL endpoint.
        """
        self.__requests += 1
        # Rate limit the requests
        if self.__requests % 100 == 0:
            time.sleep(1)

        try:
            # DBpedia SPARQL endpoint
            endpoint: str = "https://dbpedia.org/sparql"

            # Construct the SPARQL query
            query: str = """
        SELECT
            ?abstract
            ?birthDate
            ?countryName
            ?deathDate
            (GROUP_CONCAT(DISTINCT ?genre; separator=", ") as ?genres)
        WHERE {
            {
                SELECT DISTINCT ?author ?abstract ?birthDate ?countryName ?deathDate ?genre (SAMPLE(?isWriter) as ?writerSample)
                WHERE {
                    ?author dbp:name "%s"@en ;
                            dbo:abstract ?abstract .
                    OPTIONAL { ?author a dbo:Writer . BIND(true AS ?isWriter) }
                    OPTIONAL { ?author dbo:birthDate ?birthDate . }
                    OPTIONAL { 
                        ?author dbo:birthPlace ?birthPlace .
                        {
                            ?birthPlace dbo:country ?country .
                            ?country rdfs:label ?countryName .
                            FILTER (lang(?countryName) = "en")
                        }
                        UNION
                        {
                            ?birthPlace a dbo:Country .
                            ?birthPlace rdfs:label ?countryName .
                            FILTER (lang(?countryName) = "en")
                        }
                    }
                    OPTIONAL { ?author dbo:deathDate ?deathDate . }
                    OPTIONAL { ?author dbo:genre ?genre . }
                    FILTER (lang(?abstract) = 'en')
                }
                GROUP BY ?author ?abstract ?birthDate ?countryName ?deathDate ?genre
            }
            FILTER (!BOUND(?writerSample) || ?writerSample = true)
        }
        GROUP BY ?abstract ?birthDate ?countryName ?deathDate
            """ % author_name

            # Set the parameters for the request
            params: dict[str, str] = {
                'query': query,
                'format': 'application/sparql-results+json'
            }

            # Make the request to the DBpedia SPARQL endpoint
            response: Response = requests.get(endpoint, params=params)
            if response.status_code != 200:
                return False

            data = response.json()
            # Check if the response is empty
            if not data['results']['bindings']:
                if '.' in author_name:
                    author_name = author_name.replace('.', '')
                    return self.get_author_info_from_dbpedia(author_name)
                return False

            # Return the JSON response
            return data
        except RequestException as e:
            print(f"An error occurred: {e}")

            return False
