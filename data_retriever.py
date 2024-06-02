import requests
import time
from requests.exceptions import RequestException
from requests import Response


class DataRetriever:
    __requests: int

    def __init__(self):
        self.__requests = 0

    def get_json_from_isbn(self, isbn: str) -> dict | bool:
        self.__requests += 1
        if self.__requests % 10 == 0:
            time.sleep(1)

        try:
            base_api_link: str = "https://www.googleapis.com/books/v1/volumes"
            params: dict[str, str] = {
                'q': f"isbn:{isbn}",
                'langRestrict': 'en'
            }

            response: Response = requests.get(base_api_link, params=params)
            if response.status_code != 200:
                return False

            return response.json()
        except RequestException as e:
            print(f"An error occurred: {e}")

            return False

    def get_json_from_title_and_author(self, title: str, author: str | None) -> dict | bool:
        self.__requests += 1
        if self.__requests % 10 == 0:
            time.sleep(1)

        try:
            base_api_link: str = "https://www.googleapis.com/books/v1/volumes"
            if author is None:
                params: dict[str, str] = {
                    'q': f"intitle:{title}",
                    'langRestrict': 'en'
                }
            else:
                params: dict[str, str] = {
                    'q': f"intitle:{title}+inauthor:{author}",
                    'langRestrict': 'en'
                }

            response: Response = requests.get(base_api_link, params=params)
            if response.status_code != 200:
                return False

            return response.json()
        except RequestException as e:
            print(f"An error occurred: {e}")

            return False

    def get_books_from_author(self, author_name: str) -> list:
        self.__requests += 1
        if self.__requests % 10 == 0:
            time.sleep(1)

        start_index = 0
        total_items = None
        books = []

        try:
            while start_index == 0 or start_index < total_items:
                base_api_link = "https://www.googleapis.com/books/v1/volumes"
                params = {
                    'q': f"inauthor:{author_name}",
                    'langRestrict': 'en',
                    'startIndex': start_index
                }
                response = requests.get(base_api_link, params=params)
                if response.status_code != 200:
                    return []
                data = response.json()

                if 'totalItems' not in data:
                    return []
                if not total_items:
                    total_items = data['totalItems']
                books.extend(data['items'])
                start_index += 40
            return books
        except RequestException as e:
            print(f"An error occurred: {e}")

            return []

    def get_author_info_from_dbpedia(self, author_name: str) -> dict | bool:
        self.__requests += 1
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
            ?countryCode
            ?deathDate
            (GROUP_CONCAT(DISTINCT ?genre; separator=", ") as ?genres)
            (GROUP_CONCAT(DISTINCT ?influenced; separator=", ") as ?influenced)
            (GROUP_CONCAT(DISTINCT ?influencedBy; separator=", ") as ?influencedBys)
        WHERE {
            {
                SELECT DISTINCT ?author ?abstract ?birthDate ?countryCode ?deathDate ?genre ?influenced ?influencedBy (SAMPLE(?isWriter) as ?writerSample)
                WHERE {
                    ?author dbp:name "%s"@en ;
                            dbo:abstract ?abstract .
                    OPTIONAL { ?author a dbo:Writer . BIND(true AS ?isWriter) }
                    OPTIONAL { ?author dbo:birthDate ?birthDate . }
                    OPTIONAL { 
                        ?author dbo:birthPlace ?birthPlace .
                        {
                            ?birthPlace dbo:iso31661Code?countryCode .
                        }
                        UNION
                        {
                            ?birthPlace a dbo:Country .
                            ?birthPlace dbo:iso31661Code?countryCode .
                        }
                    }
                    OPTIONAL { ?author dbo:deathDate ?deathDate . }
                    OPTIONAL { ?author dbo:genre ?genre . }
                    OPTIONAL { ?author dbo:influenced ?influenced . }
                    OPTIONAL { ?author dbo:influencedBy ?influencedBy . }
                    FILTER (lang(?abstract) = 'en')
                }
                GROUP BY ?author ?abstract ?birthDate ?countryCode ?deathDate ?genre ?influenced ?influencedBy
            }
            FILTER (!BOUND(?writerSample) || ?writerSample = true)
        }
        GROUP BY ?abstract ?birthDate ?countryCode ?deathDate
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


if __name__ == '__main__':
    data_retriever = DataRetriever()
    print(data_retriever.get_author_info_from_dbpedia('JJ Smith'))
