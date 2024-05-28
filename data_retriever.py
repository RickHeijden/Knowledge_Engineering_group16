import requests
from requests.exceptions import RequestException
from requests import Response


class DataRetriever:
    @staticmethod
    def get_json_from_isbn(isbn: str) -> dict | bool:
        try:
            base_api_link: str = "https://www.googleapis.com/books/v1/volumes"
            params: dict[str, str] = {
                'q':       f"isbn:{isbn}",
                'langRestrict': 'en'
            }

            response: Response = requests.get(base_api_link, params=params)
            if response.status_code != 200:
                return False

            return response.json()
        except RequestException as e:
            print(f"An error occurred: {e}")

            return False

    @staticmethod
    def get_json_from_title_and_author(title: str, author: str | None) -> dict | bool:
        try:
            base_api_link: str = "https://www.googleapis.com/books/v1/volumes"
            if author is None:
                params: dict[str, str] = {
                    'q':       f"intitle:{title}",
                    'langRestrict': 'en'
                }
            else:
                params: dict[str, str] = {
                    'q':       f"intitle:{title}+inauthor:{author}",
                    'langRestrict': 'en'
                }

            response: Response = requests.get(base_api_link, params=params)
            if response.status_code != 200:
                return False

            return response.json()
        except RequestException as e:
            print(f"An error occurred: {e}")

            return False

    @staticmethod
    def get_books_from_author(author_name: str) -> list:
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

    @staticmethod
    def get_author_info_from_dbpedia(author_name: str) -> dict | bool:
        try:
            # DBpedia SPARQL endpoint
            endpoint: str = "https://dbpedia.org/sparql"

            # Construct the SPARQL query
            query: str = f"""
            SELECT
                ?abstract
                ?birthDate
                ?birthPlace
                (GROUP_CONCAT(DISTINCT ?birthCountry; separator=", ") as ?birthCountries)
                ?deathDate
                (GROUP_CONCAT(DISTINCT ?genre; separator=", ") as ?genres)
                ?influenced
                ?influencedBy
            WHERE {{
                ?author a dbo:Writer ;
                        foaf:name "{author_name}"@en ;
                        dbo:abstract ?abstract .
                OPTIONAL {{ ?author dbo:birthDate ?birthDate . }}
                OPTIONAL {{ ?author dbo:birthPlace ?birthPlace . }}
                OPTIONAL {{ ?author dbo:deathDate ?deathDate . }}
                OPTIONAL {{ ?author dbo:genre ?genre . }}
                OPTIONAL {{ ?author dbo:influenced ?influenced . }}
                OPTIONAL {{ ?author dbo:influencedBy ?influencedBy . }}
                OPTIONAL {{ ?birthPlace dbo:country ?birthCountry . }}
                FILTER (lang(?abstract) = 'en')
            }}
            GROUP BY ?abstract ?birthDate ?birthPlace ?deathDate ?influenced ?influencedBy
            """

            # Set the parameters for the request
            params: dict[str, str] = {
                'query':  query,
                'format': 'application/sparql-results+json'
            }

            # Make the request to the DBpedia SPARQL endpoint
            response: Response = requests.get(endpoint, params=params)
            if response.status_code != 200:
                return False

            # Return the JSON response
            return response.json()
        except RequestException as e:
            print(f"An error occurred: {e}")

            return False


if __name__ == '__main__':
    print(DataRetriever.get_author_info_from_dbpedia("J. K. Rowling"))
