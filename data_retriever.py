import requests
from requests.exceptions import RequestException
from requests import Response


class DataRetriever:
    @staticmethod
    def get_json_from_isbn(isbn: str):
        try:
            base_api_link: str = "https://www.googleapis.com/books/v1/volumes"
            params: dict[str, str] = {
                'q':       f"isbn:{isbn}",
                'country': 'US'
            }

            response: Response = requests.get(base_api_link, params=params)
            if response.status_code != 200:
                return False

            return response.json()
        except RequestException as e:
            print(f"An error occurred: {e}")

            return False

    @staticmethod
    def get_json_from_title_and_author(title: str, author: str):
        try:
            base_api_link: str = "https://www.googleapis.com/books/v1/volumes"
            params: dict[str, str] = {
                'q':       f"intitle:{title}+inauthor:{author}",
                'country': 'US'
            }

            response: Response = requests.get(base_api_link, params=params)
            if response.status_code != 200:
                return False

            return response.json()
        except RequestException as e:
            print(f"An error occurred: {e}")

            return False

    @staticmethod
    def get_author_info_from_dbpedia(author_name: str):
        try:
            # DBpedia SPARQL endpoint
            endpoint: str = "https://dbpedia.org/sparql"

            # Construct the SPARQL query
            query: str = f"""
            SELECT ?abstract ?birthDate ?deathDate ?influenced ?influencedBy WHERE {{
                ?author a dbo:Writer ;
                        foaf:name "{author_name}"@en ;
                        dbo:abstract ?abstract .
                OPTIONAL {{ ?author dbo:birthDate ?birthDate . }}
                OPTIONAL {{ ?author dbo:deathDate ?deathDate . }}
                OPTIONAL {{ ?author dbo:influenced ?influenced . }}
                OPTIONAL {{ ?author dbo:influencedBy ?influencedBy . }}
                FILTER (lang(?abstract) = 'en')
            }}
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
