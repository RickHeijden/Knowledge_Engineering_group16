import requests
from requests.exceptions import RequestException


class DataRetriever:
    @staticmethod
    def get_json_from_isbn(isbn):
        try:
            base_api_link = "https://www.googleapis.com/books/v1/volumes"
            params = {
                'q':       f"isbn:{isbn}",
                'country': 'US'
            }
            response = requests.get(base_api_link, params=params)
            if response.status_code != 200:
                return False
            return response.json()
        except RequestException as e:
            print(f"An error occurred: {e}")
            return False

    @staticmethod
    def get_json_from_title_and_author(title, author):
        try:
            base_api_link = "https://www.googleapis.com/books/v1/volumes"
            params = {
                'q':       f"intitle:{title}+inauthor:{author}",
                'country': 'US'
            }
            response = requests.get(base_api_link, params=params)
            if response.status_code != 200:
                return False
            return response.json()
        except RequestException as e:
            print(f"An error occurred: {e}")
            return False

    @staticmethod
    def get_author_info_from_dbpedia(author_name):
        try:
            # DBpedia SPARQL endpoint
            endpoint = "https://dbpedia.org/sparql"

            # Construct the SPARQL query
            query = f"""
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
            params = {
                'query':  query,
                'format': 'application/sparql-results+json'
            }

            # Make the request to the DBpedia SPARQL endpoint
            response = requests.get(endpoint, params=params)
            if response.status_code != 200:
                return False

            # Return the JSON response
            return response.json()
        except RequestException as e:
            print(f"An error occurred: {e}")
            return False
