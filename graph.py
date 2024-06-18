import pandas as pd
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, FOAF
from urllib.parse import quote_plus

from utils.category_mappings import dict_category, dict_subcategory


def _clean_author(author: str) -> str:
    if type(author) is not str:
        return ''
    return author.replace('(Author)', '').replace('(Narrator)', '').replace('Narrator)', '').replace('(Illustrator)',
                                                                                                     '').replace(
        '(Author;Narrator)', '').replace('(single author)', '')


def _filter_weird_authors(author: str) -> bool:
    return author.strip() and author != '0 more' and 'Publisher' not in author and 'Compiler' not in author and 'Editor' not in author and 'fictitious' not in author.lower()


# Author url of J. K. Rowling is J._K._Rowling
def _url_encode_author(author: str) -> str:
    return author.replace('"', '').replace(' ', '_')


class KnowledgeGraph:

    def __init__(self):
        # Create a new RDFLib graph
        self.graph = Graph()

        # Define namespaces
        self.SCHEMA = Namespace("http://schema.org/")

        # Add namespaces to the graph
        self.graph.bind("schema", self.SCHEMA)
        self.graph.bind("foaf", FOAF)

    def add_book(self, entry: pd.Series, is_bestseller: bool):
        # Column values: title, author, isbn13, isbn10, rank, rating, description, amazon_product_url, publisher, categories, year

        if not entry['isbn13']:
            return

        isbn = entry['isbn13']
        if isinstance(isbn, str) and ' ' in isbn:
            isbn = int(isbn.replace(' ', ''))
        book_uri = URIRef(self.SCHEMA[isbn])  # Book url is the isbn

        if entry['author']:
            authors = filter(_filter_weird_authors, _clean_author(entry['author']).split(';'))
            for author_name in authors:
                author_name = author_name.strip()
                author_uri = URIRef(self.SCHEMA[quote_plus(_url_encode_author(author_name))])

                if (author_uri, RDF.type, FOAF.Person) in self.graph:
                    self.graph.add((book_uri, self.SCHEMA.author, author_uri))

        self.graph.add((book_uri, self.SCHEMA.Value, Literal(isbn)))  # Add isbn as identifier
        self.graph.add((book_uri, RDF.type, self.SCHEMA.Book))
        self.add_property(book_uri, self.SCHEMA.Name, entry['title'])
        self.add_property(book_uri, self.SCHEMA.Rank, entry['rank'])
        self.add_property(book_uri, self.SCHEMA.Rating, entry['rating'])
        self.add_property(book_uri, self.SCHEMA.Description, entry['description'])
        self.graph.add((book_uri, self.SCHEMA.isBestseller, Literal(is_bestseller)))

        if entry['categories']:
            categories = entry['categories']
            if categories.startswith('[') and categories.endswith(']'):
                categories = categories[1:-1].replace('"', '').split(',')
            elif '->' in categories:
                categories = categories.split('->')
            elif '>' in categories:
                categories = categories.split('>')
            elif '""""""""' in categories:
                categories = categories.split('""""""""')
            else:
                categories = categories.split(',')

            for category in categories:
                category = category.strip().replace('"', '')
                if category == 'Books':
                    continue  # Trivial
                category_uri = URIRef(self.SCHEMA[quote_plus(category.replace(' ', '_'))])
                if not (category_uri, RDF.type, self.SCHEMA.Genre) in self.graph:
                    self.graph.add((category_uri, RDF.type, self.SCHEMA.Genre))
                    self.add_property(category_uri, self.SCHEMA.Value, category)
                    mapping_val = category.replace(' ', '').lower()
                    for key, value in dict_category.items():
                        if mapping_val in value:
                            self.graph.add((category_uri, self.SCHEMA.genreCategory, URIRef(self.SCHEMA[quote_plus(key)])))
                            break
                    for key, value in dict_subcategory.items():
                        if mapping_val in value:
                            self.graph.add((category_uri, self.SCHEMA.genreSubcategory, URIRef(self.SCHEMA[quote_plus(key)])))
                            break

                self.graph.add((book_uri, self.SCHEMA.genre, category_uri))

        if entry['publisher']:
            publisher = entry['publisher'].replace('|', ',')
            if 'national geographic' in publisher.lower():
                publisher = 'National Geographic'
            elif 'warner books' in publisher.lower():
                publisher = 'Warner Books'

            if publisher.startswith('[') and publisher.endswith(']'):
                publishers = publisher[1:-1].replace('"', '').split(',')
            elif ',' in publisher:
                publishers = publisher.split(',')
            else:
                publishers = [publisher]

            for publisher in publishers:
                publisher = publisher.replace("'", '').replace('"', '').strip()
                publisher_uri = URIRef(self.SCHEMA[quote_plus('publ_' + publisher.strip().replace(' ', '_'))])

                if not (publisher_uri, RDF.type, self.SCHEMA.Publisher) in self.graph:
                    self.graph.add((publisher_uri, RDF.type, self.SCHEMA.Publisher))
                    self.add_property(publisher_uri, self.SCHEMA.Value, publisher)

                self.graph.add((book_uri, self.SCHEMA.publisher, publisher_uri))

        if entry['year']:
            year = entry['year']
            if '-' in year:
                year = year.split('-')[0]
            elif '/' in year:
                year = year.split('/')[2]

            year_uri = URIRef(self.SCHEMA[year])

            if not (year_uri, RDF.type, self.SCHEMA.Year) in self.graph:
                self.graph.add((year_uri, RDF.type, self.SCHEMA.Year))
                self.graph.add((year_uri, self.SCHEMA.Value, Literal(year)))

            self.graph.add((book_uri, self.SCHEMA.year, year_uri))

    def add_property(self, subject: URIRef, predicate: URIRef, value: str):
        if not value:
            return

        if isinstance(value, str):
            filter_vals = ''.join([chr(i) for i in range(1, 32)])
            value = value.translate(str.maketrans('', '', filter_vals))
        self.graph.add((subject, predicate, Literal(value)))

    def add_author(self, entry: pd.Series):
        if not entry['author']:
            return

        # Column values: author, birth_date, birth_place, birth_countries, death_date, genres, properly_processed
        author_name = entry['author'].replace('"', '')
        author_uri = URIRef(self.SCHEMA[quote_plus(_url_encode_author(author_name))])

        if (author_uri, RDF.type, FOAF.Person) in self.graph:
            return

        self.graph.add((author_uri, RDF.type, FOAF.Person))
        # add all properties
        self.add_property(author_uri, self.SCHEMA.Value, author_name)
        self.add_property(author_uri, self.SCHEMA.BirthDate, entry['birth_date'])
        self.add_property(author_uri, self.SCHEMA.DeathDate, entry['death_date'])
        self.add_property(author_uri, self.SCHEMA.ProperlyProcessed, entry['properly_processed'])

        if entry['birth_date']:
            year = entry['birth_date']
            if '-' in year:
                year = year.split('-')[0]
            elif '/' in year:
                year = year.split('/')[2]

            year_uri = URIRef(self.SCHEMA[year])

            if not (year_uri, RDF.type, self.SCHEMA.Year) in self.graph:
                self.graph.add((year_uri, RDF.type, self.SCHEMA.Year))
                self.graph.add((year_uri, self.SCHEMA.Value, Literal(year)))

            self.graph.add((author_uri, self.SCHEMA.year, year_uri))

        if entry['birth_country']:
            birth_country = entry['birth_country'].strip()
            country_uri = URIRef(self.SCHEMA[quote_plus('country_' + birth_country.replace(' ', '_'))])
            if not (country_uri, RDF.type, self.SCHEMA.Country) in self.graph:
                self.graph.add((country_uri, RDF.type, self.SCHEMA.Country))
                self.add_property(country_uri, self.SCHEMA.Value, birth_country)

            self.graph.add((author_uri, self.SCHEMA.birthCountry, country_uri))

        if entry['genres']:
            if '""""""""' in entry['genres']:
                genres = entry['genres'].split('""""""""')
            else:
                genres = entry['genres'].split(';')

            for genre in genres:
                genre = genre.replace('_', ' ').replace('(genre)', '').strip()
                genre_uri = URIRef(self.SCHEMA[quote_plus(genre.replace(' ', '_'))])
                if not (genre_uri, RDF.type, self.SCHEMA.Genre) in self.graph:
                    self.graph.add((genre_uri, RDF.type, self.SCHEMA.Genre))
                    self.add_property(genre_uri, self.SCHEMA.Value, genre)
                    mapping_val = genre.replace(' ', '').lower()
                    for key, value in dict_category.items():
                        if mapping_val in value:
                            self.graph.add((genre_uri, self.SCHEMA.genreCategory, URIRef(self.SCHEMA[quote_plus(key)])))
                            break
                    for key, value in dict_subcategory.items():
                        if mapping_val in value:
                            self.graph.add((genre_uri, self.SCHEMA.genreSubcategory, URIRef(self.SCHEMA[quote_plus(key)])))
                            break

                self.graph.add((author_uri, self.SCHEMA.genre, genre_uri))

    def load_authors_csv(self, file: str):
        df = pd.read_csv(file).fillna(False)
        for index, row in df.iterrows():
            self.add_author(row)

    def load_books_csv(self, file: str, is_bestseller: bool):
        df = pd.read_csv(file).fillna(False)
        for index, row in df.iterrows():
            self.add_book(row, is_bestseller)

    def create_top_categories(self):
        genres = list(dict_subcategory.keys())
        genres.extend(dict_category.keys())
        for genre in genres:
            genre_uri = URIRef(self.SCHEMA[quote_plus(genre)])
            self.graph.add((genre_uri, RDF.type, self.SCHEMA.Genre))
            self.add_property(genre_uri, self.SCHEMA.Value, genre)


if __name__ == '__main__':
    kg = KnowledgeGraph()
    kg.create_top_categories()
    kg.load_authors_csv('datasets/author_info2.csv')
    kg.load_books_csv('combiner/processed_combined_filtered_enriched.csv', True)
    kg.load_books_csv('datasets/processed_nonbestsellers.csv', False)
    # kg.load_books_csv('datasets/non_bestseller_processed.csv', False)
    kg.graph.serialize('graph.rdf', format='xml')
    print("Graph saved as graph.rdf")
