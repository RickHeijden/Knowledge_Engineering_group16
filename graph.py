import pandas as pd
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, FOAF


def _clean_author(author: str) -> str:
    if type(author) is not str:
        return ''
    return author.replace('(Author)', '').replace('(Narrator)', '').replace('(Illustrator)', '').replace(
        '(Author;Narrator)', '')


def _filter_weird_authors(author: str) -> bool:
    return author.strip() and author != '0 more' and 'Publisher' not in author and 'Compiler' not in author and 'Editor' not in author


# Author url of J. K. Rowling is J._K._Rowling
def _url_encode_author(author: str) -> str:
    return author.replace('"', '').replace(' ', '_')


class KnowledgeGraph:

    def __init__(self):
        # Create a new RDFLib graph
        self.graph = Graph()

        # Define namespaces
        self.EX = Namespace("http://example.org/")
        self.SCHEMA = Namespace("http://schema.org/")

        # Add namespaces to the graph
        self.graph.bind("ex", self.EX)
        self.graph.bind("schema", self.SCHEMA)
        self.graph.bind("foaf", FOAF)

    def add_book(self, entry: pd.Series):
        # Column values: title, author, isbn13, isbn10, rank, rating, description, amazon_product_url, publisher, categories, year

        if not entry['isbn13']:
            return

        isbn = entry['isbn13']
        book_uri = URIRef(self.EX[isbn])  # Book url is the isbn

        if entry['author']:
            authors = filter(_filter_weird_authors, _clean_author(entry['author']).split(';'))
            for author_name in authors:
                author_name = author_name.strip()
                author_uri = URIRef(self.EX[_url_encode_author(author_name)])

                if not (author_uri, RDF.type, FOAF.Person) in self.graph:
                    print(f"Author {author_name} not found in the graph")
                    return

                self.graph.add((book_uri, self.SCHEMA.Author, author_uri))

        self.graph.add((book_uri, self.SCHEMA.identifier, Literal(isbn)))  # Add isbn as identifier
        self.graph.add((book_uri, RDF.type, self.SCHEMA.book))
        self.add_property(book_uri, self.SCHEMA.Name, entry['title'])
        self.add_property(book_uri, self.SCHEMA.Rank, entry['rank'])
        self.add_property(book_uri, self.SCHEMA.Rating, entry['rating'])
        self.add_property(book_uri, self.SCHEMA.Description, entry['description'])
        self.add_property(book_uri, self.SCHEMA.AmazonProductUrl, entry['amazon_product_url'])

        if entry['categories']:
            categories = entry['categories'][1:-1].replace('"', '').split(',')

            for category in categories:
                category_uri = URIRef(self.EX[category.replace(' ', '_')])
                if not (category_uri, RDF.type, self.SCHEMA.Category) in self.graph:
                    self.graph.add((category_uri, RDF.type, self.SCHEMA.Category))
                    self.graph.add((category_uri, self.SCHEMA.name, Literal(category)))

                self.graph.add((book_uri, self.SCHEMA.category, category_uri))

        if entry['publisher']:
            publisher_uri = URIRef(self.EX[entry['publisher'].replace(' ', '_')])

            if not (publisher_uri, RDF.type, self.SCHEMA.Publisher) in self.graph:
                self.graph.add((publisher_uri, RDF.type, self.SCHEMA.Publisher))
                self.graph.add((publisher_uri, self.SCHEMA.name, Literal(entry['publisher'])))

            self.graph.add((book_uri, self.SCHEMA.publisher, publisher_uri))

        if entry['year']:
            year = entry['year']
            if '-' in year:
                year = year.split('-')[0]
            elif '/' in year:
                year = year.split('/')[2]

            year_uri = URIRef(self.EX[year])

            if not (year_uri, RDF.type, self.SCHEMA.Year) in self.graph:
                self.graph.add((year_uri, RDF.type, self.SCHEMA.Year))
                self.graph.add((year_uri, self.SCHEMA.year, Literal(year)))

            self.graph.add((book_uri, self.SCHEMA.year, year_uri))

    def add_property(self, subject: URIRef, predicate: URIRef, value: str):
        if not value:
            return

        self.graph.add((subject, predicate, Literal(value)))

    def add_author(self, entry: pd.Series):
        if not entry['author']:
            return

        # Column values: author, birth_date, birth_place, birth_countries, death_date, genres, properly_processed
        author_name = entry['author']
        author_uri = URIRef(self.EX[_url_encode_author(author_name)])

        if (author_uri, RDF.type, FOAF.Person) in self.graph:
            return

        self.graph.add((author_uri, RDF.type, FOAF.Person))
        # add all properties
        self.graph.add((author_uri, FOAF.name, Literal(author_name)))
        self.add_property(author_uri, self.EX.BirthDate, entry['birth_date'])
        self.add_property(author_uri, self.EX.BirthPlace, entry['birth_place'])
        self.add_property(author_uri, self.EX.DeathDate, entry['death_date'])
        self.add_property(author_uri, self.EX.ProperlyProcessed, entry['properly_processed'])

        if entry['birth_country']:
            country_uri = URIRef(self.EX[entry['birth_country']])
            if not (country_uri, RDF.type, self.SCHEMA.Country) in self.graph:
                self.graph.add((country_uri, RDF.type, self.SCHEMA.Country))
                self.graph.add((country_uri, self.SCHEMA.name, Literal(entry['birth_country'])))

            self.graph.add((author_uri, self.EX.birthCountry, country_uri))

        if entry['genres']:
            genres = entry['genres'].split(',')

            for genre in genres:
                genre = genre.strip()
                genre_uri = URIRef(self.EX[genre.replace(genre, '')])
                if not (genre_uri, RDF.type, self.SCHEMA.Genre) in self.graph:
                    self.graph.add((genre_uri, RDF.type, self.SCHEMA.Genre))
                    self.graph.add((genre_uri, self.SCHEMA.name, Literal(genre.replace('_', ' '))))

                self.graph.add((author_uri, self.EX.genre, genre_uri))

    def load_authors_csv(self, file: str):
        df = pd.read_csv(file).fillna(False)
        for index, row in df.iterrows():
            self.add_author(row)

    def load_books_csv(self, file: str):
        df = pd.read_csv(file).fillna(False)
        for index, row in df.iterrows():
            self.add_book(row)


if __name__ == '__main__':
    kg = KnowledgeGraph()
    kg.load_authors_csv('datasets/author_info.csv')
    kg.load_books_csv('datasets/processed.csv')
    kg.graph.serialize('graph.rdf', format='xml')
    print("Graph saved as graph.rdf")
