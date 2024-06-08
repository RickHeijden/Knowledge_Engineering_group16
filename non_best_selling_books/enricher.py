import pandas as pd
import requests

"""
Goal: Enrich the non-best selling books dataset by fetching more info on them.

Its usage comes after the dataset is built -> retriever.py
"""


def extract_isbn(book):
    isbns = book.get('isbn', [])
    isbn13 = next((isbn for isbn in isbns if len(isbn) == 13), None)
    isbn10 = next((isbn for isbn in isbns if len(isbn) == 10), None)
    return isbn13, isbn10


def fetch_additional_details(isbn):
    url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}&key=your_google_api_key"
    response = requests.get(url)
    if response.status_code == 200:
        items = response.json().get('items', [])
        if items:
            volume_info = items[0]['volumeInfo']
            return {
                'rating': volume_info.get('averageRating'),
                'description': volume_info.get('description'),
                'publisher': volume_info.get('publisher'),
                'categories': volume_info.get('categories', []),
                'country_of_publication': volume_info.get('country')  # This may need adjustment
            }
    return {}


if __name__ == '__main__':
    # Read the non_best_selling_books.csv file
    non_best_selling_books_df = pd.read_csv('../datasets/non_best_selling_books.csv')

    # List to hold updated book information
    enriched_books = []

    for _, book in non_best_selling_books_df.iterrows():
        isbn13, isbn10 = extract_isbn(book)

        # If any field is missing, fetch additional details
        if pd.isna(book['rating']) or pd.isna(book['description']) or pd.isna(book['publisher']) or pd.isna(
                book['categories']) or pd.isna(book['country_of_publication']):
            isbn_to_use = isbn13 if pd.notna(isbn13) else isbn10
            if isbn_to_use:
                additional_details = fetch_additional_details(isbn_to_use)

                # Update book information with fetched details
                book['rating'] = additional_details.get('rating', book['rating'])
                book['description'] = additional_details.get('description', book['description'])
                book['publisher'] = additional_details.get('publisher', book['publisher'])
                book['categories'] = ', '.join(
                    additional_details.get('categories', [])) if 'categories' in additional_details else book[
                    'categories']
                book['country_of_publication'] = additional_details.get('country_of_publication',
                                                                        book['country_of_publication'])

        enriched_books.append(book)

    # Convert list to DataFrame and write to CSV
    enriched_books_df = pd.DataFrame(enriched_books)
    enriched_books_df.to_csv('../datasets/enriched_non_best_selling_books.csv', index=False)
