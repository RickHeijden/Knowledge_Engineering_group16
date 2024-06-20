import pandas as pd
import requests
import time

from non_best_selling_books.enricher import load_intermediate_results, save_intermediate_results, \
    intermediate_processing


def extract_isbn(book):
    """
    Extract ISBN-13 and ISBN-10 from the book data.
    @param book: A dictionary representing a book
    @return: A tuple containing the ISBN-13 and ISBN-10
    """
    isbn13 = book.get('isbn13')
    isbn10 = book.get('isbn10')
    return isbn13, isbn10


def fetch_categories_by_ISBN(ISBN):
    """
    Fetch categories for a book using the Open Library API.
    @param ISBN: The ISBN of the book to fetch categories for
    @return: A list of categories for the book
    """
    url = f'http://openlibrary.org/isbn/{ISBN}.json'
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()

        # Check if categories are available
        categories = data.get('subjects', [])
        if categories:
            return categories

        # If categories are missing, try to recover them from other data fields
        subjects = data.get('subject', [])  # Use 'subject' instead of 'subjects'
        if not subjects:
            subjects = data.get('subjects', [])  # Fallback to 'subjects' if 'subject' is not found

        return subjects

    return []

if __name__ == '__main__':
    best_selling_books_path = './datasets/combined_filtered.csv'
    final_results_path = './datasets/combined_filtered_enriched.csv'

    # Read the filtered non_best_selling_books.csv file
    non_best_selling_books_df = pd.read_csv(best_selling_books_path)

    # Load previously saved intermediate results
    enriched_books_df = load_intermediate_results(final_results_path)
    enriched_books_titles = set(enriched_books_df['title'].unique())

    # List to hold updated book information
    enriched_books = enriched_books_df.to_dict('records')

    api_calls_count = 0
    start_time = time.time()

    for _, book in non_best_selling_books_df.iterrows():
        if book['title'] in enriched_books_titles:
            continue

        isbn13, isbn10 = extract_isbn(book)

        # If any field is missing, fetch additional details
        if pd.isna(book['rating']) or pd.isna(book['description']) or pd.isna(book['publisher']) or pd.isna(
                book['categories']) or pd.isna(book['country_of_publication']):
            isbn_to_use = isbn13 if pd.notna(isbn13) else isbn10
            if isbn_to_use:
                categories_fetched = fetch_categories_by_ISBN(isbn_to_use)

                # Update book information with fetched details
                if len(categories_fetched) != 0:
                    book['categories'] = ', '.join(categories_fetched)
                    print("Updated categories of " + book.get('title') + " to " + str(categories_fetched))
                # Track the number of API calls
                intermediate_processing(api_calls_count, start_time, enriched_books, final_results_path)

        enriched_books.append(book.to_dict())

    # Convert list to DataFrame and write to CSV
    enriched_books_df = pd.DataFrame(enriched_books)
    save_intermediate_results(enriched_books_df, final_results_path)
    print(f"Enriched best-selling books saved to '{final_results_path}'")