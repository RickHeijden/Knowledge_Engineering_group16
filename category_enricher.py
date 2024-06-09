import pandas as pd
import requests
import os
import time


def extract_isbn(book):
    isbn13 = book.get('isbn13')
    isbn10 = book.get('isbn10')
    return isbn13, isbn10


def fetch_categories_by_ISBN(ISBN):
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


def load_intermediate_results(file_path):
    if os.path.exists(file_path):
        return pd.read_csv(file_path)
    print("Creating a new dataframe")
    return pd.DataFrame(columns=['title', 'author', 'isbn13', 'isbn10', 'rating', 'description',
                                 'publisher', 'categories', 'country_of_publication'])


def save_intermediate_results(data, save_path):
    data.to_csv(save_path, index=False)


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
                api_calls_count += 1

                # Print metrics every 50 API calls
                if api_calls_count % 50 == 0:
                    end_time = time.time()
                    duration_minutes = (end_time - start_time) / 60.0
                    api_calls_per_minute = api_calls_count / duration_minutes
                    print(f"{api_calls_count} API calls so far")
                    print(f"API calls per minute: {api_calls_per_minute:.2f}")

                # Save intermediate results every 100 API calls
                if api_calls_count % 100 == 0:
                    enriched_books_df = pd.DataFrame(enriched_books)
                    save_intermediate_results(enriched_books_df, final_results_path)

        enriched_books.append(book.to_dict())

    # Convert list to DataFrame and write to CSV
    enriched_books_df = pd.DataFrame(enriched_books)
    save_intermediate_results(enriched_books_df, final_results_path)
    print(f"Enriched best-selling books saved to '{final_results_path}'")