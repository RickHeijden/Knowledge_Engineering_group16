import os
import time

import pandas as pd
import requests

"""
Goal: Retrieve non-best selling books from best-selling authors.
This script extracts all the authors from our registered best-selling books and finds (from openlibrary API) books 
written by them.
Then, it cross-checks and removes all the books contained in our dataset (which are best-selling).

It is good to clean the csv file before running this script, in case there's an error with the fields -> csv_cleaner.py
After this, we can enrich the the dataset to have similar info as the best selling one -> enricher.py 
"""


def extract_authors(file_path):
    authors_df = pd.read_csv(file_path)
    return authors_df['author'].tolist()


def save_intermediate_results(data, save_path):
    # Ensure directory exists
    directory = os.path.dirname(save_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
    data.to_csv(save_path, index=False)


def load_intermediate_results(file_path):
    if os.path.exists(file_path):
        return pd.read_csv(file_path)
    return pd.DataFrame(columns=['title', 'author', 'isbn13', 'isbn10', 'rating', 'description',
                                 'publisher', 'categories', 'country_of_publication'])


def fetch_books_by_author(author):
    url = f'http://openlibrary.org/search.json?author={author}'
    response = requests.get(url)
    if response.status_code == 200:
        books = response.json().get('docs', [])

        # Keep track of titles encountered
        titles_seen = set()
        unique_books = []

        # Filter out duplicate titles
        for book in books:
            title = book.get('title')
            if title not in titles_seen:
                unique_books.append(book)
                titles_seen.add(title)

        return unique_books

    return []


def is_bestseller(book_to_check, best_selling_books_dict):
    best_selling_author = book_to_check.get('author_name')
    title = book_to_check.get('title').lower()  # Convert title to lowercase for case-insensitive comparison
    if isinstance(best_selling_author, list):
        best_selling_author = best_selling_author[0]  # Use the first author if it's a list

    # Check if the book is in the list of best-selling books
    is_best_selling = best_selling_author in best_selling_books_dict and \
                      title in map(str.lower, best_selling_books_dict[best_selling_author])

    if is_best_selling:
        rating = book_to_check.get('rating')
        publication_year = book_to_check.get('publication_year')
        sales_count = book_to_check.get('sales_count')
        review_count = book_to_check.get('review_count')

        # Add more conditions here if needed
        is_best_selling = (
                (rating is None or rating >= 4.0) and
                (publication_year is None or publication_year >= 2010) and
                (sales_count is None or sales_count >= 1000) and
                (review_count is None or review_count >= 100)
        )

    return is_best_selling


def filter_non_best_selling_books(author_books_to_filter, best_selling_books_dict):
    non_best_selling_books_to_filter = []
    for auth_book in author_books_to_filter:
        if not is_bestseller(auth_book, best_selling_books_dict):
            non_best_selling_books_to_filter.append(auth_book)
    return non_best_selling_books_to_filter


def extract_best_selling_books(file_path):
    best_selling_books_df = pd.read_csv(file_path)
    best_selling_books_dict = {}
    for _, row in best_selling_books_df.iterrows():
        author = row['author']
        title = row['title']
        if author not in best_selling_books_dict:
            best_selling_books_dict[author] = []
        best_selling_books_dict[author].append(title)
    return best_selling_books_dict


def extract_isbn(book_to_extract_from):
    isbns = book_to_extract_from.get('isbn', [])
    if isinstance(isbns, list) and all(isinstance(isbn, str) for isbn in isbns):
        isbn13 = next((isbn for isbn in isbns if len(isbn) == 13), None)
        isbn10 = next((isbn for isbn in isbns if len(isbn) == 10), None)
    elif isinstance(isbns, list) and all(isinstance(isbn, dict) for isbn in isbns):
        isbn13 = next((isbn['identifier'] for isbn in isbns if len(isbn.get('identifier', '')) == 13), None)
        isbn10 = next((isbn['identifier'] for isbn in isbns if len(isbn.get('identifier', '')) == 10), None)
    else:
        isbn13 = isbn10 = None
    return isbn13, isbn10


if __name__ == '__main__':
    authors = extract_authors('../datasets/author_info2.csv')
    best_selling_books = extract_best_selling_books('../datasets/combined_filtered.csv')
    non_best_selling_books_path = '../datasets/non_best_selling_books.csv'
    non_best_selling_books = []

    # Load previously saved intermediate results
    non_best_selling_books_df = load_intermediate_results(non_best_selling_books_path)
    already_retrieved_authors = set()
    if len(non_best_selling_books_df) > 0:
        already_retrieved_authors = set(non_best_selling_books_df['author'].unique())

    print("Number of authors: " + str(len(authors)))
    api_calls_count = 0
    start_time = time.time()
    for author in authors:
        if author in already_retrieved_authors:
            continue
        author_books = fetch_books_by_author(author)
        api_calls_count += 1

        if api_calls_count % 50 == 0:
            end_time = time.time()
            duration_minutes = (end_time - start_time) / 60.0
            api_calls_per_minute = api_calls_count / duration_minutes
            print(str(api_calls_count) + " api calls so far")
            print("api calls per minutes: " + str(api_calls_per_minute))

        # Save intermediate results every 500 retrievals
        if api_calls_count % 500 == 0:
            df_to_append = pd.DataFrame(non_best_selling_books)
            non_best_selling_books_df = pd.concat([non_best_selling_books_df, df_to_append], ignore_index=True)
            save_intermediate_results(non_best_selling_books_df, non_best_selling_books_path)
            non_best_selling_books = []  # Reset the list after saving

        non_best_selling = filter_non_best_selling_books(author_books, best_selling_books)

        for book in non_best_selling:
            # If it's already in the data, skip it
            # If it's already in the data, skip it
            if not non_best_selling_books_df[
                (non_best_selling_books_df['title'] == book.get('title')) &
                (non_best_selling_books_df['author'] == author)
            ].empty:
                continue
            isbn13, isbn10 = extract_isbn(book)
            non_best_selling_books.append({
                'title': book.get('title'),
                'author': author,
                'isbn13': isbn13,
                'isbn10': isbn10,
                'rating': book.get('rating'),
                'description': book.get('description'),
                'publisher': book.get('publisher'),
                'categories': ', '.join(book.get('subject', [])),
                'country_of_publication': book.get('publish_country')
            })

        df_to_append = pd.DataFrame(non_best_selling_books)
        non_best_selling_books_df = pd.concat([non_best_selling_books_df, df_to_append], ignore_index=True)
        non_best_selling_books = []  # Reset the list after appending to the DataFrame
    # Save final results
    save_intermediate_results(non_best_selling_books_df, non_best_selling_books_path)
