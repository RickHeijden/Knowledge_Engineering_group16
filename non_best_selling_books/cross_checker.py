"""
Goal: Cross check the generated non-best selling books csv with the best selling one.

Use after retrieving them -> retriever.py
"""

import pandas as pd
import requests

from non_best_selling_books.retriever import extract_best_selling_books, extract_authors


def create_author_book_dict(books):
    """
    Create a dictionary where each author is mapped to a list of their books.
    """
    author_book_dict = {}
    for book in books:
        author = book.get('author')
        title = book.get('title')
        if author not in author_book_dict:
            author_book_dict[author] = []
        author_book_dict[author].append(title)
    return author_book_dict


def is_contained(title1, title2):
    """
    Check if title1 is contained within title2 or vice versa.
    """
    return title1 in title2 or title2 in title1


if __name__ == "__main__":
    authors = extract_authors('../datasets/author_info2.csv')
    best_selling_books = extract_best_selling_books('../datasets/combined_filtered.csv')
    non_best_selling_books = []

    # Create dictionaries mapping authors to their books
    best_selling_author_books = create_author_book_dict(best_selling_books)
    non_best_selling_author_books = create_author_book_dict(non_best_selling_books)

    # Check for containment in best-selling books
    for author, books in best_selling_author_books.items():
        for title in books:
            # Check if the title is contained in any other title of the same author in non-best selling books
            for other_title in non_best_selling_author_books.get(author, []):
                if is_contained(title, other_title):
                    print(f"'{title}' is contained in '{other_title}'")

    # Check for containment in non-best selling books
    for author, books in non_best_selling_author_books.items():
        for title in books:
            # Check if the title is contained in any other title of the same author in best-selling books
            for other_title in best_selling_author_books.get(author, []):
                if is_contained(title, other_title):
                    print(f"'{title}' is contained in '{other_title}'")
