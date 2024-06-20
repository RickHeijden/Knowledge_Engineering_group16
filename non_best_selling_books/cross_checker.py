"""
Goal: Cross-check the generated non-best selling books csv with the best selling one based on title containment.

Use after retrieving them -> retriever.py
"""

import pandas as pd
import os


def extract_best_selling_books(file_path: str) -> list[dict]:
    """
    @param file_path: The path to the CSV file containing the best-selling books.
    @return: A list of dictionaries containing the best-selling books.
    """
    best_selling_books_df = pd.read_csv(file_path)
    best_selling_books_to_extract = []
    for _, row in best_selling_books_df.iterrows():
        best_selling_books_to_extract.append({
            'author': row['author'],
            'title': row['title']
        })
    return best_selling_books_to_extract


def create_author_book_dict(books):
    """
    Create a dictionary mapping authors to their books.
    @param books: A list of dictionaries containing books.
    @return: A dictionary mapping authors to their books.
    """
    author_book_dict = {}
    for book in books:
        author = book['author']
        title = book['title']
        if author not in author_book_dict:
            author_book_dict[author] = []
        author_book_dict[author].append(title)
    return author_book_dict


def is_contained(title1, title2):
    """
    Check if one title is contained in another.
    @param title1: The first title.
    @param title2: The second title.
    @return: true if one title is contained in another, false otherwise.
    """
    return title1 in title2 or title2 in title1


if __name__ == "__main__":
    best_selling_books = extract_best_selling_books('../datasets/combined_filtered.csv')
    non_best_selling_books_df = pd.read_csv('../datasets/non_best_selling_books.csv')

    # Create dictionaries mapping authors to their books
    best_selling_author_books = create_author_book_dict(best_selling_books)
    non_best_selling_author_books = create_author_book_dict(non_best_selling_books_df.to_dict(orient='records'))

    # Initialize a list to keep track of non-best-selling books to keep
    books_to_keep = []

    # Check for containment in best-selling books
    for _, row in non_best_selling_books_df.iterrows():
        author = row['author']
        title = row['title']
        if author in best_selling_author_books:
            is_contained_flag = False
            for best_selling_title in best_selling_author_books[author]:
                if is_contained(title, best_selling_title):
                    print(f"'{title}' by {author} is contained in '{best_selling_title}'")
                    is_contained_flag = True
                    break
            if not is_contained_flag:
                books_to_keep.append(row)
        else:
            books_to_keep.append(row)

    # Convert the list of books to keep into a DataFrame
    remaining_books_df = pd.DataFrame(books_to_keep)

    # Save the remaining books to a new CSV file
    new_file_path = '../datasets/non_best_selling_books_filtered.csv'
    directory = os.path.dirname(new_file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
    remaining_books_df.to_csv(new_file_path, index=False)
    print(f"Kept {len(books_to_keep)} out of {len(non_best_selling_books_df)}")
    print(f"Filtered non-best-selling books saved to {new_file_path}")
