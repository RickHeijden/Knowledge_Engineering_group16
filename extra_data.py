import requests
import pandas as pd


def add_data_according_to_isbn():
    # https://e-service.nlt.go.th/File/DetailByName?fileName=E%3A%5C_files%5CFolio.Test%5CAdminDownload%5C0cc018b8-9ad6-44bd-969e-a2f95c043aef.pdf
    # ISBN13 structure
    # 978-1-56619-909-4
    # GS1 element: 978
    # Registration group element: 1
    # Registrant element: 56619
    # Publication element: 909
    # Check digit: 4

    # Open combined.csv, using pandas
    df = pd.read_csv('datasets/combined.csv')
    # For each row that doesn't have a publisher, use the ISBN to get the publisher
    for index, row in df.iterrows():
        file_publisher = row["publisher"]
        if pd.isna(file_publisher) or not file_publisher:
            found_publisher = get_publisher_using_isbn(row['isbn13'])
            if found_publisher == 'Publisher not found':
                print(f"Publisher not found for row {index}")
                continue
            df.at[index, 'publisher'] = found_publisher
            print(f"Added publisher {found_publisher} to row {index}")


def get_publisher_using_isbn(isbn):
    found_publisher = get_publisher_from_open_library(isbn)
    if not found_publisher:
        found_publisher = get_publisher_from_google_books(isbn)
    return found_publisher


def get_publisher_from_google_books(isbn):
    url = f'https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if 'items' in data:
            found_publisher = data['items'][0]['volumeInfo'].get('publisher', 'Publisher not found')
            return publisher
    return 'Publisher not found'


def get_publisher_from_open_library(isbn):
    url = f'https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        key = f'ISBN:{isbn}'
        if key in data:
            found_publisher = data[key].get('publishers', [{'name': 'Publisher not found'}])[0]['name']
            return found_publisher
    return 'Publisher not found'


# TODO: Use multithreading to speed up the process
if __name__ == '__main__':
    isbn = "9780545153775"
    publisher = get_publisher_using_isbn(isbn)
    print(f"Publisher for ISBN {isbn}: {publisher}")
    add_data_according_to_isbn()