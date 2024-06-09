import requests
import pandas as pd
from utils.isbn13_country_mappings import get_country_from_isbn


def add_missing_data_according_to_isbn(attribute='publisher'):
    # https://e-service.nlt.go.th/File/DetailByName?fileName=E%3A%5C_files%5CFolio.Test%5CAdminDownload%5C0cc018b8-9ad6-44bd-969e-a2f95c043aef.pdf
    # ISBN13 structure
    # 978-1-56619-909-4
    # GS1 element: 978
    # Registration group element: 1
    # Registrant element: 56619
    # Publication element: 909
    # Check digit: 4

    # Open combined.csv, using pandas
    working_path = 'datasets/combined.csv'
    df = pd.read_csv(working_path)
    # Create switch-case condition for the attribute
    if attribute not in df.columns:
        clear_values_for_field(df, attribute)
    if attribute == 'publisher':
        add_publisher(df)
        return
    if attribute == 'country_of_publication':
        add_country_of_publication(df, working_path)
        return


# Add the county of publication based on the ISBN13 field
def add_country_of_publication(df, working_file_path):
    for index, row in df.iterrows():
        file_country = row["country_of_publication"]
        if pd.isna(file_country) or not file_country:
            if not row['isbn13']:
                continue
            found_country = get_country_from_isbn(row['isbn13'])
            df.at[index, 'country_of_publication'] = found_country
            print(f"Added country {found_country} to row {index}")
    # Save the csv
    df.to_csv(working_file_path, index=False)


def add_publisher(df):
    # For each row that doesn't have a publisher, use the ISBN to get the publisher
    for index, row in df.iterrows():
        file_publisher = row["publisher"]
        if pd.isna(file_publisher) or not file_publisher:
            found_publisher = get_publisher_using_api_call_on_isbn(row['isbn13'])
            if found_publisher == 'Publisher not found':
                print(f"Publisher not found for row {index}")
                continue
            df.at[index, 'publisher'] = found_publisher
            print(f"Added publisher {found_publisher} to row {index}")


# TODO: Use multithreading to speed up the process?
def get_publisher_using_api_call_on_isbn(isbn):
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
            return found_publisher
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


# Add a new field to the combined.csv file
def add_new_field(field):
    df = pd.read_csv('datasets/combined.csv')
    if field in df.columns:
        return
    clear_values_for_field(df, field)


def clear_values_for_field(df=None, field="country_of_publication"):
    if df is None:
        df = pd.read_csv('datasets/combined.csv')
    df[field] = None
    df.to_csv('datasets/combined.csv', index=False)


if __name__ == '__main__':
    isbn = "9780545153775"
    publisher = get_publisher_using_api_call_on_isbn(isbn)
    print(f"Publisher for ISBN {isbn}: {publisher}")
    add_new_field("country_of_publication")
    # clear_values_for_field(field="country_of_publication")
    add_missing_data_according_to_isbn("country_of_publication")

    # Example of a book published in Greece
    isbn = '9789609308939'
    country = get_country_from_isbn(isbn)
    print(f'The country of publication for ISBN {isbn} is: {country}')