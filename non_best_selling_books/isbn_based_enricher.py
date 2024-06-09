import pandas as pd

from extra_data import add_country_of_publication

if __name__ == '__main__':
    working_file_path = '../datasets/non_best_selling_books_filtered.csv'
    df = pd.read_csv(working_file_path)
    add_country_of_publication(df, working_file_path)

