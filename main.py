from CombineCSVFiles import combine_csv_files
from ConvertJSONToCSV import json_to_csv
from Utils import explore_csv_headers, check_duplicate_titles, check_num_of_rows


# 1) Convert the fourth dataset from JSON file to CSV
# 2) Explore the csv headers
# 3) Combine the 4 CSV files into a single CSV file
# Note: Country of publication has to be engineered
# 4) Check how many duplicate titles there are in the combined dataset
if __name__ == '__main__':
    # Dataset files
    directory = 'datasets/'
    file1 = directory + 'amazon_bs_20102020.csv'
    file2 = directory + 'Amazon_popular_books_dataset.csv'
    file3 = directory + 'bestsellers.csv'
    file4 = directory + 'nyt1.json'

    desired_file4 = directory + 'nyt2.csv'

    json_to_csv(file4, desired_file4)
    file4 = desired_file4

    csv_files = [file1, file2, file3, file4]
    explore_csv_headers(csv_files)

    # Combine the 4 CSV files into a single CSV file
    csv_headers_to_keep = ['title', 'author', 'isbn10', 'isbn13', 'country_of_publication', 'rank', 'rating']
    key_fields = ['isbn10', 'isbn13', 'title']
    output_file = directory + 'combined.csv'
    combine_csv_files(csv_files, csv_headers_to_keep, key_fields, output_file)

    check_num_of_rows(output_file)
    check_duplicate_titles(output_file)
