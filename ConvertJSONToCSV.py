import json
import csv
from pathlib import Path


def json_to_csv(json_file, csv_file):
    # If csv file already exists, return
    csv_file = Path(csv_file)
    if csv_file.exists():
        print(f'CSV file {csv_file} already exists!')
        return
    # Define the CSV file headers
    headers = ['title', 'author', 'rank', 'amazon_product_url']

    # Open CSV file for writing
    with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        # Write the headers
        csvwriter.writerow(headers)

        # Read JSON data line by line
        with open(json_file, 'r', encoding='utf-8') as f:
            for line in f:
                entry = json.loads(line)
                row = [
                    entry.get('title', ''),
                    entry.get('author', ''),
                    entry['rank'].get('$numberInt', '') if 'rank' in entry else '',
                    entry.get('amazon_product_url', '')
                ]
                csvwriter.writerow(row)
    print(f'CSV file {csv_file} created successfully!')
