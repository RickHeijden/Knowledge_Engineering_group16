import csv


def explore_csv_headers(csv_files):
    unique_headers = set()
    for file in csv_files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader)
                print(f"Headers in {file}:")
                print(f"[{', '.join(headers)}]")
                print()
                # Make all headers lowercase
                headers = [header.lower() for header in headers]
                unique_headers.update(headers)
        except Exception as e:
            print(f"Error reading {file}: {e}")

    print("Unique headers across all files:")
    print(f"[{', '.join(unique_headers)}]")


def check_num_of_rows(output_file):
    with open(output_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        num_rows = sum(1 for row in reader)
        print(f"Number of rows in {output_file}: {num_rows}")


def check_duplicate_titles(output_file):
    with open(output_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        titles = [row['title'] for row in reader]
        unique_titles = set(titles)
        duplicates = len(titles) - len(unique_titles)
        print(f"Number of duplicate titles: {duplicates}")
