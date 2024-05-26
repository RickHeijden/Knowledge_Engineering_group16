import csv
from pathlib import Path


def read_csv_to_dict(file, csv_headers_to_keep, key_fields):
    data = {}
    with open(file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        file_headers = reader.fieldnames
        for row in reader:
            # Uppercase the title for consistent keys
            if 'title' in row and row['title']:
                row['title'] = row['title'].upper()
            # Create a unique key based on available key fields
            key = tuple(row[field].upper() if field == 'title' and row[field] else row[field]
                        for field in key_fields if field in file_headers and row[field])
            if not key:
                continue
            if key not in data:
                data[key] = {field: row[field] for field in csv_headers_to_keep if field in row}
            else:
                for field in csv_headers_to_keep:
                    if field in row and row[field]:
                        data[key][field] = row[field]
    return data


def merge_records(existing_record, new_record, csv_headers_to_keep):
    for field in csv_headers_to_keep:
        if field in new_record and new_record[field] and (field not in existing_record or not existing_record[field]):
            existing_record[field] = new_record[field]
    return existing_record


def combine_csv_files(csv_files, csv_headers_to_keep, key_fields, output_file):
    output_file = Path(output_file)
    if output_file.exists():
        print(f'CSV file {output_file} already exists!')
        return
    combined_data = {}
    for file in csv_files:
        file_data = read_csv_to_dict(file, csv_headers_to_keep, key_fields)
        for key, value in file_data.items():
            matched_key = None
            # Check if exact key exists
            if key in combined_data:
                matched_key = key
            else:
                # Check for subset matches
                for existing_key in combined_data:
                    if set(existing_key).issubset(set(key)) or set(key).issubset(set(existing_key)):
                        matched_key = existing_key
                        break
            if matched_key:
                combined_data[matched_key] = merge_records(combined_data[matched_key], value, csv_headers_to_keep)
            else:
                combined_data[key] = value

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=csv_headers_to_keep)
        writer.writeheader()
        for key, value in combined_data.items():
            writer.writerow(value)


def check_for_duplicate_titles(csv_files):
    title_dict = {}
    for file in csv_files:
        with open(file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                title = row['title'].upper() if 'title' in row and row['title'] else None
                if title:
                    if title not in title_dict:
                        title_dict[title] = []
                    title_dict[title].append(row)

    for title, records in title_dict.items():
        if len(records) > 1:
            print(f"Duplicate records found for title: {title}")
            for record in records:
                print(record)
            print()


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
                unique_headers.update(headers)
        except Exception as e:
            print(f"Error reading {file}: {e}")

    print("Unique headers across all files:")
    print(f"[{', '.join(sorted(unique_headers))}]")
