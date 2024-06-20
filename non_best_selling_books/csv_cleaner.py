"""
Goal: Clean a csv that has values on non-existing fields.

Good to use before fetching the non-best selling books from best selling authors -> retriever.py
"""


def clean_csv(file_path, cleaned_file_path):
    """
    Clean a CSV file by removing lines that do not have the expected number of columns.
    @param file_path: The path to the CSV file to clean
    @param cleaned_file_path: The path to save the cleaned data
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    # Assume the first line is the header
    header = lines[0].strip().split(',')
    num_columns = len(header)

    cleaned_lines = [lines[0]]  # Start with the header

    for line in lines[1:]:
        # Split the line into columns
        columns = line.strip().split(',')
        if len(columns) == num_columns:
            cleaned_lines.append(line)
        else:
            # Handle the case where the line does not match the expected number of columns
            print(f"Skipping line with unexpected number of columns: {line.strip()}")

    # Write the cleaned lines to a new file
    with open(cleaned_file_path, 'w', encoding='utf-8') as file:
        file.writelines(cleaned_lines)

    print(f"Cleaned data saved to {cleaned_file_path}")


# main
if __name__ == '__main__':
    clean_csv('../datasets/author_info2.csv', '../datasets/author_info2.csv')
