import pandas as pd


def json_to_csv(json_file, csv_file):
    df = pd.read_json(json_file, lines=True, dtype_backend='pyarrow', engine='pyarrow')
    df = df[['title', 'author', 'rank', 'amazon_product_url']]
    df['rank'] = df['rank'].apply(lambda x: x.get('$numberInt', None) if x else None)
    df.to_csv(csv_file, index=False)
    print(f'CSV file {csv_file} created successfully!')
