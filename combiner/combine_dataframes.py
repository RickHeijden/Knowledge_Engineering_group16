import pandas as pd


def combine_dataframes(df_file1: pd.DataFrame, df_file2: pd.DataFrame, df_file3: pd.DataFrame, df_file4: pd.DataFrame):
    columns = set(df_file1.columns) | set(df_file2.columns) | set(df_file3.columns) | set(df_file4.columns)
    dataframes = [df_file1, df_file2, df_file3, df_file4]

    for df in dataframes:
        for column in columns:
            if column not in df.columns:
                df[column] = None

    return pd.concat(dataframes, ignore_index=True)


def combine_on_isbn10(df: pd.DataFrame):
    combined_dataframe_without_isbn10 = df[df['isbn10'].isnull()]
    df = df.groupby('isbn10', as_index=False).agg(combine_rows_series)
    df = pd.concat([df, combined_dataframe_without_isbn10], ignore_index=True)

    return df


def combine_on_isbn13(df: pd.DataFrame):
    combined_dataframe_without_isbn13 = df[df['isbn13'].isnull()]
    df = df.groupby('isbn13', as_index=False).agg(combine_rows_series)
    df = pd.concat([df, combined_dataframe_without_isbn13], ignore_index=True)

    return df


def combine_on_title_author(df: pd.DataFrame):
    combined_dataframe_without_title_author = \
        df[df['title'].isnull() | df['author'].isnull()]
    df = df.groupby(['title', 'author'], as_index=False).agg(combine_rows_series)
    df = pd.concat([df, combined_dataframe_without_title_author], ignore_index=True)

    return df

def combine_rows_series(series: pd.Series):
    return series.dropna().iloc[0] if not series.dropna().empty else None
