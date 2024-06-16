import pandas as pd


def combine_dataframes(
    df_file1: pd.DataFrame,
    df_file2: pd.DataFrame,
    df_file3: pd.DataFrame,
    df_file4: pd.DataFrame
) -> pd.DataFrame:
    """
    Combines the dataframes by filling in missing columns with None values and concatenating the dataframes.

    @param df_file1: The first dataframe to use for combining.
    @param df_file2: The second dataframe to use for combining.
    @param df_file3: The third dataframe to use for combining.
    @param df_file4: The fourth dataframe to use for combining.

    @return: The combined dataframe.
    """
    columns = set(df_file1.columns) | set(df_file2.columns) | set(df_file3.columns) | set(df_file4.columns)
    dataframes = [df_file1, df_file2, df_file3, df_file4]

    for df in dataframes:
        for column in columns:
            if column not in df.columns:
                df[column] = None

    return pd.concat(dataframes, ignore_index=True)


def combine_on_isbn10(df: pd.DataFrame) -> pd.DataFrame:
    """
    Combines the dataframe on the isbn10 column by merging rows that have the same isbn10.

    @param df: The dataframe to combine on the isbn10 column.

    @return: The combined dataframe.
    """
    combined_dataframe_without_isbn10 = df[df['isbn10'].isnull()]
    df = df.groupby('isbn10', as_index=False).agg(combine_rows_series)
    df = pd.concat([df, combined_dataframe_without_isbn10], ignore_index=True)

    return df


def combine_on_isbn13(df: pd.DataFrame):
    """
    Combines the dataframe on the isbn13 column by merging rows that have the same isbn13.

    @param df: The dataframe to combine on the isbn13 column.

    @return: The combined dataframe.
    """
    combined_dataframe_without_isbn13 = df[df['isbn13'].isnull()]
    df = df.groupby('isbn13', as_index=False).agg(combine_rows_series)
    df = pd.concat([df, combined_dataframe_without_isbn13], ignore_index=True)

    return df


def combine_on_title_author(df: pd.DataFrame):
    """
    Combines the dataframe on the title and author columns by merging rows that have the same title and author.

    @param df: The dataframe to combine on the title and author columns.

    @return: The combined dataframe.
    """
    combined_dataframe_without_title_author = \
        df[df['title'].isnull() | df['author'].isnull()]
    df = df.groupby(['title', 'author'], as_index=False).agg(combine_rows_series)
    df = pd.concat([df, combined_dataframe_without_title_author], ignore_index=True)

    return df


def combine_rows_series(series: pd.Series):
    """
    Combines the rows in the series by taking the first non-null value.

    @param series: The series to combine.
    @return: The combined series.
    """
    return series.dropna().iloc[0] if not series.dropna().empty else None
