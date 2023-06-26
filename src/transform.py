def identify_and_remove_duplicated_data(df):
    """Method that removes identifies and removes duplicates"""

    if df.duplicated().sum() > 0:
        # identify duplicated data
        print("# of duplicated rows:", df.duplicated().sum())

        # drop the duplicated rows and keep only first appearance
        df_cleaned = df.drop_duplicates(keep='first')

        print("-" * 60)
        print("shape of data before removing duplicated date", df.shape)
        print("shape of data after removing duplicated date", df_cleaned.shape)
        print("-" * 60)

    else:

        print("No duplicate rows found")
        df_cleaned = df

    return df_cleaned