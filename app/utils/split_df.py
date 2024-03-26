def split_df(df, batch_size) -> list:
    df_list = []
    for i in range(0, len(df), batch_size):
        df_list.append(df.iloc[i:i + batch_size])
    return df_list
