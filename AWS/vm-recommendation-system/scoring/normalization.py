def normalize_columns(df, columns):
    df = df.copy()
    for col in columns:
        min_val = df[col].min()
        max_val = df[col].max()
        df[col + "_norm"] = (df[col] - min_val) / (max_val - min_val + 1e-9)
    return df
