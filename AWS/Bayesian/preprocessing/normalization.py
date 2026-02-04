from sklearn.preprocessing import MinMaxScaler

def normalize(df, columns):
    scaler = MinMaxScaler()
    df[columns] = scaler.fit_transform(df[columns])
    return df
