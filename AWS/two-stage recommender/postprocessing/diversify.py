def diversify(df, per_family=2, top_n=10):
    result = []
    counts = {}

    for _, row in df.iterrows():
        fam = row["family"]
        counts.setdefault(fam, 0)

        if counts[fam] < per_family:
            result.append(row)
            counts[fam] += 1

        if len(result) >= top_n:
            break

    return df.loc[[r.name for r in result]]