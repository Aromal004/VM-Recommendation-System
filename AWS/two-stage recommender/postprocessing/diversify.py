# postprocessing/diversify.py

def diversify(df, per_family=2, top_n=10):
    """
    Select top_n instances with at most per_family per instance family.
    Deduplicates by instanceType first so identical rows don't consume
    multiple family slots.
    """
    # Drop duplicate instance types — keep the first occurrence (highest ranked)
    df = df.drop_duplicates(subset=["instanceType"], keep="first")

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