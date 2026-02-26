def hard_filter(df, req):
    df = df.copy()

    df = df[df["compute_score"] >= req["required_compute"]]
    df = df[df["memory_gib"] >= req["memory_gib"]]
    df = df[df["network_mbps"] >= req["network_mbps"]]

    if "max_price" in req:
        df = df[df["price_per_hr"] <= req["max_price"]]

    return df