# preprocessing/hard_filter.py
def hard_filter(df, req):
    df = df.copy()

    df = df[df["vcpu"] >= req["vcpu"]]
    df = df[df["memory_gib"] >= req["memory_gib"]]
    df = df[df["network_mbps"] >= req["network_mbps"]]

    # Modern only
    df = df[df["currentGeneration"] == "Yes"]

    # Budget cap (IMPORTANT)
    if "max_price" in req:
        df = df[df["price_per_hr"] <= req["max_price"]]

    return df
