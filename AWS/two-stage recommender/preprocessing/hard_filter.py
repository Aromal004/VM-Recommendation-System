def hard_filter(df, req):
    df = df.copy()

    df = df[df["compute_score"] >= req["required_compute"]]
    df = df[df["memory_gib"]    >= req["memory_gib"]]

    # Only apply network filter if a real requirement exists
    if req.get("network_mbps", 0) > 0:
        df = df[df["network_mbps"] >= req["network_mbps"]]

    if "max_price" in req and req["max_price"] > 0:
        df = df[df["price_per_hr"] <= req["max_price"]]

    return df