def filter_vms(df, workload):
    df = df.copy()

    filtered = df[
        (df["regionCode"] == workload["region"]) &
        (df["operatingSystem"].str.contains(workload["operating_system"], case=False, na=False)) &
        (df["tenancy"] == workload["tenancy"]) &
        (df["vcpu"] >= workload["min_vcpu"]) &
        (df["memory"] >= workload["min_memory"]) &
        (df["currentGeneration"] == "Yes")
    ]

    if workload["budget_per_hour"] is not None:
        filtered = filtered[
            (filtered["price_per_hr"].isna()) |
            (filtered["price_per_hr"] <= workload["budget_per_hour"])
        ]

    if workload["gpu_required"]:
        filtered = filtered[filtered["gpu"].fillna(0) > 0]

    return filtered
