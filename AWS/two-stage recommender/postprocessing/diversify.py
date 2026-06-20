import math
import re
import pandas as pd


def _family_key(row) -> str:
    provider = row["provider"].lower()
    itype    = row["instanceType"]

    if provider == "aws":
        return f"aws:{itype.split('.')[0]}"

    if provider == "azure":
        m = re.match(r"([A-Za-z_]+)", itype)
        prefix = m.group(1).rstrip("_") if m else itype[:8]
        return f"azure:{prefix}"

    if provider == "gcp":
        return f"gcp:{itype.split('-')[0]}"

    return f"{provider}:{itype[:8]}"


def diversify(df: pd.DataFrame, per_family: int = 2, top_n: int = 10) -> pd.DataFrame:
    if df.empty:
        return df

    # ── Deduplicate: keep only the highest-scored row per instanceType ──
    df = (
        df.sort_values("final_score", ascending=False)
          .drop_duplicates(subset=["instanceType"], keep="first")
    )

    providers    = df["provider"].unique().tolist()
    n_providers  = len(providers)
    per_provider = max(1, math.floor(top_n / n_providers))

    result_idx  = []
    fam_counts  = {}
    prov_counts = {p: 0 for p in providers}

    # Pass 1 — guaranteed quota per provider
    for provider in providers:
        for idx, row in df[df["provider"] == provider].iterrows():
            if prov_counts[provider] >= per_provider:
                break
            k = _family_key(row)
            fam_counts.setdefault(k, 0)
            if fam_counts[k] < per_family:
                result_idx.append(idx)
                fam_counts[k] += 1
                prov_counts[provider] += 1

    # Pass 2 — fill remaining slots in global score order
    for idx, row in df.iterrows():
        if len(result_idx) >= top_n:
            break
        if idx in result_idx:
            continue
        k = _family_key(row)
        fam_counts.setdefault(k, 0)
        if fam_counts[k] < per_family:
            result_idx.append(idx)
            fam_counts[k] += 1

    return df.loc[result_idx]