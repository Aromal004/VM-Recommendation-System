import requests
import pandas as pd
import time
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json


# ===========================================================
# Helper: Detect CPU vendor
# ===========================================================
def _extract_cpu_vendor(text):
    text = str(text).lower()
    if "amd" in text:
        return "AMD"
    elif "intel" in text:
        return "Intel"
    elif "arm" in text or "ampere" in text:
        return "ARM"
    else:
        return "Unknown"


# ===========================================================
# Create resilient session with retries
# ===========================================================
def create_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=5,  # increased retries
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        backoff_factor=5,  # exponential wait between retries
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


# ===========================================================
# Fetch Azure VM retail pricing
# ===========================================================
def fetch_azure_vm_pricing(limit=2000, session=None):
    if session is None:
        session = create_session()

    print("\n🔹 Fetching Azure VM retail pricing data...")
    base_url = "https://prices.azure.com/api/retail/prices?$filter=serviceName eq 'Virtual Machines'"
    next_page = base_url
    all_items = []
    series_filter = ("P", "T", "B", "D", "H", "F")
    page = 0

    with tqdm(desc="Downloading Azure VM pages", unit="page") as pbar:
        while next_page and len(all_items) < limit:
            try:
                response = session.get(next_page, timeout=60)
                response.raise_for_status()
                data = response.json()

                items = data.get("Items", [])
                filtered_items = []

                for item in items:
                    if len(all_items) >= limit:
                        break
                    sku = item.get("armSkuName", "")
                    if sku and any(sku.startswith(prefix) for prefix in series_filter):
                        filtered_items.append({
                            "VM Name": sku,
                            "Product Name": item.get("productName"),
                            "Location": item.get("armRegionName"),
                            "Unit Price (USD)": item.get("unitPrice"),
                            "Currency": item.get("currencyCode"),
                            "Meter Region": item.get("meterRegion"),
                            "CPU Vendor": _extract_cpu_vendor(item.get("productName", "")),
                            "Series": sku[:2] if len(sku) > 1 else sku,
                            "Service Family": item.get("serviceFamily"),
                            "Type": item.get("type"),
                            "Arm SKU": sku
                        })

                all_items.extend(filtered_items)
                page += 1
                tqdm.write(f"  ✓ Page {page}: {len(filtered_items)} filtered / {len(items)} total")
                pbar.update(1)

                next_page = data.get("NextPageLink")
                if not next_page or len(all_items) >= limit:
                    break
                time.sleep(1.5)  # polite delay

            except requests.exceptions.Timeout:
                tqdm.write(f"  ⚠️ Timeout on page {page + 1}, retrying next page...")
                continue
            except Exception as e:
                tqdm.write(f"  ⚠️ Error fetching Azure data: {e}")
                break

    print(f"✅ Azure VM records collected: {len(all_items)}")
    return pd.DataFrame(all_items)


# ===========================================================
# Fetch AWS EC2 instance data
# ===========================================================
def fetch_aws_instances(session=None):
    if session is None:
        session = create_session()

    print("\n🔹 Fetching AWS EC2 instance data...")

    urls = [
        "https://ec2instances.info/instances.json",
        "https://raw.githubusercontent.com/powdahound/ec2instances.info/master/www/instances.json",
    ]

    for url in urls:
        try:
            print(f"  📍 Trying: {url}")
            response = session.get(url, timeout=60)
            response.raise_for_status()
            data = response.json()

            if isinstance(data, dict):
                instances = list(data.values())
            else:
                instances = data

            if not instances:
                continue

            df = pd.DataFrame(instances)

            if "processor" in df.columns:
                df["CPU Vendor"] = df["processor"].apply(lambda x: _extract_cpu_vendor(x))
            elif "Processor" in df.columns:
                df["CPU Vendor"] = df["Processor"].apply(lambda x: _extract_cpu_vendor(x))
            else:
                df["CPU Vendor"] = "Unknown"

            print(f"✅ AWS instances collected: {len(df)}")
            return df

        except Exception as e:
            print(f"  ⚠️ Failed ({type(e).__name__}): {str(e)[:100]}")
            continue

    print("⚠️ Could not fetch AWS data.")
    return pd.DataFrame()


# ===========================================================
# Create CoreMark benchmark sample data
# ===========================================================
def create_sample_coremark_data():
    print("\n🔹 Creating CoreMark benchmark data...")
    sample_data = {
        "CPU": [
            "Intel Xeon Platinum 8490H", "Intel Xeon Gold 6338",
            "AMD EPYC 7763", "AMD EPYC 7B13", "ARM Graviton3"
        ],
        "Single-Core Score": [1980, 1720, 1880, 1820, 1850],
        "Multi-Core Score": [31400, 28400, 40200, 39200, 18500],
        "Cores": [60, 32, 64, 64, 64]
    }

    df = pd.DataFrame(sample_data)
    df["CPU Vendor"] = df["CPU"].apply(lambda x: _extract_cpu_vendor(x))
    print(f"✅ CoreMark entries: {len(df)}")
    return df


# ===========================================================
# Main execution
# ===========================================================
def main():
    session = create_session()

    print("=" * 65)
    print("   CLOUD VM + BENCHMARK DATA COLLECTOR")
    print("=" * 65)

    # --- Fetch Data ---
    azure_df = fetch_azure_vm_pricing(limit=2000, session=session)
    aws_df = fetch_aws_instances(session=session)
    coremark_df = create_sample_coremark_data()

    # --- Save Results ---
    output_file = "cloud_vm_benchmarks.xlsx"
    try:
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            if not azure_df.empty:
                azure_df.to_excel(writer, sheet_name="Azure_VMs", index=False)
                print(f"  📊 Azure sheet: {len(azure_df)} rows")
            if not aws_df.empty:
                aws_df.to_excel(writer, sheet_name="AWS_VMs", index=False)
                print(f"  📊 AWS sheet: {len(aws_df)} rows")
            if not coremark_df.empty:
                coremark_df.to_excel(writer, sheet_name="CoreMark_Scores", index=False)
                print(f"  📊 CoreMark sheet: {len(coremark_df)} rows")

        print("\n🎉 Data collection complete!")
        print(f"📁 Output file: {output_file}")

    except Exception as e:
        print(f"\n⚠️ Excel write error: {e}")
        print("Saving CSV backups...")
        if not azure_df.empty:
            azure_df.to_csv("azure_vms.csv", index=False)
        if not aws_df.empty:
            aws_df.to_csv("aws_vms.csv", index=False)
        if not coremark_df.empty:
            coremark_df.to_csv("coremark_scores.csv", index=False)
        print("✅ CSV backups saved.")


# ===========================================================
# Entry point
# ===========================================================
if __name__ == "__main__":
    main()
