from preprocessing.loader import load_data
from preprocessing.filter import filter_vms
from preprocessing.feature_engineering import feature_engineering
from scoring.normalization import normalize_columns
from scoring.scorer import score_vms
from recommender.recommend import recommend_vms

workload = {
    "workload_type": "cpu_intensive",
    "min_vcpu": 8,
    "min_memory": 16,
    "gpu_required": False,
    "operating_system": "Linux",
    "region": "ap-southeast-5",
    "tenancy": "Shared",
    "budget_per_hour": 4.0
}

df = load_data("C:/Users/USER/Desktop/Final Project/AWS/aws_ec2_full_dataset.csv")

filtered_df = filter_vms(df, workload)

if filtered_df.empty:
    print("No VM satisfies the workload constraints.")
else:
    engineered_df = feature_engineering(filtered_df)

    engineered_df = normalize_columns(
        engineered_df,
        ["compute_score", "memory_score", "network_score", "cost_efficiency"]
    )

    scored_df = score_vms(engineered_df, workload["workload_type"])

    recommendations = recommend_vms(scored_df, top_k=5)

    print("\nRecommended VM Instances:\n")
    print(recommendations)
