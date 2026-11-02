# ==============================================================================
# PIPELINE: Healthcare Gold Star Schema & Star Rating KPIs
# AUTHOR: Abhishek Das (Senior Data & Analytics Engineer)
# DESCRIPTION: Aggregates member retention, MLR (Medical Loss Ratio), & CMS Star Ratings
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, round

silver_claims = spark.read.format("delta").table("healthcare_lakehouse.silver_claims")

gold_mlr_kpis = (
    silver_claims
    .groupBy("plan_id", "provider_network", "claim_year")
    .agg(
        sum("paid_amount").alias("total_medical_payout"),
        count("claim_id").alias("total_claim_count"),
        round(sum("paid_amount") / sum("premium_revenue"), 4).alias("medical_loss_ratio")
    )
)

gold_mlr_kpis.write.format("delta").mode("overwrite").saveAsTable("healthcare_lakehouse.gold_plan_performance")
print("Gold Healthcare KPIs & MLR Data Model Published to OneLake.")
