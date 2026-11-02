# ==============================================================================
# PIPELINE: Healthcare Claims Deduplication & HIPAA Data Scrubbing
# AUTHOR: Abhishek Das (Senior Data & Analytics Engineer)
# DESCRIPTION: Scrubs PHI/PII fields and validates ICD-10 medical claims data
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, current_timestamp, when, regexp_replace

# 1. Read Raw Claims Data from Bronze Lakehouse Layer
claims_bronze_df = spark.read.format("delta").table("healthcare_lakehouse.bronze_medicare_claims")

# 2. Hash Patient HICN/SSN for HIPAA compliance and standardize ICD-10 codes
claims_silver_df = (
    claims_bronze_df
    .dropDuplicates(["claim_id", "member_id"])
    .withColumn("hashed_member_id", sha2(col("member_id"), 256))
    .drop("member_id", "patient_ssn") # Remove raw PHI fields for HIPAA compliance
    .withColumn("icd10_diagnosis_code", regexp_replace(col("icd10_code"), "[^A-Za-z0-9]", ""))
    .withColumn("claim_status", when(col("paid_amount") > 0, "APPROVED").otherwise("DENIED"))
    .withColumn("silver_processed_at", current_timestamp())
)

# 3. Write Cleansed Data to Silver Layer
claims_silver_df.write.format("delta").mode("overwrite").saveAsTable("healthcare_lakehouse.silver_claims")
print("HIPAA Scrubbed Claims successfully written to Silver Lakehouse Layer.")
