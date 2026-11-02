# healthcare-claims-fabric-analytics
Multi-terabyte Medicare Claims Processing Engine built on Microsoft Fabric, Medallion Architecture, PySpark, and HIPAA RLS/OLS security.
# Healthcare Claims Analytics & HIPAA Governance Engine (Microsoft Fabric)
[![Microsoft Fabric](https://img.shields.io/badge/Microsoft_Fabric-Certified-0078D4?logo=microsoft)](https://learn.microsoft.com) [![HIPAA Compliance](https://img.shields.io/badge/HIPAA-Compliant_RLS/OLS-emerald)](https://cms.gov) [![PySpark](https://img.shields.io/badge/PySpark-3.4+-E25A1C?logo=apachespark)](https://spark.apache.org)

An end-to-end healthcare data engineering solution designed in **Microsoft Fabric** to process multi-terabyte Medicare & Medicaid claims data while enforcing strict HIPAA compliance standards.

## 🏗️ Architecture & Medallion Pipeline
- **Bronze Layer:** Ingests raw member eligibility records, claims logs, and pharmacy feeds from SFTP/SQL endpoints using Azure Data Factory pipelines.
- **Silver Layer:** PySpark notebooks execute SHA-256 anonymization on PHI (Patient Health Information), validate ICD-10 diagnosis codes, and deduplicate claim records.
- **Gold Layer:** Dimensional modeling of Member Retention, Medical Loss Ratio (MLR), and CMS Star Rating KPIs stored in OneLake Delta format.
- **Governance & Security:** Implements Row-Level Security (RLS) and Object-Level Security (OLS) in Power BI to restrict data visibility by provider network and territory.

## 🛠️ Tech Stack
Microsoft Fabric, Azure Data Factory, Databricks, PySpark, T-SQL, Power BI, DAX Studio, Azure DevOps.
