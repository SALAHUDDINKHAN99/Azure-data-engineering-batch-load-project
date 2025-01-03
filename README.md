# Azure Data Engineering Project: Azure Data Factory + Databricks + Medallion Architecture
- - - -
## Project Overview:
This project implements a data engineering pipeline using **Azure Data Factory (ADF)**, **Azure Databricks** and supporting Azure resources.
* The pipeline extracts data from on-premises sources like **MySQL** and **SFTP** and dynamically loads it into **Azure Data Lake Storage Gen2 (ADLS Gen2)**.
* Then it processes it through a **Medallion Architecture** to transform raw data into business-ready insights.
* It uses **Azure Key Vault** to store secrets and credentials.
* It implements **Azure Logic Apps** for automated email notifications regarding pipeline statuses.
* The pipeline leverages **Azure Databricks** for logging, transformations and dynamically passing of parameters.

- - - -

## Architecture
The project follows the Medallion Architecture:
* **Landing** (Raw Data): Data extracted from on-premises MySQL and SFTP.
* **Bronze** (Staging): Raw data with minimal transformations.
* **Silver** (Cleaned Data): Data cleaned, normalized, and validated.
* **Gold** (Aggregated Data): Data transformed into business-ready insights.

- - - -


