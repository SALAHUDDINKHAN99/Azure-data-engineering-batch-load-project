# Azure Data Engineering Project: Azure Data Factory + Databricks + Medallion Architecture
- - - -
## Project Overview:
This project implements a data engineering pipeline using **Azure Data Factory (ADF)**, **Azure Databricks** and supporting Azure resources.The pipeline extracts data from on-premises MySQL and SFTP, dynamically loads it into Azure Data Lake Storage Gen2 (ADLS Gen2), and processes it through a Medallion Architecture to transform raw data into business-ready insights.

- - - -

## Architecture

![](https://github.com/SALAHUDDINKHAN99/Azure-data-engineering-batch-load-project/blob/main/Images/Project%20Architecture.png)

* The project follows the Medallion Architecture:
  * **Landing** (Raw Data): Data extracted from on-premises MySQL and SFTP. 
  * **Bronze** (Staging): Raw data with minimal transformations.
  * **Silver** (Cleaned Data): Data cleaned, normalized, and validated.
  * **Gold** (Aggregated Data): Data transformed into business-ready insights.
* The pipeline extracts data from on-premises sources like **MySQL** and **SFTP** and dynamically loads it into **Azure Data Lake Storage Gen2 (ADLS Gen2)**.
* It uses **Azure Key Vault** to store secrets and credentials.
* It implements **Azure Logic Apps** for automated email notifications regarding pipeline statuses.
* The pipeline leverages **Azure Databricks** for logging, transformations and dynamically passing of parameters.

- - - -

## Tools and Technologies
* **Azure Data Factory (ADF):** Data ingestion and pipeline orchestration.
* **Azure Databricks:** Data transformation and logging.
* **Azure Data Lake Storage Gen2 (ADLS Gen2):** Data storage.
* **Azure Key Vault:** Secure storage of secrets and credentials.
* **Azure Logic Apps:** Automated email notifications.
* **Integration Runtime (IR):** Connectivity to on-premises data sources.

- - - -

## Linked Services
A linked service is a connection to a specific service or data store that can either be a source of data, or a destination (also called target or sink). In other words, it is much like connection strings, which define the connection information needed for the service to connect to external resources or data sources.

#### In this project we have created:<br/>
**ls_mysql ->** To establish connection between ADF and MySQL data source.<br/>
**ls_sftp ->** To establish connection between ADF and SFTP data source.<br/>
**ls_adls ->** To establish connection between ADF and ADLS Gen2 storage account.<br/>
**ls_databricks_compute ->** To establish connection between ADF and Azure Databricks.<br/>
**ls_databricks_delta ->** To establish connection between ADF and Delta Tables stored in Datalake.<br/>
**ls_keyvault ->** To establish connection between ADF and Azure Key Vault to access the secrets stored in it.<br/>

- - - -


  




