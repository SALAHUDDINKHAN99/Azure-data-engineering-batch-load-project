# Azure Data Engineering Project: Azure Data Factory + Databricks + Medallion Architecture
- - - -
# 📖Overview:
This project implements a data engineering pipeline using **Azure Data Factory (ADF)**, **Azure Databricks** and supporting Azure resources.The pipeline extracts data from on-premises MySQL and SFTP, dynamically loads it into Azure Data Lake Storage Gen2 (ADLS Gen2), and processes it through a Medallion Architecture to transform raw data into business-ready insights.

- - - -

# 🏗️Architecture
This project follows Medallion Architecture **Landing**, **Bronze**, **Silver**, and **Gold** layers:

![](https://github.com/SALAHUDDINKHAN99/Azure-data-engineering-batch-load-project/blob/main/Images/Project%20Architecture.png)

  * **Landing:** Stores raw data exactly as received from source systems. Data is extracted from on-premises MySQL and SFTP sources.
  * **Bronze** (Staging): Stores raw data with minimal transformations.
  * **Silver** (Cleaned Data): Data is cleaned, normalized, and validated.
  * **Gold** (Aggregated Data): Contains refined data transformed into business-ready insights.
* The pipeline extracts data from on-premises sources like **MySQL** and **SFTP** and dynamically loads it into **Azure Data Lake Storage Gen2 (ADLS Gen2)**.
* Use **Azure Key Vault** to store secrets and credentials.
* Use **Azure Logic Apps** for automated email notifications regarding pipeline statuses.
* Use **Azure Databricks** for logging, transformations and dynamically passing of parameters.

- - - -

## Tools and Technologies
* **Azure Data Factory (ADF):** Data ingestion and pipeline orchestration.
* **Azure Databricks:** Data transformation and logging.
* **Azure Data Lake Storage Gen2 (ADLS Gen2):** Data storage.
* **Azure Key Vault:** Secure storage of secrets and credentials.
* **Azure Logic Apps:** Automated email notifications.
* **Integration Runtime (IR):** Connectivity to on-premises data sources.

- - - -

## Key Features:
**Dynamic Parameterization:** Source details and parameters are stored in tables to avoid hardcoding pipeline configurations.<br/>
**Automated Logging:** Databricks logs activity details after each operation.<br/>
**Email Notifications:** Logic Apps trigger alerts automatically based on pipeline status.<br/>

- - - -

# 🚀Requirements:


## Linked Services:
A linked service is a connection to a specific service or data store that can either be a source of data, or a destination (also called target or sink). In other words, it is much like connection strings, which define the connection information needed for the service to connect to external resources or data sources.

#### Required Linked Services (ls) for this project:<br/>
**ls_mysql ->** To establish connection between ADF and MySQL data source.<br/>
**ls_sftp ->** To establish connection between ADF and SFTP data source.<br/>
**ls_adls ->** To establish connection between ADF and ADLS Gen2 storage account.<br/>
**ls_databricks_compute ->** To establish connection between ADF and Azure Databricks.<br/>
**ls_databricks_delta ->** To establish connection between ADF and Delta Tables stored in Datalake.<br/>
**ls_keyvault ->** To establish connection between ADF and Azure Key Vault to access the secrets stored in it.<br/>

- - - -


## Datasets:
Datasets in ADF serve as reference points or views of the actual data to be used in data integration activities. They provide the structure and metadata for the data within ADF. However, to populate a dataset with actual data, a connection to the corresponding data storage is required using a linked service.

#### Required Datasets (ds) for this project:<br/>
![](https://github.com/SALAHUDDINKHAN99/Azure-data-engineering-batch-load-project/blob/main/Images/Datasets.png)<br/>
**ds_input_mysql ->** to point to the data present in MySQL data source.<br/>
**ds_input_sftp ->** to point to the data present in SFTP data source.<br/>
**ds_metadata_add_deltalake ->** to point to the data/metadata tables present in deltalake.<br/>
**ds_output_csv ->** points to the landing container in ADLS(we pull SFTP data and store in CSV format in landing container).<br/>
**ds_output_parquet->** points to the landing container in ADLS(we pull MySQL data and store in Parquet format in landing container).<br/>

- - - -


## Containers required in the storage account for the project:
![](https://github.com/SALAHUDDINKHAN99/Azure-data-engineering-batch-load-project/blob/main/Images/StorageAccount.png)

- - - -

### Authenticaton Methods for connecting between different Azure cloud services and Granting of Roles:

![](https://github.com/SALAHUDDINKHAN99/Azure-data-engineering-batch-load-project/blob/main/Images/Linked%20Services%20and%20Authentication%20Types%20inside%20Azure%20Cloud.jpg)

* Grant **Contributor** role to Data Factory from Databricks because this role allows Azure Data Factory to have comprehensive management permissions including creating, updating, and managing data pipelines that interact with Databricks workspaces.<br/>
* Grant **Storage Blob Data Contributor** role to Data Factory from ADLS Gen2 storage account so that ADF can have permission to read, write, and manage data stored in Azure Data Lake Storage Gen2 containers.<br/>
* Grant **Storage Blob Data Contributor** role to Access Connector(i.e. used to establish connection between Databricks and ADLS) from ADLS Gen2 storage account so that the Databricks can have permission to read, write and manage data stored in Azure Data Lake Storage Gen2 containers.<br/>

- - - -

## Databricks

#### Here are the creation scripts for various tables and processes within Databricks, including metadata tables, log tables, and bronze, silver, and gold tables, along with an archive files script for managing data flow.
- **[Metadata Table Creation Script](https://github.com/SALAHUDDINKHAN99/Azure-data-engineering-batch-load-project/blob/main/Scripts/METADATA%20Tables.ipynb):** Create metadata tables in Databricks so that we can pass the values from the delta tables created into the ADF pipeline dynamically from databricks.<br/>
- **[Log Table Creation Script](https://github.com/SALAHUDDINKHAN99/Azure-data-engineering-batch-load-project/blob/main/Scripts/log%20table%20creation.ipynb):** Create Log tables in Databricks so that we can log dynamically the details of each activity performed in ADF.<br/>
- **[Archive Files Script](https://github.com/SALAHUDDINKHAN99/Azure-data-engineering-batch-load-project/blob/main/Scripts/archive%20files%20scripts.ipynb):** Source files are archived once the data is moved into bronze layer from landing container.<br/>
- **[Bronze Tables Creation Sript](https://github.com/SALAHUDDINKHAN99/Azure-data-engineering-batch-load-project/blob/main/Scripts/bronze_tables_creation_scripts.ipynb)** <br/>
- **[Silver Tables Creation Sript](https://github.com/SALAHUDDINKHAN99/Azure-data-engineering-batch-load-project/blob/main/Scripts/silver_tables_creation_scripts.ipynb)** <br/>
- **[Gold Tables Creation Sript](https://github.com/SALAHUDDINKHAN99/Azure-data-engineering-batch-load-project/blob/main/Scripts/gold_tables_creation_scripts.ipynb)** <br/>

- - - -
# ⌛Execution:

## ETL - Extraction, Transformation & Loading of Data across Landing,Bronze,Silver & Gold layers:

- Data is extrated from `MySQL` OnPrem datasource and stored into the `landing container` in Azure Data Lake in parquet format. Similarly data is extrated from OnPrem `SFTP` source and store it in `landing container` in CSV format aswell.<br/>
- Data is then loaded into `Bronze layer` after checking the data quality and by removing the first rows if it matches the table headers.
- Data is then loaded into `Silver layer` after cleaning the data and enforcing the correct schema.
- Data is then loaded into `Gold layer` after De-normalizing the data wherever it is possible to enhance the performance while the data is being queried for extracting business insights by avoiding joins.
- Refer [this document](https://github.com/SALAHUDDINKHAN99/Azure-data-engineering-batch-load-project/blob/main/Pipline%20Execution%20Detailed%20Explaination/Project%20Over%20Flow%20in%20Detail.ipynb) for detailed explaination.

---

## 🌟 About Me

Hi there! I'm **Salahuddin Khan**, I’m an Azure Data Engineer with 2+ years of expertise in designing, developing, and optimizing data 
pipelines on Microsoft Azure.

Let's stay in touch! Feel free to connect with me:<br/><br/>
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/salahuddinkhan99)
