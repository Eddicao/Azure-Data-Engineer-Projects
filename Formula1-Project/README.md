
# Formula1 Project

This is the end-to-end data engineering process on the Azure platform, using Azure Data Lake, DataBricks, Azure Data Factory, Delta Lake and Pyspark & Spark SQL, Power BI.

+ mainly use Databricks create an automated data pipeline to ingest and transform the JSON/CSV files & folders into parquet format, aimed at making the data available for further analysis as well as presentation.

## Overview of the projects

#### Required Information

The Formula 1 racing project involves processing data for both **driver champions & constructor champions**. Points are awarded based on the finishing position in each race, and there are separate championships for drivers and constructors. Pole positions are determined through qualifying races.

#### Data Source

The data is sourced from the Ergest Developer API, providing tables such as *circuits, races, constructors, drivers, results, pitstops, lap times, and qualifying.*

[ER Diagram](http://ergast.com/images/ergast_db.png)

[Files' details](https://drive.google.com/file/d/1I-YGROsBYqRZtLlS4prJU1xYfMNz4wV5/view?usp=drive_link)

#### Project - Solution Architecture

#### Azure Services Used
1. **Azure Databricks**: using Python & SQL to ingest & process the data.
2. **Azure Data Lake Gen2 (Delta Lake / LakeHouse)**: For hierarchical storage and utilizing delta tables.
3. **Azure Data Factory**: For pipeline orchestration for Databricks Notebook.
4. **Azure Key Vault**: For storing secrets for ADLS credentials used in the Databricks Notebook.

[Formula1 Architecture](https://drive.google.com/file/d/1hfcTdBI_cGCOJMxlif9v6ofO8j3tSr59/view?usp=drive_link)

## Project Process in details

##### Step 1: Gather the requirements and collect source data

##### Step 2: Choose the suitable solution architecture - 03 layers

##### Step 3: Setup environments on Azure Storage using ADLS Gen2 with 03 Containers
- Created Azure Data Lake Storage with three containers: raw, processed, presentation.

##### Step 4: Create Databricks Compute and initiate Databricks cluster with specific configuration

##### Step 5: Grant access from Databricks to Azure Data Lake using Service Principal & Key Vault
- Mounted Azure Data Lake using Service Principal & Key Vault for secure access.

##### Step 6: Ingest and transformed data from CSV/JSON files & folders into Parquet formats on Databricks
- Ingested eight types of files and folders from the **raw container**.
- Created separate notebooks for ingestion and converted raw data into processed data, save in the **processed container**.

##### Step 7: Create Databricks workflow in Databricks / data pipelines in Azure Data Factory
- Used processed data to perform further transformation and analysis.
- Created notebooks for **race results, driver standings, constructor standings**, and calculated race results.

##### Step 8: Create database and tables ready for data analysis / visualization
- Stored data generated from processed notebooks in Parquet format - save in the presentation-layer container.
- Analyzed and visualized dominant drivers and dominant teams.

#### Reference:
The project is built based on the valuable instructions from the great course taught by Mr Ramesh on Udemy:
- Databricks course: [Azure Databricks & Spark For Data Engineers (PySpark / SQL)](https://www.udemy.com/course/azure-databricks-spark-core-for-data-engineers/learn/lecture/37939572?start=0#overview).