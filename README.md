# Currency-Exchange-Rate-Analysis


## Overview

The **Currency Exchange Rate Analysis Pipeline** is a powerful ETL (Extract, Transform, Load) solution designed to fetch, process, and analyze currency exchange rates from a reliable API. This project utilizes Apache Airflow for orchestrating the data pipeline and Snowflake for data storage and analysis. The pipeline enables users to efficiently extract exchange rate data, transform it into a structured format, and load it into a Snowflake database for further analysis.

## Diagram
![exchange rate img](exchange%20rate%20architecture.png)

## Features

- **Data Extraction**: Automatically fetch the latest currency exchange rates from the [Exchange Rates API](https://exchangeratesapi.io/).
- **Data Transformation**: Clean and format the extracted data, adding timestamps and additional metadata.
- **Data Loading**: Store the transformed data in Snowflake tables for easy access and analysis.
- **Real-Time Analysis**: Utilize Snowpipe to ingest data in real-time from an S3 bucket, allowing for immediate analysis.
- **Comprehensive Queries**: Execute complex SQL queries to analyze exchange rate trends, volatility, and averages.

## Architecture

The pipeline consists of the following components:

1. **Apache Airflow**: Manages the ETL workflow, scheduling tasks, and handling dependencies.
2. **AWS S3**: Serves as the staging area for storing extracted CSV files.
3. **Snowflake**: Acts as the data warehouse where transformed data is stored and analyzed.
4. **Exchange Rates API**: The external API used for fetching the latest currency exchange rates.

## Getting Started

### Prerequisites

- Python 3.x
- Apache Airflow
- Boto3 (for AWS S3 interaction)
- Pandas (for data manipulation)
- Snowflake Connector for Python

## Queries

The following SQL queries are provided for analyzing the exchange rate data:

1. **Latest Exchange Rate Comparison:** Compares the latest exchange rates from both tables.
2. **Percentage Change in Exchange Rates:** Calculates the percentage change in exchange rates for currencies present in both tables.
3. **Top 10 Most Volatile Currencies:** Identifies the top 10 currencies with the largest fluctuations in exchange rates.
4. **Average Exchange Rate for Each Currency:** Calculates the average exchange rate for each currency across both tables.