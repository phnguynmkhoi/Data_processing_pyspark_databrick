# F1 Formula Data ETL Pipeline on Databricks

This repository contains an ETL (Extract, Transform, Load) pipeline built on Databricks, designed to process and analyze F1 Formula racing data. The pipeline supports both full and incremental data loads and follows a three-layer architecture: Raw, Processed, and Presentation, utilizing Databricks File System (DBFS) for storage.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Loading](#data-loading)
  - [Full Load](#full-load)
  - [Incremental Load](#incremental-load)
- [ETL Process](#etl-process)
  - [Raw Layer](#raw-layer)
  - [Processed Layer](#processed-layer)
  - [Presentation Layer](#presentation-layer)
- [Technologies Used](#technologies-used)
- 
## Overview

This ETL pipeline is designed to process F1 Formula racing data, including race results, driver statistics, lap times, pit stops, and more. Built using Databricks and Apache Spark, the pipeline efficiently handles large-scale data and organizes it into three distinct layers:

1. **Raw Layer**: Captures and stores the raw, unprocessed F1 data as it is ingested from various sources.
2. **Processed Layer**: Cleans, normalizes, and enriches the data to prepare it for analysis. The data in this layer is stored as managed tables within a local directory.
3. **Presentation Layer**: Formats the processed data for end-user consumption, including dashboards, reports, and analytical applications. Like the processed layer, this data is also saved as managed tables in a local directory. 

## Architecture

The ETL pipeline follows a three-layer architecture on Databricks:

1. **Raw Layer**: Ingests data from various sources (such as APIs, CSV files, or databases) and stores it in its original format on Databricks DBFS.
2. **Processed Layer**: Cleans, normalizes, and transforms the raw data into a structured format suitable for analysis.
3. **Presentation Layer**: Aggregates and formats the processed data to be used in reports, dashboards, and analytical models.

![Architechture](https://github.com/user-attachments/assets/36d958bb-722e-4462-98b4-bf3ecbf4d715)

## Data Description

The F1 Formula data encompasses several datasets that capture a comprehensive view of Formula 1 racing events, from race outcomes to the smallest details of lap performance:

- **Race Results**: Provides complete details for each race, including race name, location, date, finishing positions, driver and constructor names, points awarded, and total lap count.
- **Driver Statistics**: Contains essential information about the drivers, such as names, nationalities, teams, birthdates, and detailed career statistics.
- **Lap Times**: Offers granular lap-by-lap data for each driver across all races, including the lap number, time taken, and the driver's position during each lap.
- **Pit Stops**: Records details of each pit stop made during a race, including the lap number, pit stop time, and duration, providing insights into race strategies.
- **Qualifying Results**: Captures the outcomes of qualifying sessions, including the position, lap times, and details of each driver’s performance.
- **Circuit Details**: Contains data about the circuits, including circuit names, geographic locations, lengths, and the number of laps required.
- **Constructor Data**: Provides information about the constructors (teams), including their names, nationalities, and points standings throughout the season.

![Formula 1 Ergast Database Data Model](https://github.com/user-attachments/assets/3b8eab96-5a0c-403a-916e-d5c91b1e7c1a)

## Data Loading

### Full Load

The **Full Load** process involves ingesting all F1 data from the source systems. This process is utilized during the initial data load or when significant changes occur in the source data. Full loads are scheduled periodically to maintain data completeness and consistency. This method is primarily applied to relatively static datasets, such as drivers, races, circuits, and constructors, which do not frequently change.

### Incremental Load

The **Incremental Load** process focuses on ingesting only new or updated F1 data since the last successful load. This approach optimizes processing time and resource usage by employing techniques like time-based filtering. Incremental loads are mainly used for dynamic datasets such as pit stops, lap times, qualifying results, and race outcomes, which receive updates every race week. 

## ETL Process

### Raw Layer

- **Description**: Stores ingested F1 data in its original format without any transformation.
- **Data Sources**: APIs, databases, CSV files, etc.
- **Location**: Stored on Databricks DBFS at paths like `/FileStore/raw`.
- **Format**: Supports various formats like JSON, CSV, Parquet.

### Processed Layer

- **Description**: Contains cleaned and transformed F1 data for analytical purposes.
- **Transformations**: Data cleansing, normalization, enrichment (e.g., calculating average lap times, pit stop durations, and driver standings).
- **Location**: Stored on DBFS at paths like `/FileStore/processed`.
- **Format**: Optimized storage formats like Parquet and saved as managed table.

### Presentation Layer

- **Description**: Stores aggregated and formatted data for end-user consumption.
- **Usage**: Ideal for creating dashboards, generating reports, and feeding machine learning models.
- **Location**: Stored on DBFS at paths like `/FileStore/presentation`.
- **Format**: Data is stored in parquet formats and saved as managed table optimized for performance and quick retrieval, such as Delta Lake.

## Technologies Used

- **Databricks**: Unified data analytics platform for big data and machine learning.
- **Apache Spark**: Distributed data processing engine for handling large datasets.
- **Databricks DBFS**: Databricks File System for scalable and distributed storage of all data layers.
