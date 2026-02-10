# Cloud-Native Real-Time Data Pipeline (Apache NiFi → Snowflake)

## Overview

This project demonstrates an end-to-end cloud-native data pipeline that
simulates real-time data ingestion, processing, and historical tracking
using modern data engineering tools.

Synthetic customer data is generated using Python and streamed through
Apache NiFi running in Docker on AWS EC2. Data lands in AWS S3 and is
automatically ingested into Snowflake using Snowpipe. Snowflake Streams
and Tasks are used to capture changes and maintain Slowly Changing
Dimension (SCD Type 2) history.

This project showcases orchestration, ingestion automation, and
incremental processing similar to production data engineering workflows.

------------------------------------------------------------------------

## Architecture

<img width="1031" height="691" alt="Snowflake architecture Diagram" src="https://github.com/user-attachments/assets/a6bf54ec-8d2c-4ad1-a247-e82ab3e9a602" />


Pipeline Flow:

    Docker (EC2)
     ├─ Jupyter / Faker
     ├─ Apache NiFi
     └─ Zookeeper
            ↓
          AWS S3
            ↓
         Snowpipe
            ↓
       Raw / Staging Table
            ↓
           Stream
            ↓
            Task
            ↓
       History / Target Table

------------------------------------------------------------------------

## Tech Stack

### Data Generation

-   Python\
-   Faker\
-   Jupyter Lab

### Streaming & Orchestration

-   Apache NiFi\
-   Zookeeper\
-   Docker

### Cloud Infrastructure

-   AWS EC2\
-   AWS S3\
-   IAM / Storage Integration

### Data Warehouse

-   Snowflake\
-   Snowpipe\
-   Streams\
-   Tasks\
-   Stored Procedures\
-   SQL / JavaScript

------------------------------------------------------------------------

## Project Structure

    project-root/
    │
    ├── sql/
    │   ├── tables.sql
    │   ├── stage_pipe.sql
    │   ├── stream_task.sql
    │   ├── scd_raw_task.sql
    │   └── stream_test_examples.sql
    │
    ├── docker/
    │   └── docker-compose.yml
    │
    ├── notebooks/
    │   └── faker.ipynb
    │
    ├── images/
    │   ├── architecture.png
    │   ├── nifi_flow.png
    │   └── snowflake_objects.png
    │
    ├── requirements.txt
    └── README.md

------------------------------------------------------------------------

## How It Works

1.  Python Faker generates synthetic customer records\
2.  Apache NiFi ingests and routes records\
3.  Files are written to AWS S3\
4.  Snowpipe automatically loads files into Snowflake tables\
5.  Streams track incremental table changes\
6.  Tasks execute scheduled merges and SCD logic\
7.  Historical data is maintained in target tables

------------------------------------------------------------------------

## Key Features

-   Containerized pipeline deployment\
-   Automated cloud ingestion\
-   Incremental change capture\
-   SCD Type 2 history tracking\
-   Task-based workflow automation\
-   Modular SQL organization\
-   Separation of pipeline and testing scripts

------------------------------------------------------------------------

## Setup Instructions

### Start Services

    docker-compose up -d

### Install Python Dependencies

    pip install -r requirements.txt

### Generate Streaming Data

Run the notebook:

    notebooks/faker.ipynb

### Execute Snowflake Scripts

Run SQL files in order:

1.  tables.sql\
2.  stage_pipe.sql\
3.  scd_raw_task.sql\
4.  stream_task.sql

------------------------------------------------------------------------

## Learning Outcomes

-   Designing end-to-end data pipelines\
-   Integrating open-source and cloud tools\
-   Automating ingestion with Snowpipe\
-   Implementing SCD Type 2 in Snowflake\
-   Using Streams and Tasks for incremental processing\
-   Structuring production-style repositories

------------------------------------------------------------------------

## Future Improvements

-   Kafka-based event streaming\
-   Airflow orchestration\
-   Data quality validation layer\
-   Monitoring and alerting\
-   Infrastructure as Code (Terraform)

------------------------------------------------------------------------

## Author

Tejas Bomble\
Aspiring Data Engineer focused on cloud-native pipeline development and
scalable data systems.

------------------------------------------------------------------------

## License

For educational and demonstration purposes.
