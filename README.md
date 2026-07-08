# AWS End-to-End Data Engineering Pipeline: Fitness Tracker

Welcome to the AWS Fitness Tracker Data Engineering repository! 🚀 This project demonstrates a comprehensive, serverless data pipeline solution, from data ingestion through a web application to generating a queryable data lake. Designed as a portfolio project, it highlights industry practices in cloud architecture, ETL processes, and event-driven data engineering.

## 🚀 Projects Requirements

### Building the Serverless Pipeline (Data Engineering)

**Objective**

Develop a modern, automated data pipeline on AWS to consolidate and process user workout logs, enabling analytical reporting and informed decision-making without managing server infrastructure.

**Specifications**

*   **Data Sources:** Ingest workout data from a custom Flask web application hosted on an EC2 instance directly into an Amazon DynamoDB transactional database.
*   **Change Data Capture (CDC):** Utilize DynamoDB Streams to trigger an AWS Lambda function, extracting and moving raw data into an S3 Staging bucket.
*   **Integration & Transformation:** Use an AWS Glue PySpark job to merge user profiles with their workout logs, resolving the data into an optimized format (Parquet with Snappy compression) inside an S3 Warehouse bucket.
*   **Data Cataloging:** Deploy an AWS Glue Crawler to automatically infer the schema from the data warehouse and populate the AWS Glue Data Catalog.
*   **Monitoring & Alerting:** Implement Amazon EventBridge to monitor ETL job states and trigger Amazon SNS to send email notifications in the event of a job failure.

### BI: Analytics & Reporting (Data Analytics)

**Objective**

Develop SQL-based analytics using Amazon Athena to deliver detailed insights into:
*   User Activity Preferences
*   Total Calories Burned per User
*   Maximum Workout Durations
*   Average Heart Rate Trends

These insights empower stakeholders to understand fitness behaviors and platform usage using standard SQL queries directly against the S3 data lake.

## 🛡️ License

This project is licensed under the MIT License. You are free to use, modify, and share this project with the proper attribution.

## 💼 About Me

Hi everyone! I'm Parn. I'm interested in Cloud Data Engineering, specifically focusing on ETL processes, data pipelines, and data warehousing. I'm currently training for this career and utilizing projects like this to study and apply AWS serverless architectures to build robust data infrastructure. For this project, I'm studying and following the YouTuber Date With Data. Thank you for the great video!