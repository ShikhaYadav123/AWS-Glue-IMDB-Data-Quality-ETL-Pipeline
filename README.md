**IMDB Data Quality ETL Pipeline** : 

**Technology Used** : 

Amazon Web Services, S3 (Simple Storage Service), Glue Crawler, Glue Catalog, Visual ETL, Redshift, CloudWatch, EventBridge, SNS (Social Networking Service)

**Overview** : 

I have created an ETL pipeline designed to extract data from the source S3, transform it to ensure data quality and consistency using AWS Glue, and load it into the Redshift for further analysis and reporting.

**Architecture** : 

![Architecture](https://github.com/user-attachments/assets/c6b582e0-aeb4-4036-9822-9bb8aa6fef15)

**Features** : 

1)**Data Extraction:** Utilized AWS S3 for storing raw IMDB movie data, ensuring secure and scalable storage, extracted the metadata using Glue Crawler.

2)**Data Transformation:** Employed AWS Glue for data transformation, implementing rules for data quality and consistency checks. This included handling missing values, data type conversions, and applying business rules.

3)**Data Loading:** Load transformed data into the destination Redshift table for analysis. And Loading Bad Data into S3 bucket for further analysis.

4)**Automation and Monitoring:** Integrated AWS CloudWatch for monitoring ETL job performance and logging. Set up AWS EventBridge to trigger ETL jobs based on specific events.

5)**Alerting and Notifications:** Configured AWS SNS to send notifications for ETL job statuses and failures, ensuring timely updates and quick resolution of issues.

<img width="1468" alt="imdb-glue-visual-ETL" src="https://github.com/user-attachments/assets/6d845536-075e-459e-a9c1-90457010f98f">

<img width="1468" alt="Redshift-table" src="https://github.com/user-attachments/assets/a9a2e0cd-d97f-4620-95b3-b68162177110">

**DataSet Used** : 

Here's the DataSet link - https://www.kaggle.com/datasets/thedevastator/netflix-imdb-scores





