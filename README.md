**IMDB Data Quality ETL Pipeline** : 

**Technology Used** : 

Amazon Web Services, S3 (Simple Storage Service), Glue Crawler, Glue Catalog, Visual ETL, Redshift, CloudWatch, EventBridge, SNS (Social Networking Service)

**Overview** : 

I have created an ETL pipeline designed to extract data from the source S3, transform it to ensure data quality and consistency using AWS Glue, and load it into the Redshift for further analysis and reporting.

**Architecture** : 

![Architecture](https://github.com/user-attachments/assets/c6b582e0-aeb4-4036-9822-9bb8aa6fef15)

**Features** : 

1)Data Extraction: Retrieve metadata of IMDB movies and shows from the source S3 using Glue Crawler.

2)Data Transformation: Apply quality checks, cleaning, and standardization to ensure high-quality data.

3)Data Loading: Load transformed data into the destination Redshift table for analysis. And Loading Bad Data into S3 bucket for further analysis.

4)Monitoring and Logging: Monitor the ETL pipeline's performance and log any errors or anomalies for easy troubleshooting and getting alerts on the registered email account.

<img width="1468" alt="imdb-glue-visual-ETL" src="https://github.com/user-attachments/assets/6d845536-075e-459e-a9c1-90457010f98f">

<img width="1468" alt="Redshift-table" src="https://github.com/user-attachments/assets/a9a2e0cd-d97f-4620-95b3-b68162177110">

**DataSet Used** : 

Here's the DataSet link - https://www.kaggle.com/datasets/thedevastator/netflix-imdb-scores





