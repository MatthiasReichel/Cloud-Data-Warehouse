## 1) Discuss the purpose of this database in the context of the startup Sparkify and their analytical goals.

The startup is growing making it necessary to move from local servers to an cloud environment as it allows to up- and downscale much quicker. Hereby, the company gains much more flexibiliy as it is not necessary to buy and maintain the hardware and its environment. Due to the possibiliy to scale up or down by setting up several clusters and respective nodes its possible to run only on machines which are needed at a specific time allowing to manage costs effectively. 
More specifically AWS Redshift was choosen as the database allows massive parallel processing (MPP) enabling queries to run on multiple CPUs/Nodes. In addition it comes along with column-orientated database design enabling speeding up database operations signficantly.   

## 2) State and justify your database schema design and ETL pipeline.

The ETL pipeline runs on AWS using S3 Bucket as datasource. However, the data still need to be structured before it can be loaded into the DWH.
The data DWH is deployed on AWS Redshift. To tackle the data extraction two staging tables have been created containing the events (derived from the user behaviour) as well the songs details. To avoid record based inserts the data being copied from S3 bucket using an bulk ingestion (redshift copy command). Thereby the data extractions is speeded up significantly. Finally the data is being transformed and loaded to 4 dimensional (user, time, songs, artist) and 1 fact (songplays) table following the star schema. 
By relying on Amazon Redshift EC2 machines are not needed to execute ETL process (only to point datasource to the staging area and beyond). Thereby, storage can be saved. In addition, it is possible to connect different BI apps to Amazon Redshift DB layer. 



