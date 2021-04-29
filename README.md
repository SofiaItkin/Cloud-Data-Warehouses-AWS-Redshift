# AWS DWH PROJECT SUMMRY:

This project contains ETL Pipeline development in AWS environment using Python.
The ETL Pipeline designed  to extract data from AWS S3 and transform data from staging tables to dimension and fact tables in AWS Redshift, using STAR schema.
used for analysis of data usage from a music app.

The primery Tables are:

Staging Tables -

staging_events: reads from events logs data files.

staging_songs: reads from songs data files.

Dimension Tables -

users: contains users in the music app.
songs: contains songs in the database.
artists: contains artists in the database.
time: timestamp of records in songplays broken down into specific units.
Fact Table:
songplays: records in the log data associated with song plays.

# Project Files

dwh.cfg - Config file,contains database and IAM role info.

Etl.py - Contains Python script that transform the data between the Staging tables and DWH tables using SQL and Python.

SQl_Queries.py - Contains all SQL Queries that the file "etl.py"use.

How to run:

run create_tables.py to create database and tables.

run etl.py to execute the pipeline to read data from data files and transfer to respective tables.


