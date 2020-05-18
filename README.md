# Project: Data Lake

## Project Summary
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project I built an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.


## Data Source
The data sources to ingest into data lake are provided by two public S3 buckets:

Songs bucket (s3://udacity-dend/song_data), contains info about songs and artists. All files are in the same directory.

Event bucket (s3://udacity-dend/log_data), contains info about actions done by users, what song are listening, ...

## How to run
    1. Replace AWS IAM Credentials in dl.cfg
    2. In the terminal, run:
       python etl.py

## Dimensional Tables in the data lake
### TABLE songs

root

 |-- song_id: string (nullable = true)
 
 |-- title: string (nullable = true)
 
 |-- artist_id: string (nullable = true)
 
 |-- year: long (nullable = true)
 
 |-- duration: double (nullable = true)
 
partitionBy("year", "artist_id")


### TABLE artists
 
 root
 
 |-- artist_id: string (nullable = true)
 
 |-- name: string (nullable = true)
 
 |-- location: string (nullable = true)
 
 |-- latitude: double (nullable = true)
 
 |-- longitude: double (nullable = true)
 
 
### TABLE users

root

 |-- user_id: string (nullable = true)
 
 |-- first_name: string (nullable = true)
 
 |-- last_name: string (nullable = true)
 
 |-- gender: string (nullable = true)
 
 |-- level: string (nullable = true)
 
 
### TABLE time

root

 |-- start_time: timestamp (nullable = true)
 
 |-- hour: integer (nullable = true)
 
 |-- day: integer (nullable = true)
 
 |-- week: integer (nullable = true)
 
 |-- month: integer (nullable = true)
 
 |-- year: integer (nullable = true)
 
 |-- weekday: integer (nullable = true)
 
partitionBy("year", "month")


## Fact table

### TABLE songplays

root

 |-- songplay_id: long (nullable = true)
 
 |-- start_time: timestamp (nullable = true)
 
 |-- user_id: string (nullable = true)
 
 |-- level: string (nullable = true)
 
 |-- song_id: string (nullable = true)
 
 |-- artist_id: string (nullable = true)
 
 |-- session_id: long (nullable = true)
 
 |-- location: string (nullable = true)
 
 |-- user_agent: string (nullable = true)
 
partitionBy("year", "month")
