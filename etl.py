import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_date
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id
from pyspark.sql.types import TimestampType, DateType

# Read config file
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    ''' Connect to hadoop and create spark session'''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''Read song data and build etl solution 
    to create song table and artist table'''
    
    # read song data file
    song_data = input_data+'song_data/*/*/*/*.json'    
    df = spark.read.json(song_data).

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(output_data+"songs-data")

    # extract columns to create artists table
    artists_fields = ["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude"]
    artists_table = df.selectExpr(artists_fields).dropDuplicates()
    artists_table.write.mode('overwrite').parquet(output_data+"artists-data")


def process_log_data(spark, input_data, output_data):
    '''Read log data and build etl solution 
    to create users table, time table and the fact table (songplay table)'''
    
     # read log data file
    log_data = input_data+'log_data/*/*/*.json'   
    df = spark.read.json(log_data).limit(1000)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns and create users table    
    users_fields = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = df.selectExpr(users_fields).dropDuplicates()
    users_table.write.mode('overwrite').parquet(output_data+"users-data")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(col("ts")))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: to_date(x), TimestampType())
    df = df.withColumn("start_time", get_timestamp(col("ts")))
    
    # extract columns to create time table partition by year and month
    df = df.withColumn("hour", hour("timestamp")) \
                    .withColumn("day", dayofmonth("timestamp")) \
                    .withColumn("month", month("timestamp")) \
                    .withColumn("year", year("timestamp")) \
                    .withColumn("week", weekofyear("timestamp")) \
                    .withColumn("weekday", dayofweek("timestamp")) 
    time_table = df.select("start_time","hour","day","week","month","year","weekday")\
                .dropDuplicates()

    time_table.write.partitionBy("year", "month").mode('overwrite').parquet(output_data+"time-data")

    # join to song data and artist data to create songplays table
    song_df = spark.read.parquet(output_data+'songs-data/*/*/*')
    artist_df = spark.read.parquet(output_data+'artists-data/*')

    songplays = df.join(song_df, song_df.title == df.song, "inner")\
                .join(artist_df, artist_df.name == df.artist, "inner")\
                .drop(artist_df.location)
    
    # Extract songplays columns and partitioned by year and month
    songplays_table = songplays.withColumn("songplay_id", monotonically_increasing_id()) \
                       .select('songplay_id','start_time',col('userId').alias('user_id'), \
                               'level','song_id','artist_id', col('sessionId').alias('session_id'),\
                               'location', col('userAgent').alias('user_agent'),'year','month')\
                        .repartition('year', 'month')
   
    songplays_table.write.partitionBy("year","month").mode('overwrite').parquet(output_data+"songplays-data")


def main():
    '''ETL steps to create dimension tables and fact table'''
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://aws-logs-215553988149-us-east-2/elasticmapreduce/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
