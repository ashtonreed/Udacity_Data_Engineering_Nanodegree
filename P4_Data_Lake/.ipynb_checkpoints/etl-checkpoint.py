import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    "Instantiate a Spark Session"
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    "Load data from song data file into a separate song table and artist table (dimension tables) and save back to S3"
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data).dropDuplicates()

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').mode('overwrite').parquet(output_data + 'songs')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    """"Load data from log data file into a separate user table and time table (dimension tables), 
    along with songplays table (fact table) and save back to S3"""
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn('start_time', get_datetime('ts'))
    
    # extract columns to create time table
    time_table = df.select('start_time').withColumn('hour', hour('start_time')).withColumn('day', dayofmonth('start_time'))\
                .withColumn('week', weekofyear('start_time')).withColumn('month', month('start_time'))\
                .withColumn('year', year('start_time')).withColumn('weekday', dayofweek('start_time')).dropDuplicates()          
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').mode('overwrite').parquet(output_data + 'time')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.withColumn('songplay_id', monotonically_increasing_id()).join(song_df, song_df.title == df.song)\
    .select('songplay_id', 'start_time', col('userId').alias('user_id'), 'level', 'song_id', 'artist_id',\
    col('sessionId').alias('session_id'), 'location', col('userAgent').alias('user_agent'))\
    .withColumn('month', month('start_time')).withColumn('year', year('start_time')).dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').mode('overwrite').parquet(output_data + 'songplays')


def main():
    spark = create_spark_session()
    input_data = input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-lake-output-ash/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()