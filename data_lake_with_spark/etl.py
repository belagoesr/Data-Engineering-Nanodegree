import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, date_format, dayofmonth, hour, dayofweek, weekofyear
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Creates a Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Loads songs data from S3 and processes it into songs and artists tables. 
        Save results into a S3 bucket.
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/A/B/C/*.json'#"song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    
    df.createOrReplaceTempView('songs')
    
    # extract columns to create songs table
    songs_table = spark.sql("""
        SELECT DISTINCT (song_id), 
            title, 
            artist_id, 
            year, 
            duration 
        FROM songs
        DISTRIBUTE BY (year, artist_id)
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/songs.parquet')

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT DISTINCT(artist_id), 
            artist_name, 
            artist_location, 
            artist_latitude, 
            artist_longitude
        FROM songs
    """)
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    """
        Loads log data from S3 and processes it into users, time and songplay tables. 
        Save results into a S3 bucket. 
        Depedencies: reads a parquet file representing the songs table created by process_song_data function.
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/2018/11/*.json'#"log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    df.createOrReplaceTempView("events")
    
    # filter by actions for song plays and create timestamp column from original timestamp column
    df = spark.sql("""
        SELECT *,
        CAST(events.ts/1000 as Timestamp) AS start_time
        FROM events
        WHERE page='NextSong'
    """)
    df.createOrReplaceTempView("events")

    # extract columns for users table    
    users_table = spark.sql("""
        SELECT DISTINCT(userId) AS user_id, 
            firstName AS first_name, 
            lastName AS last_name, 
            gender, 
            level 
        FROM events
    """);
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/')


    # extract columns to create time table
    time_table = spark.sql("""   
        SELECT DISTINCT(start_time),
                hour(start_time) AS hour,
                dayofmonth(start_time) AS day,
                month(start_time) AS month,
                weekofyear(start_time) AS week,
                year(start_time) AS year,
                dayofweek(start_time) AS weekday
        FROM events
        DISTRIBUTE BY (year, month)
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + 'time/')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs/songs.parquet")
    song_df.createOrReplaceTempView("songs")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT DISTINCT(start_time), 
            e.userId as user_id,
            s.song_id,
            s.artist_id,
            e.level, 
            e.sessionId AS session_id, 
            e.location, 
            e.userAgent AS user_agent,
            year(start_time) as year,
            month(start_time) as month
        FROM events e
        JOIN songs s
        ON s.title=e.song
        DISTRIBUTE BY (year, month)    
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays/')


def main():
    """ 
        Read songs and events data from S3 buckets, process it into tables format, and load it back to S3 
        using parquet format
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-dend-igrc/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
