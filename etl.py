import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, \
dayofweek, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['CRED']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['CRED']['AWS_SECRET_ACCESS_KEY']

# Selection attributes for songs_table 
SONG_ATTRIBUTES = ['song_id', 'title', 'artist_id', 'year', 'duration']

# SQL style expression for selecting artist attributes with correct aliases.
ARTIST_ATTRIBUTES = ['artist_id', 'artist_name as name', 'artist_location as location', \
                     'artist_latitude as latitude', 'artist_longitude as longitude']

# SQL style expression for selecting user attributes with correct aliases.
USER_ATTRIBUTES = ['UserId as user_id', 'firstName as first_name', \
                   'lastName as last_name', 'gender', 'level']

# SQL style expression for selecting attributes with correct aliases for the songlays fact table.
SONGPLAYS_ATTRIBUTES = ['start_time', 'userId as user_id', 'level', 'song_id', 'artist_id', \
                        'sessionId as session_id', 'location', 'userAgent as user_agent']


def create_spark_session():
    """Description:
    This function creates a Spark Session instance.
    
        Arguments:
            None.

        Returns:
            spark {object}: Spark session object.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    return spark


def process_song_data(spark, input_data, output_data):
    """Description: 
        This function performs ETL operations on the song data
        and populates the song dimension table.

        Key steps:
        1. Reads the song data from the AWS S3 bucket.
        2. Extracts and transforms information pertaining
           to the song table.
        3. Writes the table into parquet files.

        Arguments:
            spark {object}: Spark Session instance.
            input_data {string}: Path to AWS S3 bucket containing the input data.
            output_data {string}:Path to AWS S3 bucket containing the output data.

        Returns:
            None
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(SONG_ATTRIBUTES).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + 'songs/')

    # extract columns to create artists table
    artists_table = df.selectExpr(ARTIST_ATTRIBUTES).distinct()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/')
    
    return

    
def process_log_data(spark, input_data, output_data):
    """Description:
        This function performs ETL operations on the user log data
        and populates the various facts and dimension tables. It

        Key steps:
        1. Reads the user log data from the AWS S3 bucket.
        2. Extracts and transforms information pertaining
           to users, time and songplays tables
        3. Writes the tables into parquet files.

        Arguments:
            spark {object}: Spark Session instance.
            input_data {string}: Path to AWS S3 bucket containing the input data.
            output_data {string}:Path to AWS S3 bucket containing the output data.

        Returns:
            None
    """
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.selectExpr(USER_ATTRIBUTES).distinct()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:  datetime.fromtimestamp(x/1000). \
                        strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x:  datetime.fromtimestamp(x/1000). \
                       strftime('%Y-%m-%d'))
    df = df.withColumn('start_time', get_timestamp(df.ts))
    
    # extract columns to create time table
    df = df.withColumn('hour', hour('timestamp')) \
            .withColumn('day', dayofmonth('timestamp')) \
            .withColumn('week', weekofyear('timestamp')) \
            .withColumn('month', month('timestamp')) \
            .withColumn('year', year('timestamp')) \
            .withColumn('weekday', dayofweek('timestamp'))
    
    time_table = df.select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday').distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time/')

    # read in song data to use for songplays table
    song_data = input_data + 'song_data/*/*/*/*.json'
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.artist == song_df.artist_name, how = 'left') \
                        .selectExpr(SONGPLAYS_ATTRIBUTES) \
                        .withColumn('songplay_id', monotonically_increasing_id()) \
                        .withColumn('year', year('start_time')) \
                        .withColumn('month', month('start_time'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(output_data + 'songplays/')
    
    return
    

def main():
    """Description:
        This is the main driver which controls the flow
        of the ETL pipeline.

        Key steps:
        1. Creates a Spark Session object.
        2. Invokes the data processing routines.

        Arguments:
            None

        Return:
            None
    """
    
    spark = create_spark_session()
    input_data = "path to input S3 bucket"
    output_data = "path to output S3 bucket"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    return

    
if __name__ == "__main__":
    main()
