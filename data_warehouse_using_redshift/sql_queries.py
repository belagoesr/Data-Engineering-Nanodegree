import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events";
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     DOUBLE PRECISION,
        artist_longitude    DOUBLE PRECISION,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER
    )
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id INTEGER PRIMARY KEY, 
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender VARCHAR, 
        level VARCHAR
    )
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id VARCHAR PRIMARY KEY DISTKEY SORTKEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year INTEGER, 
        duration REAL NOT NULL
    )                                    
""")

artist_table_create = ("""
     CREATE TABLE artists (
         artist_id VARCHAR PRIMARY KEY DISTKEY,
         artist_name VARCHAR NOT NULL,
         artist_location VARCHAR,
         artist_latitude DOUBLE PRECISION,
         artist_longitude DOUBLE PRECISION
    )
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time TIMESTAMP PRIMARY KEY,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday INTEGER
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplay (
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,  
        start_time TIMESTAMP REFERENCES time(start_time), 
        user_id VARCHAR REFERENCES users(user_id),  
        song_id VARCHAR REFERENCES songs(song_id) DISTKEY SORTKEY, 
        artist_id VARCHAR REFERENCES artists(artist_id), 
        level VARCHAR,
        session_id INTEGER, 
        location VARCHAR, 
        user_agent VARCHAR
    )
""")


staging_events_table_create= ("""
    CREATE TABLE staging_events (
        artist              VARCHAR,
        auth                VARCHAR,
        first_name          VARCHAR,
        gender              VARCHAR,
        itemInSession       INTEGER,
        last_name           VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            VARCHAR,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        session_id          INTEGER,
        song                VARCHAR,
        status              INTEGER,
        ts                  TIMESTAMP,
        user_agent          VARCHAR,
        user_id             INTEGER 
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' format as JSON {}
    timeformat 'epochmillisecs'
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' format as JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, song_id, artist_id, level, session_id, location, user_agent)
    (SELECT DISTINCT(se.ts) as start_time, se.user_id, ss.song_id, ss.artist_id,  se.level, se.session_id, se.location, se.user_agent
    FROM staging_events se
    JOIN staging_songs ss ON ss.title=se.song
    WHERE se.page='NextSong')
""")

user_table_insert = ("""
    INSERT INTO users  
    (SELECT DISTINCT(user_id), first_name, last_name, gender, level 
    FROM staging_events
    WHERE page='NextSong')
""")

song_table_insert = ("""
    INSERT INTO songs  
    (SELECT DISTINCT(song_id), title, artist_id, year, duration
     FROM staging_songs)
""")


artist_table_insert = ("""
    INSERT INTO artists  
    (SELECT DISTINCT(artist_id), artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs)
""")

time_table_insert = ("""
    INSERT INTO time 
    (SELECT DISTINCT(start_time),
        extract(hour from start_time) AS hour,
        extract(day from start_time) AS day,
        extract(week from start_time) AS week,
        extract(month from start_time) AS month,
        extract(year from start_time) AS year,
        extract(weekday from start_time) AS weekday
    FROM songplay)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
