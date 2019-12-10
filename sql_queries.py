import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = ("DROP TABLE IF EXISTS staging_events")
staging_songs_table_drop = ("DROP TABLE IF EXISTS staging_songs")
songplay_table_drop = ("DROP TABLE IF EXISTS songplays")
user_table_drop = ("DROP TABLE IF EXISTS users")
song_table_drop = ("DROP TABLE IF EXISTS songs")
artist_table_drop = ("DROP TABLE IF EXISTS artists")
time_table_drop = ("DROP TABLE IF EXISTS time")

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE staging_events(
    event_id BIGINT IDENTITY(0,1),
    artist_name VARCHAR(255),
    auth VARCHAR(50),
    user_first_name VARCHAR(255),
    user_gender  VARCHAR(1),
    item_in_session	INTEGER,
    user_last_name VARCHAR(255),
    song_length	DOUBLE PRECISION,
    user_level VARCHAR(50),
    location VARCHAR(255),
    method VARCHAR(25),
    page VARCHAR(35),
    registration VARCHAR(50),
    session_id BIGINT,
    song_title VARCHAR(255),
    status INTEGER,
    ts VARCHAR(50),
    user_agent TEXT,
    user_id VARCHAR(100),
    PRIMARY KEY (event_id))
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs(
    song_id VARCHAR(100),
    num_songs INTEGER,
    artist_id VARCHAR(100),
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    title VARCHAR(255),
    duration DOUBLE PRECISION,
    year INTEGER,
    PRIMARY KEY (song_id))
""")


songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
    songplay_id BIGINT IDENTITY(0, 1),
    start_time TIMESTAMP, 
    user_id VARCHAR(100) REFERENCES users (user_id), 
    level VARCHAR(50), 
    song_id VARCHAR(100) REFERENCES songs (song_id), 
    artist_id VARCHAR(100) REFERENCES artists (artist_id), 
    session_id BIGINT, 
    location VARCHAR(255), 
    user_agent TEXT,
    PRIMARY KEY (songplay_id))
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(100), 
    first_name VARCHAR(255), 
    last_name VARCHAR(255), 
    gender VARCHAR(1), 
    level VARCHAR(50),
    PRIMARY KEY (user_id))
""")

song_table_create = ("""CREATE TABLE songs(
    song_id VARCHAR(100),
    title VARCHAR(255),
    artist_id VARCHAR(100) NOT NULL,
    year INTEGER,
    duration DOUBLE PRECISION,
    PRIMARY KEY (song_id))
""")

artist_table_create = ("""CREATE TABLE artists(
    artist_id VARCHAR(100),
    name VARCHAR(255),
    location VARCHAR(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    PRIMARY KEY (artist_id))
""")

time_table_create = ("""CREATE TABLE time(
    start_time TIMESTAMP,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER,
    PRIMARY KEY (start_time))
""")

# STAGING TABLES

# Populate event staging table loading from S3 Buckets using LOG_JSONPATH ('s3://udacity-dend/log_json_path.json')

staging_events_copy = ("""copy staging_events from '{}' 
    credentials 'aws_iam_role={}'
    region 'us-west-2' 
    JSON '{}'
    """).format(config.get('S3','LOG_DATA'),
                          config.get('IAM_ROLE', 'ARN'),
                          config.get('S3','LOG_JSONPATH'))

# Populate songs staging table loading from S3 Buckets using JSON Array

staging_songs_copy = ("""copy staging_songs from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    JSON 'auto'
    """).format(config.get('S3','SONG_DATA'), 
                config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

# Add users from staging table. Hereby ensure that users are unique and did listen to a song.

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)  
    SELECT DISTINCT 
        user_id,
        user_first_name,
        user_last_name,
        user_gender, 
        user_level
    FROM staging_events
    WHERE page = 'NextSong'
    AND user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")

# Add songs from staging table. Ensure that songs are unique and did not exist before.

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
    SELECT DISTINCT 
        song_id, 
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")

# Add artists from staging table. Ensure that artists are unique and did not exist before.

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
    SELECT DISTINCT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

# Add time details from staging table using the timestamp in seconds as base.

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
    SELECT DISTINCT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

# Add time details from staging table using the timestamp (column: ts) in ms from staging_events as base.
# Extract all needed time parts from datetime. Ensure that the datetime is not added twice to the table.

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT 
        start_time, 
        EXTRACT(hr from start_time) AS hour,
        EXTRACT(d from start_time) AS day,
        EXTRACT(w from start_time) AS week,
        EXTRACT(mon from start_time) AS month,
        EXTRACT(yr from start_time) AS year, 
        EXTRACT(weekday from start_time) AS weekday 
    FROM (
        SELECT DISTINCT (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') as start_time 
        FROM staging_events     
    )
    WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time)
""")

# Add information about played songs to songplay table using events and songs staging table
# Ensure that record is not loaded twice checking if user id exists already (as songplay_id is generated during insert) 

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT DISTINCT 
        (TIMESTAMP 'epoch' + (se.ts)/1000 * INTERVAL '1 second') as start_time, 
        se.user_id, 
        se.user_level,
        ss.song_id,
        ss.artist_id,
        se.session_id,
        se.location,
        se.user_agent
    FROM staging_events as se, staging_songs as ss
    WHERE se.page = 'NextSong'
    AND se.song_title = ss.title
    AND se.artist_name = ss.artist_name
    AND user_id NOT IN (SELECT DISTINCT user_id FROM songplays)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]