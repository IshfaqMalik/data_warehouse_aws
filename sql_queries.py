import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
# DROP TABLES

staging_events_table_drop = " drop table if exists staging_events "
staging_songs_table_drop = " drop table if exists staging_songs "
songplay_table_drop = " drop table if exists songplays "
user_table_drop = " drop table if exists users "
song_table_drop = " drop table if exists songs "
artist_table_drop = " drop table if exists artists"
time_table_drop = " drop table if exists time "

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR(1), 
    itemInSession INTEGER,
    lastName VARCHAR,
    length DECIMAL,
    level VARCHAR,
    location VARCHAR,
    method CHAR(3),
    page VARCHAR,
    registration VARCHAR,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER, 
    ts BIGINT,
    userAgent TEXT,
    userId INTEGER

);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
artist_id VARCHAR,
artist_latitude decimal,
artist_location text,
artist_longitude decimal,
artist_name VARCHAR,
duration decimal,
num_songs INTEGER,
song_id VARCHAR,
title VARCHAR,
year INTEGER

);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
songplay_id INT         IDENTITY(0,1)   PRIMARY KEY,
start_time TIMESTAMP NOT NULL,
user_id INT NOT NULL ,
level VARCHAR,
song_id VARCHAR NOT NULL ,
artist_id VARCHAR NOT NULL ,
session_id INT NOT NULL,
location VARCHAR,
user_agent TEXT

);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
user_id INT NOT NULL PRIMARY KEY,
first_name VARCHAR,
last_name VARCHAR, 
gender CHAR,
level VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
song_id VARCHAR NOT NULL PRIMARY KEY,
title VARCHAR,
artist_id VARCHAR NOT NULL ,
year INT,
durataion decimal
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
artist_id VARCHAR NOT NULL PRIMARY KEY,
name VARCHAR,
location VARCHAR, 
latitude DECIMAL,
longitude DECIMAL
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
start_time TIMESTAMP NOT NULL PRIMARY KEY,
hour INT,
day INT,
week INT,
month INT, 
year INT,
weekday INT

);
""")

# STAGING TABLES

staging_events_copy = (""" 
                       copy staging_events from {} iam_role{} json{};
                       """).format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
                          COPY staging_songs 
                          from {}
                          iam_role{}
                          json 'auto';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
insert into songplay (start_time, user_id, level,
song_id ,
artist_id ,
session_id ,
location ,
user_agent )

SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
se.userId as user_id,
se.level,
ss.song_id,
ss.artist_id,
se.sessionId  as session_id,
se.location,
se.userAgent    AS user_agent
FROM staging_events se
JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name
WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
insert into users (
                   user_id ,
                   first_name,
                   last_name , 
                   gender ,
                   level  )
            select userId as user_id,
                   firstName as first_name,
                   lastName as last_name,
                    gender,
                    level
            from staging_events
            WHERE page = 'NextSong' AND userId IS NOT NULL
""")

song_table_insert = ("""
insert into songs ( song_id,
                    title,
                    artist_id,
                    year ,
                    durataion 
                    )
        select song_id,
               title,
               artist_id,
               year,
               duration
        from staging_songs
        WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
insert into artists(artist_id , 
                    name,
                    location,
                    latitude,
                    longitude)
        select 
                 artist_id, 
                 artist_name as name,
                 artist_location as location, 
                 artist_latitude  as latitude, 
                 artist_longitude as longitude 
        from staging_songs
        where artist_id is not null 
""")

time_table_insert = ("""
insert into time (
                 start_time,
                 hour,
                 day,
                 week,
                 month,
                 year,
                 weekday)

    SELECT DISTINCT 
    TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
    EXTRACT(hour FROM (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')) AS hour,
    EXTRACT(day FROM (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')) AS day,
    EXTRACT(week FROM (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')) AS week,
    EXTRACT(month FROM (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')) AS month,
    EXTRACT(year FROM (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')) AS year,
    EXTRACT(dow FROM (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')) AS weekday
FROM 
    staging_events
WHERE 
    ts IS NOT NULL

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
