import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"


# CREATE TABLES
## staging table create
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession int,
    lastName varchar,
    length numeric,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration numeric,
    sessionId int,
    song varchar,
    status int,
    ts bigint,
    userAgent varchar,
    userId int
);
""")


staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs 
(
   song_id varchar(MAX),
   title varchar(MAX),
   duration numeric,
   year numeric,
   num_songs numeric,
   artist_id varchar(MAX),
   artist_name varchar(MAX),
   artist_latitude numeric,
   artist_longitude numeric,
   artist_location varchar(MAX)
);
""")


songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(
    songplay_id int IDENTITY(0,1) PRIMARY KEY, 
    start_time bigint NOT NULL, 
    user_id int NOT NULL, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id int, 
    location varchar, 
    user_agent varchar
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
(
    user_id int, 
    first_name varchar, 
    last_name varchar, 
    gender varchar, 
    level varchar
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(
    song_id varchar PRIMARY KEY, 
    title varchar NOT NULL, 
    artist_id varchar NOT NULL, 
    year int, 
    duration numeric
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
(
    artist_id varchar PRIMARY KEY, 
    name varchar NOT NULL, 
    location varchar, 
    latitude numeric, 
    longitude numeric
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
(
    start_time timestamp NOT NULL, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int
);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {} \
iam_role {} \
region 'us-west-2' FORMAT AS JSON {}; 
""").format(config.get("S3","LOG_DATA"),
            config.get("IAM_ROLE","ARN"),
            config.get("S3","LOG_JSONPATH"))


staging_songs_copy = ("""
copy staging_songs from {} \
iam_role {} \
region 'us-west-2' json 'auto'; 
""").format(config.get("S3","SONG_DATA"),
            config.get("IAM_ROLE","ARN"))


# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays 
(
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent
) 
SELECT staging_events.ts as start_time, 
       staging_events.userId as user_id, 
       staging_events.level as level, 
       staging_songs.song_id as song_id, 
       staging_songs.artist_id as artist_id, 
       staging_events.sessionId as session_id, 
       staging_songs.artist_location as location, 
       staging_events.userAgent as user_agent
FROM staging_events JOIN staging_songs ON 
   staging_events.song = staging_songs.title AND 
   staging_events.artist = staging_songs.artist_name
WHERE staging_events.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users 
(
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level
) 
SELECT userId as user_id, 
       firstName as first_name, 
       lastName as last_name, 
       gender, 
       level
FROM staging_events
""")

song_table_insert = ("""
INSERT INTO songs 
(
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
)
SELECT song_id, 
       title, 
       artist_id, 
       year, 
       duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists 
(
    artist_id, 
    name, 
    location, 
    latitude, 
    longitude
)
SELECT artist_id, 
       artist_name as name, 
       artist_location as location, 
       artist_latitude as latitude, 
       artist_longitude as longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time 
(
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday
) SELECT start_time, 
   EXTRACT(hr from start_time), 
   EXTRACT(d from start_time), 
   EXTRACT(w from start_time), 
   EXTRACT(mon from start_time), 
   EXTRACT(yr from start_time), 
   EXTRACT(weekday from start_time) 
FROM (SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time
FROM staging_events)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
