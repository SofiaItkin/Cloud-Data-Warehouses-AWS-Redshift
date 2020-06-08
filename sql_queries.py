import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = " drop table if exists staging_events "
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplay"
user_table_drop = " drop table if exists users"
song_table_drop = " drop table if exists song"
artist_table_drop = " drop table if exists artist"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("""Create Table staging_events (artist varchar,
auth varchar,
first_name varchar,
gender varchar,
iteminsession varchar,
lastname varchar,
length varchar,
level varchar,
location varchar,
method varchar,
page varchar,
registration float,
sessionid varchar,
song varchar,
status varchar,
ts varchar,
useragent varchar,
userid varchar
);
""")

staging_songs_table_create = (""" Create Table staging_songs (
artist_id varchar,
artist_latitude varchar,
artist_location varchar,
artist_longitude varchar,
artist_name varchar,
duration varchar,
num_songs int,
song_id varchar,
title varchar,
year int 
);
""")

songplay_table_create = ("""Create Table songplay (songplay_id INT IDENTITY(1, 1) PRIMARY KEY, 
                                                    start_time TIMESTAMP NOT NULL, 
                                                    user_id  varchar NOT NULL, 
                                                    level varchar NOT NULL, 
                                                    song_id varchar, 
                                                    artist_id varchar,
                                                    session_id varchar NOT NULL,
                                                    location varchar, 
                                                    user_agent varchar);
""")

user_table_create = ("""Create Table users (user_id varchar PRIMARY KEY, 
                                            first_name varchar , 
                                            last_name varchar, 
                                            gender varchar, 
                                            level varchar NOT NULL);
""")

song_table_create = ("""Create Table song (song_id varchar PRIMARY KEY,
                                            title varchar NOT NULL, 
                                            artist_id varchar NOT NULL, 
                                            year  varchar NOT NULL, 
                                            duration varchar NOT NULL);
""")

artist_table_create = ("""Create Table artist (artist_id varchar PRIMARY KEY, 
                                                artist_name varchar NOT NULL, 
                                                artist_location varchar, 
                                                artist_latitude varchar, 
                                                artist_longitude varchar);
""")


time_table_create = ("""Create Table time (start_time TIMESTAMP WITHOUT TIME ZONE  PRIMARY KEY, 
                                           hour int NOT NULL, 
                                           day int NOT NULL, 
                                           week int NOT NULL, 
                                           month int NOT NULL, 
                                           year varchar NOT NULL, 
                                           weekday int NOT NULL);
""")

# STAGING TABLES


staging_events_copy = ("""
    copy staging_events from {}
    iam_role '{}'
    format as json {}
    """).format(
    config.get("S3", "LOG_DATA"),
    config.get("IAM_ROLE", "ARN"),
    config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ( """copy staging_songs (artist_id,
artist_latitude,
artist_location,
artist_longitude,
artist_name,
duration,
num_songs,
song_id,
title,
year 
) from {}
    iam_role '{}' 
    format as json 'auto'
    """).format(
    config.get("S3", "SONG_DATA"),
    config.get("IAM_ROLE", "ARN")
     )

# FINAL TABLES

songplay_table_insert = ("""


insert into songplay (start_time, 
                      user_id, 
                      level, 
                      song_id, 
                      artist_id,
                      session_id,
                      location, 
                      user_agent)
select Distinct TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time,userid,level,song,artist,sessionid,location,useragent
from staging_events
where page = 'nextsong'


""")

user_table_insert = ("""
insert into users (user_id, 
first_name, 
last_name, 
gender, 
level)
select Distinct
userid,
first_name, 
lastname, 
gender, 
level
from staging_events
where page = 'nextsong'

""")

song_table_insert = ("""
insert into song (
song_id,
title, 
artist_id, 
year, 
duration
)
select distinct song_id,title,artist_id,year,duration
from staging_songs
""")

artist_table_insert = ("""
insert into artist(
artist_id, 
artist_name, 
artist_location, 
artist_latitude, 
artist_longitude
)
select distinct artist_id, 
artist_name, 
artist_location, 
artist_latitude, 
artist_longitude
from staging_songs

""")

time_table_insert = ("""
insert into time(
start_time, 
hour,
day, 
week, 
month, 
year, 
weekday
)
select distinct
TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time, 
date_part(h,(TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) as hour,
date_part(d,(TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) as day, 
date_part(w,(TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) as week, 
date_part(m,(TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) as month, 
date_part(y,(TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) as year, 
date_part(dw,(TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) as weekday
from staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert,
                        artist_table_insert, 
                        time_table_insert
                       ]
