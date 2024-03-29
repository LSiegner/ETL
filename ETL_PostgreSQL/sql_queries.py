# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

songplay_table_create = ("""
                         CREATE TABLE IF NOT EXISTS songplay (
                        songplay_id SERIAL PRIMARY KEY, 
                        ts timestamp  NOT NULL, 
                        user_id int NOT NULL, 
                        level varchar NOT NULL, 
                        song_id varchar NOT NULL, 
                        artist_id varchar NOT NULL,
                        session_id int NOT NULL, 
                        location varchar NOT NULL,
                        user_agent varchar NOT NULL
                        );
                        """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS user_table (
                    user_id int PRIMARY KEY, 
                    first_name varchar NOT NULL,
                    last_name varchar NOT NULL,
                    gender varchar NOT NULL, 
                    level varchar NOT NULL
                    );
                    """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS song_table (
                    song_id varchar PRIMARY KEY, 
                    title varchar NOT NULL, 
                    artist_id varchar NOT NULL,
                    year int NOT NULL ,
                    duration int NOT NULL
                    );
                    """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist_table (
                        artist_id varchar PRIMARY KEY,
                        name varchar NOT NULL,
                       location varchar NOT NULL,
                       latitude float8 NOT NULL,
                       longitude float8 NOT NULL
                       );
                        """)


time_table_create = ("""CREATE TABLE IF NOT EXISTS time_table (
                    ts timestamp PRIMARY KEY,
                    hour int NOT NULL,
                    day int NOT NULL,
                    week int NOT NULL,
                    month int NOT NULL,
                    year int NOT NULL,
                    weekday varchar NOT NULL
                    );
                    """)

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplay (ts, user_id, level, song_id,
                         artist_id, session_id, location, user_agent)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s )
                        """)

user_table_insert = ("""INSERT INTO user_table (
                        user_id,
                        first_name,
                        last_name,
                        gender,
                        level)
                        VALUES(%s,%s,%s,%s,%s)
                        ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level
                        """)

song_table_insert = ("""INSERT INTO song_table (
                        song_id,
                        title,
                        artist_id,
                        year,
                        duration)
                        VALUES (%s, %s, %s, %s, %s) 
                        On CONFLICT(song_id) DO NOTHING
                        """)

artist_table_insert = ("""INSERT INTO artist_table (
                          artist_id,
                          name,
                          location,
                          latitude,
                          longitude)
                          VALUES (%s,%s, %s, %s, %s) 
                          ON CONFLICT(artist_id) DO UPDATE 
                          SET location = EXCLUDED.location,
                          latitude = EXCLUDED.latitude,
                          longitude = EXCLUDED.longitude
                          """)


time_table_insert = ("""INSERT INTO time_table (
                        ts,
                        hour,
                        day,
                        week,
                        month,
                        year,
                        weekday)
                        VALUES(%s, %s, %s, %s, %s, %s, %s)\
                        ON CONFLICT (ts) DO NOTHING
                        """)

# FIND SONGS

song_select = ("""SELECT song_id, artist_id, duration
                  FROM song_table
                  """)

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
