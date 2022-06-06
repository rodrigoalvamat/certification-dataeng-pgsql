# DROP TYPES

gender_type_drop = "DROP TYPE IF EXISTS GENDER"
level_type_drop = "DROP TYPE IF EXISTS LEVEL"
song_artist_type_drop = "DROP TYPE IF EXISTS SONG_ARTIST"

# CREATE TYPES

gender_type_create = "CREATE TYPE GENDER AS ENUM ('F', 'M');"
level_type_create = "CREATE TYPE LEVEL AS ENUM ('free', 'paid');"
song_artist_type_create = """
CREATE TYPE SONG_ARTIST AS (
    index       INTEGER,
    title       VARCHAR(100),
    duration    NUMERIC,
    artist      VARCHAR(100)
);
"""

# DROP TABLE IF EXISTS

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# DROP FUNCTION IF EXISTS

song_artist_ids_drop = "DROP FUNCTION IF EXISTS song_artist_ids"

# CREATE TABLES

artist_table_create = ("""
CREATE TABLE artists (
    artist_id                   VARCHAR(18) PRIMARY KEY,
    name                        VARCHAR(100) NOT NULL,
    location                    VARCHAR(100) NOT NULL,
    latitude                    DOUBLE PRECISION,
    longitude                   DOUBLE PRECISION
);
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id                     VARCHAR(18) PRIMARY KEY,
    title                       VARCHAR(100) NOT NULL,
    year                        INTEGER NOT NULL,
    duration                    NUMERIC NOT NULL,
    artist_id                   VARCHAR(18) NOT NULL,
    CONSTRAINT fk_artist_id     FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
);
""")

time_table_create = ("""
CREATE TABLE time (
    start_time                  TIMESTAMP PRIMARY KEY,
    hour                        NUMERIC(2) NOT NULL,
    day                         NUMERIC(2) NOT NULL,
    week                        NUMERIC(2) NOT NULL,
    month                       NUMERIC(2) NOT NULL,
    year                        INTEGER NOT NULL,
    weekday                     VARCHAR(50) NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE users (
    user_id                     INTEGER PRIMARY KEY,
    first_name                  VARCHAR(50) NOT NULL,
    last_name                   VARCHAR(100) NOT NULL,
    gender                      GENDER NOT NULL,
    level                       LEVEL NOT NULL
);
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id                 SERIAL PRIMARY KEY,
    level                       LEVEL NOT NULL,
    location                    VARCHAR(100) NOT NULL,
    session_id                  SMALLINT NOT NULL,
    user_agent                  TEXT NOT NULL,
    start_time                  TIMESTAMP NOT NULL,
    user_id                     INTEGER NOT NULL,
    artist_id                   VARCHAR(18),
    song_id                     VARCHAR(18),
    CONSTRAINT fk_start_time    FOREIGN KEY (start_time) REFERENCES time (start_time),
    CONSTRAINT fk_user_id       FOREIGN KEY (user_id) REFERENCES users (user_id),
    CONSTRAINT fk_artist_id     FOREIGN KEY (artist_id) REFERENCES artists (artist_id),
    CONSTRAINT fk_song_id       FOREIGN KEY (song_id) REFERENCES songs (song_id)
);
""")

# INSERT RECORDS

artist_table_insert = ("""
INSERT INTO artists
    (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO UPDATE SET
    name = EXCLUDED.name,
    location = EXCLUDED.location,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude;
""")

song_table_insert = ("""
INSERT INTO songs
    (song_id, title, year, duration, artist_id)
    VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO UPDATE SET
    title = EXCLUDED.title,
    year = EXCLUDED.year,
    duration = EXCLUDED.duration,
    artist_id = EXCLUDED.artist_id;
""")

time_table_insert = ("""
INSERT INTO time
    (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) DO UPDATE SET
    hour = EXCLUDED.hour,
    day = EXCLUDED.day,
    week = EXCLUDED.week,
    month = EXCLUDED.month,
    year = EXCLUDED.year,
    weekday = EXCLUDED.weekday;
""")

user_table_insert = ("""
INSERT INTO users
    (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    gender = EXCLUDED.gender,
    level = EXCLUDED.level;
""")

songplay_table_insert = ("""
INSERT INTO songplays
    (level, location, session_id, user_agent, start_time, user_id, artist_id, song_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (songplay_id) DO UPDATE SET
    level = EXCLUDED.level,
    location = EXCLUDED.location,
    session_id = EXCLUDED.session_id,
    user_agent = EXCLUDED.user_agent,
    start_time = EXCLUDED.start_time,
    user_id = EXCLUDED.user_id,
    artist_id = EXCLUDED.artist_id,
    song_id = EXCLUDED.song_id;
""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, a.artist_id
FROM songs s
JOIN artists a
ON s.artist_id = a.artist_id
WHERE s.title = %s AND s.duration = %s AND a.name = %s;
""")

# FIND SONGS FUNCTION

song_artist_ids = """
CREATE OR REPLACE FUNCTION song_artist_ids(list SONG_ARTIST[])
RETURNS TABLE(index INTEGER, song_id VARCHAR(18), artist_id VARCHAR(18)) AS
$func$
   SELECT $1[i].index, songs.song_id, songs.artist_id
   FROM   generate_subscripts($1, 1) i
   JOIN   songs ON songs.title = $1[i].title
                AND songs.duration = $1[i].duration
   JOIN   artists ON artists.name = $1[i].artist
$func$  LANGUAGE SQL STABLE;
"""

# QUERY LISTS

create_function_queries = [song_artist_ids]

create_table_queries = [user_table_create, artist_table_create,
                        time_table_create, song_table_create, songplay_table_create]
                        
create_type_queries = [gender_type_create,
                       level_type_create, song_artist_type_create]

drop_table_queries = [songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]

drop_function_queries = [song_artist_ids_drop]

drop_type_queries = [gender_type_drop, level_type_drop, song_artist_type_drop]
