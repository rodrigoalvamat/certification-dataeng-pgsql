# DROP TYPES

gender_type_drop = "DROP TYPE IF EXISTS GENDER"
level_type_drop = "DROP TYPE IF EXISTS LEVEL"

# CREATE TYPES

gender_type_create = "CREATE TYPE GENDER AS ENUM ('F', 'M');"
level_type_create = "CREATE TYPE LEVEL AS ENUM ('free', 'paid');"

# DROP TABLE IF EXISTSS

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id                 SERIAL PRIMARY KEY,
    level                       LEVEL NOT NULL,
    location                    VARCHAR(100) NOT NULL,
    session_id                  SMALLINT NOT NULL,
    user_agent                  TEXT NOT NULL,
    artist_id                   VARCHAR(18),
    song_id                     VARCHAR(18),
    start_time                  TIMESTAMP NOT NULL,
    user_id                     INTEGER NOT NULL,
    CONSTRAINT fk_artist_id     FOREIGN KEY (artist_id) REFERENCES artists (artist_id),
    CONSTRAINT fk_song_id       FOREIGN KEY (song_id) REFERENCES songs (song_id),
    CONSTRAINT fk_start_time    FOREIGN KEY (start_time) REFERENCES time (start_time),
    CONSTRAINT fk_user_id       FOREIGN KEY (user_id) REFERENCES users (user_id)
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

song_table_create = ("""
CREATE TABLE songs (
    song_id                     VARCHAR(18) PRIMARY KEY,
    title                       VARCHAR(100) NOT NULL,
    year                        INTEGER NOT NULL,
    duration                    REAL NOT NULL,
    artist_id                   VARCHAR(18) NOT NULL,
    CONSTRAINT fk_artist_id     FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
);
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id                   VARCHAR(18) PRIMARY KEY,
    name                        VARCHAR(100) NOT NULL,
    location                    VARCHAR(100) NOT NULL,
    latitude                    DOUBLE PRECISION,
    longitude                   DOUBLE PRECISION
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

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (level, location, user_agent, artist_id, session_id, song_id, start_time, user_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (songplay_id) DO UPDATE SET
    level = EXCLUDED.level,
    location = EXCLUDED.location,
    user_agent = EXCLUDED.user_agent,
    artist_id = EXCLUDED.artist_id,
    session_id = EXCLUDED.session_id,
    song_id = EXCLUDED.song_id,
    start_time = EXCLUDED.start_time,
    user_id = EXCLUDED.user_id;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    gender = EXCLUDED.gender,
    level = EXCLUDED.level;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, year, duration, artist_id)
    VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO UPDATE SET
    title = EXCLUDED.title,
    year = EXCLUDED.year,
    duration = EXCLUDED.duration,
    artist_id = EXCLUDED.artist_id;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO UPDATE SET
    name = EXCLUDED.name,
    location = EXCLUDED.location,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) DO UPDATE SET
    hour = EXCLUDED.hour,
    day = EXCLUDED.day,
    week = EXCLUDED.week,
    month = EXCLUDED.month,
    year = EXCLUDED.year,
    weekday = EXCLUDED.weekday;
""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, a.artist_id
FROM songs s
JOIN artists a
ON s.artist_id = a.artist_id
WHERE s.title = %s AND a.name = %s AND s.duration = %s;
""")

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, time_table_create, song_table_create, songplay_table_create]
create_type_queries = [gender_type_create, level_type_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
drop_type_queries = [gender_type_drop, level_type_drop]
