songplays_by_user = """
SELECT COUNT(*) AS plays, CONCAT(u.first_name, ' ', u.last_name) AS user, u.gender, u.level
FROM songplays AS s
JOIN users AS u
ON s.user_id = u.user_id
GROUP BY s.user_id, u.first_name, u.last_name, u.gender, u.level
ORDER BY plays DESC;
"""

songplays_by_time = """
SELECT s.location, s.start_time AS timestamp, t.hour, t.day, t.month, t.weekday 
FROM songplays AS s
JOIN time AS t
ON s.start_time = t.start_time
ORDER BY s.start_time DESC;
"""

songs_by_artist = """
SELECT * FROM songs AS s
JOIN artists AS a
ON  s.artist_id = a.artist_id
"""

artists_by_location = """
SELECT location, latitude, longitude
FROM artists
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
"""
