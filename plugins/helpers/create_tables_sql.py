class CreateTableQueries:
    create_staging_events = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
    artist        VARCHAR,
    auth          VARCHAR, 
    firstName     VARCHAR,
    gender        VARCHAR, 
    iteminSession INTEGER, 
    lastName      VARCHAR, 
    length        VARCHAR, 
    level         VARCHAR,
    location      VARCHAR,
    method        VARCHAR,
    page          VARCHAR,
    registration  VARCHAR, 
    sessionId     INTEGER    NOT NULL SORTKEY DISTKEY, 
    song          VARCHAR,
    status        INTEGER,
    ts            BIGINT     NOT NULL, 
    userAgent     VARCHAR, 
    userId        INTEGER);""")

    create_staging_songs=("""
    CREATE TABLE IF NOT EXISTS staging_songs ( 
    num_songs        INTEGER,
    artist_id        VARCHAR SORTKEY DISTKEY,
    artist_latitude  VARCHAR,
    artist_longitude VARCHAR, 
    artist_location  VARCHAR, 
    artist_name      VARCHAR, 
    song_id          VARCHAR, 
    title            VARCHAR,
    duration         FLOAT, 
    year             INTEGER);""")

    create_songplays=("""
    CREATE TABLE IF NOT EXISTS songplays (
    songplay_id VARCHAR PRIMARY KEY,
    start_time  TIMESTAMP  NOT NULL,
    users_id      INTEGER    NOT NULL,
    level        VARCHAR,  
    song_id      VARCHAR   NOT NULL,
    artist_id    VARCHAR   NOT NULL,
    session_id   INTEGER,
    location     VARCHAR,
    user_agent   VARCHAR);""")

    create_users_table =("""
    CREATE TABLE IF NOT EXISTS users_table (
    users_id    INTEGER PRIMARY KEY SORTKEY,
    first_name VARCHAR,
    last_name  VARCHAR,
    gender     VARCHAR,
    level      VARCHAR) diststyle all; """)

    create_song_table = ("""
    CREATE TABLE IF NOT EXISTS song_table(
    song_id    VARCHAR  PRIMARY KEY SORTKEY,
    title      VARCHAR,
    artist_id  VARCHAR,
    year       INTEGER,
    duration   FLOAT) diststyle all;""")

    create_artist_table = ("""
    CREATE TABLE IF NOT EXISTS artist_table(
    artist_id  VARCHAR PRIMARY KEY SORTKEY,
    name       VARCHAR,
    location   VARCHAR,
    latitude   VARCHAR,
    longitude  VARCHAR) diststyle all;""")

    create_time_table = ("""
    CREATE TABLE IF NOT EXISTS time_table(
    start_time TIMESTAMP PRIMARY KEY SORTKEY,
    hour       INTEGER,
    day        INTEGER,
    week       INTEGER,
    month      INTEGER,
    year       INTEGER,
    weekday    VARCHAR)diststyle all;""")

