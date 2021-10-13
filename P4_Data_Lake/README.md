# Data Lake Project

## Introduction
"A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.""

Using Amazon's AWS platform, I built an ETL pipeline to extract data from S3, process it using Spark, and load it back into S3 as a fact table and dimension tables so that the analytics team could more easily unearth insights regarding users, habits, and music popularity.


## Final tables created after loading and staging data

### Fact Table
* songplays - records in event data associated with song plays i.e. records with page NextSong
    * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
* users - users in the app
    * user_id, first_name, last_name, gender, level
* songs - songs in music database
    * song_id, title, artist_id, year, duration
* artists - artists in music database
    * artist_id, name, location, lattitude, longitude
* time - timestamps of records in songplays broken down into specific units
    * start_time, hour, day, week, month, year, weekday