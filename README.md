# Udacity Data Engineering Project 04

## Overview
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

We need to build an ETL pipeline that:

1. **extracts their data from S3**

2. **processes them using Spark**

3. **loads the data back into S3 as a set of dimensional tables**.

   

   This will allow them to continue finding insights in what are most played songs by there users.



## Schema

This project implements a star schema. `songplays` is the fact table in the data model, while `users`, `songs`, `artists`, and `time` are all dimensional tables.

### Fact Table
* **`songplays`** - records in event data associated with song plays (records with page = NextSong)
  * `start_time`, `userId`, `level`, `sessionId`, `location`, `userAgent`, `song_id`, `artist_id`, `songplay_id`

### Dimensional Tables
* **users** - users of the Sparkify app.
  * `firstName`, `lastName`, `gender`, `level`, `userId`
* **songs** - collection of songs.
  * `song_id`, `title`, `artist_id`, `year`, `duration` 
* **artists** - information about artists.
  * `artist_id`, `artist_name`, `artist_location`, `artist_lattitude`, `artist_longitude`
* **time** - timestamps of records in songplays, deconstructed into various date-time parts.
  * `start_time`, `hour`, `day`, `week`, `month`, `year`, `weekday`


## How to ?
1. Add appropriate AWS Credentials and S3 input and output paths in `environment.cfg`
3. Run `etl.py`