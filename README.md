# Data Lakes using Spark


## Introduction


The music streaming startup, Sparkify, has grown its user base and song database. As a result, it has collected a copious amount of user activity logs and song data. The data currently resides in S3 buckets in the form of JSON files. The analytics team at Sparkify wants to use this data to deliver insights that can drive business decisions and improve user experience. To achieve this the data needs to be migrated into a data lake hosted on AWS S3. The aim of this project is to build an end to end big data ecosystem that supports this data lake.

The following are the sub-tasks of this project:
- Construct an **ETL** pipeline that:
	- **E**xtracts the raw data (around 400K records) from the input S3 buckets.
	- **T**ransforms the data into analytics tables using Spark.
	-  **L**oad the processed data onto the output S3 bucket to build the data lake.


## Database Schema

The analytics team at Sparkify is interested in understanding what songs the users listen to frequently. Given this use case, a star schema implementation that is attuned to this requirement is implemented.

A relational database model is ideal for this use case as:

- The data is well structured and can be represented in a tabular format.
- The insights required by the analytics team are simple and can be achieved by using SQL queries.
- The ability to perform JOINs is relevant to this scenario.


### Fact Table

A single fact table called **`songplays`** is implemented whose records hold log data associated with song plays. It has the following attributes:

| Attribute   | Data Type | Notes                              |
|-------------|-----------|------------------------------------|
| songplay_id | bigint    | auto-incremented (*Primary Key* )  |
| start_time  | timestamp | timestamp of App usage by user     |
| user_id     | int       | unique integer ID of user          |
| level       | varchar   | user subscription (paid/free)      |
| song_id     | varchar   | song ID of the requested song      |
| artist_id   | varchar   | artist ID of the requested song.   |
| session_id  | int       | browser session ID.                |
| location    | varchar   | user location                      |
| user_agent  | varchar   | browser and OS details of the user |


### Dimension Tables

Four dimension tables are created to capture the attributes corresponding to the different data elements. Below is a brief description of the same:

#### 1. **`users`**

This table captures user information like age, gender, name, etc. It has the following attributes:

| Attribute  | Data Type | Notes        |
|------------|-----------|--------------|
| user_id    | int       | *Primary Key * |
| first_name | varchar   | first name of user   |
| last_name  | varchar   | last name of user  |
| gender     | varchar   |  M/F   |
| level      | varchar   | paid/free |

#### 2. **`songs`**

The songs table holds all the information about songs available in the music streaming app like artist name, duration, year of release, etc. It has the following attributes:

| Attribute | Data Type | Notes                |
|-----------|-----------|----------------------|
| song_id   | varchar   |  unique alpha-numeric ID  of the song (*Primary Key*) |
| title     | varchar   |  name of the song   |
| artist_id | varchar   |  |
| year      | int       | release year         |
| duration  | decimal   | length of the song in ms |

#### 3. **`artists`**

Just like the songs table, the artists table holds all the information about artists like name, location, etc. This table has the following attributes:

| Attribute | Data Type | Notes                                              |
|-----------|-----------|----------------------------------------------------|
| artist_id | varchar   | unique alpha-numeric ID  of the artist (*Primary Key*) |
| name      | varchar   | name of artist                                     |
| location  | varchar   | Location of artist                                 |
| latitude  | decimal   | latitude coordinate of the location                |
| longitude | decimal   | longitude coordinate of the location               |

#### 4. **`time`**

The timestamps of records in the songplays table are broken down into specific units and are contained in the time table. The following are the attributes of this table:

| Attribute  | Data Type | Notes                                  |
|------------|-----------|----------------------------------------|
| start_time | time      | time stamp of App usage (*Primary Key*)|
| hour       | int       | Hour of the day (00 - 23)              |
| day        | int       | Day of the week (1-7)                  |
| week       | int       | Week number (1-52)                     |
| month      | int       | Month number (1-12)                    |
| year       | int       | Year of activity                       |


## File Structure  and usage

Along with the logs and song data contained in the S3 bucket, the following files are used in this project:

| File name                  | Description                                                                      |
|----------------------------|----------------------------------------------------------------------------------|
| etl.py                     | Extracts the raw data, transforms it and loads it into the data lake             |
| dl.cfg                     | Contains configurations pertaining to the data lake,  and IAM roles.             |
| README.md                  | Contains the project description.                                                |


To use the project the following steps must be performed:


- Run the **etl.py** file by using the command: `python3 etl.py`

**Good Practice:** 

- Never share your AWS Access Key ID and the Secret access key with unauthorized personnel or on public platforms.
