# Sparkify Database


>Author: Rodrigo de Alvarenga Mattos


## Introduction

The goal of this project is to create a database solution optimized for queries and analysis of users' song play activities for the Sparkify service. 

This new design solved the difficulty of performing analytical tasks with information from the JSON log and metadata files.


## Project Dependencies

- [Python 3.10](https://www.python.org) 
- [Altair Viz 4.2.0](https://altair-viz.github.io) 
- [Ipython SQL 0.3.9](https://pypi.org/project/ipython-sql)
- [Matplotlib 3.5.2](https://matplotlib.org)
- [Pandas 1.4.2](https://pandas.pydata.org)
- [Pdoc3 0.8.1](https://pdoc3.github.io/pdoc)
- [Psycopg2 2.9.3](https://www.psycopg.org)
- [SqlAlchemy 1.4.37](https://www.sqlalchemy.org)
- [Streamlit 1.10.0](https://streamlit.io)
- [Wordcloud 1.8.1](https://github.com/amueller/word_cloud)


## Docker Image

Start the project container using the docker run command:

```console
docker run --name datadiver-postgres --hostname localhost -p 5432:5432 -p 8080:8080 -p 80:80 -d datadiverdev/postgres-jupyter
```

| Action               | Link                                           | Description                               |
|----------------------|------------------------------------------------|-------------------------------------------|
| **Open Jupyter Lab** | [http://localhost/lab](http://localhost/lab)   | Run python scripts and jupyter notebooks. |    
| **View Dashboard**   | [http://localhost:8080](http://localhost:8080) | Graphs and interactive analytic queries.  |


## JSON Data Files Schema


1. **Song Dataset** - The JSON files, located in the directory [data/song_data](./data/song_data), are a subset of the [Million Song Dataset](http://millionsongdataset.com) and each file contains the following data schema:
   
```javascript
{
    "num_songs": 1,
    "artist_id": "ARD7TVE1187B99BFB1",
    "artist_latitude": null,
    "artist_longitude": null,
    "artist_location": "California - LA",
    "artist_name": "Casual",
    "song_id": "SOMZWCG12A8C13C480",
    "title": "I Didn't Mean To",
    "duration": 218.93179,
    "year": 0
}
```


2. **Log Dataset** - The JSON files, located in the directory [data/log_data](./data/log_data), were generated by an [event simulator](https://github.com/Interana/eventsim) according to the songs data from the previous dataset, and each file contains the following data schema:
```javascript
{
    "artist":"The Mars Volta",
    "auth":"Logged In",
    "firstName":"Kaylee",
    "gender":"F",
    "itemInSession":5,
    "lastName":"Summers",
    "length":380.42077,
    "level":"free",
    "location":"Phoenix-Mesa-Scottsdale, AZ",
    "method":"PUT",
    "page":"NextSong",
    "registration":1540344794796.0,
    "sessionId":139,
    "song":"Eriatarka",
    "status":200,
    "ts":1541106673796,
    "userAgent":"\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/35.0.1916.153 Safari\/537.36\"",
    "userId":"8"
}
```


## Database Schema Design

The database is modeled as a star schema that consists of a fact table (songplays) referencing four dimension tables (artists, songs, time, users), and some custom types have also been defined.

All the SQL types and tables were defined in the [src/etl/sql_queries.py](./src/etl/sql_queries.py) file.

The figure below shows the database structure as an entity relationship diagram:

![Sparkify ER Diagram](./images/sparkify-schema.png)


## Creating the Database

The sample code below shows the main pipeline of the database creation process in the [src/etl/create_tables.py](./src/etl/create_tables.py) script:

```python
drop_functions(cur, conn)
drop_tables(cur, conn)
drop_types(cur, conn)
create_types(cur, conn)
create_tables(cur, conn)
create_functions(cur, conn)
```


*Note:* You should change the [src/etl/create_tables.py](./src/etl/create_tables.py) source code if you want to customize the database connection host, database name, username and password.

**Run the command** below to create the database schema:

```bash
# change directory to src 
cd src
# to create the schema in the local database
python -m etl.create_tables 
# to create the schema in the cloud database
python -m etl.create_tables --cloud
```


## ETL Pipeline Development

The ETL processes were developed in **two phases**. The first one implements the **project requirements** as specified. The second one is a full code refactoring emphasizing **performance improvement** and adding some extra transformations.

So we have a code file for **phase 1** - [src/etl/etl.py](./src/etl/etl.py) - and another for **phase 2** - [src/etl/etl2.py](./src/etl/etl2.py). Both codes are fully functional, but the second one is significantly faster and adds important transformations. The table below shows the sequence of the ETL pipeline processes:

| Process             | [src/etl/etl.py](./src/etl/etl.py) | [src/etl/etl2.py](./src/etl/etl2.py)       |
|---------------------|------------------------------------|--------------------------------------------|
| Process log files   | Loop through each file             | DataFrame concatenation of all files       |
| Process song files  | Loop through each file             | DataFrame concatenation of all files       |
| String Trim         | *none*                             | Full DataFrame transformation              |
| Timestamp type cast | Pandas datetime                    | Pandas datetime                            |
| User ID type cast   | Pandas Int64                       | Pandas Int64                               |
| User ID null values | *none*                             | Generated from user table data             |
| SQL table inserts   | One db call per record             | Batch copy_from an StringIO buffer **(1)** |
| SQL songs query     | One db call per record             | Batch select with stored procedure **(2)** |


**Note:** Take a look at notebooks [src/etl/etl.ipybn](./src/etl/etl.ipynb) and [src/etl/etl2.ipynb](./src/etl/etl2.ipynb) to see the step-by-step development of each ETL process described above.


1. **Batch ```copy_from``` an StringIO buffer:** The [psycopg2 copy_from](https://www.psycopg.org/docs/cursor.html) cursor method was selected based on the information presented in this [benchmark](https://naysan.ca/2020/05/09/pandas-to-postgresql-using-psycopg2-bulk-insert-performance-benchmark/). The figure below shows that this is the fastest operation of this library.

![psycopg2 copy_from benchmark](./images/benchmark.png)
Image by **Naysan Saran** via [Bulk Insert Performance Benchmark](https://naysan.ca/2020/05/09/pandas-to-postgresql-using-psycopg2-bulk-insert-performance-benchmark/)


2. **Batch Select with Stored Procedure:** According to the project requirements, the for loop with thousands of query selections, combined with a join operation, was the most critical point of performance bottlenecks. The average execution time was approximately 22 seconds on the local and the cloud database. The SQL code shown below reduced this time to less than 1 second in a single batch operation:

```sql
CREATE OR REPLACE FUNCTION song_artist_ids(list SONG_ARTIST[])
RETURNS TABLE(index INTEGER, song_id VARCHAR(18), artist_id VARCHAR(18)) AS
$func$
   SELECT $1[i].index, songs.song_id, songs.artist_id
   FROM   generate_subscripts($1, 1) i
   JOIN   songs ON songs.title = $1[i].title
                AND songs.duration = $1[i].duration
   JOIN   artists ON artists.name = $1[i].artist
$func$  LANGUAGE SQL STABLE;
```


**Run the command** below to execute the ETL pipeline:

```bash
# change directory to src 
cd src
# first version
python -m etl.etl 
# first version with cloud db
python -m etl.etl --cloud

# second version with improved performance
python -m etl.etl2
# second version with improved performance and cloud db
python -m etl.etl2 --cloud
```


| 3 Executions Mean (sec) | [src/etl/etl.py](./src/etl/etl.py) | [src/etl/etl2.py](./src/etl/etl2.py) | Improvement       |
|-------------------------|------------------------------------|--------------------------------------|-------------------|
| Local database          | 30.68                              | 1.64                                 | **18.7x** faster  |
| Cloud database          | 6832.46                            | 49.27                                | **133.3x** faster |


**Note:** These results can vary remarkably according to your local computer hardware and the cloud provider service.


## Auto-generate API Documentation

The [pdoc](https://pdoc3.github.io/pdoc/) documentation generator was used to output the [HTML docs](https://htmlpreview.github.io/?https://github.com/rodrigoalvamat/datadiver-pgsql/blob/main/docs/index.html) from the source code ```DOCSTRIGS```.

## Database Schema Validation and Sanity Tests

The [src/etl/test.ipynb](./src/etl/test.ipynb) notebook defines a set of test to validate the database table schema, data types, column constraints, primary keys and upsert conflict checks. You should run the **tests after** the execution of the [src/etl/create_tables.py](./src/etl/create_tables.py) and [src/etl/etl2.py](./src/etl/etl2.py) (or [src/etl/etl.py](./src/etl/etl.py)) scripts.


## Dashboard and Analytics

The [Streamlit](https://streamlit.io) application framework was used to build an interactive analytics dashboard. The following table describes the main application features and resources:

| Feature or Resource   | URL                                                                                          |
|-----------------------|----------------------------------------------------------------------------------------------|
| Dashboard Application | [http://localhost:8080](http://localhost:8080) |     
| Dashboard Source Code | [Source](./src/dashboard)                                                                    |
| SQL Analytics         | [queries.py](./src/dashboard/queries.py)                                                     |
| Cloud PostgresSQL     | [ElephantSQL](https://www.elephantsql.com/)                                                  |


### Dashboard Screenshot
![Sparkify Dashboard](./images/dashboard.png)


There were some limitations to developing a more interactive dashboard, with a greater variety of information, because of the missing data in the JSON files and some restrictions defined by the project requirements. However, several improvements are on the roadmap to overcome this issues:

### Next Release Roadmap
- Download a larger sample of the [Million Song Dataset](http://millionsongdataset.com) and use the [event simulator](https://github.com/Interana/eventsim) to improve the dataset quality.
- Use the [Google Maps API Python Client](https://github.com/googlemaps/google-maps-services-python) geocode method to get the latitude and longitude for the songplays' locations. 
- Create interactive graph and vizualization components for all dimensions (songs, artists, users, time) of the fact table (songplays).
- Docstring documentation of the dashboard source code.
