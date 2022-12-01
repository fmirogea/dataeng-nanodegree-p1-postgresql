# Project: Data Modeling with Postgres

This is Project 1: Data Modeling with PostgreSQL, part of Udacity's Nanodegree in Data Engineering.

## Project Description

The project is in the context of an startup, Sparkify, and its goal is to improve their analytical capabilities. This is achieved by using the song and log datasets, creating a data model (based on a star schema) optimized for queries on song play analysis.

## Running the scripts

How to run the Python scripts:

First, run `python create_tables.py` to create the database and its tables.

Run `python etl.py` to run the ETL pipeline i.e. read and process the files from `song_data` and `log_data` and loads them into the final tables.

## File descriptions

Different files and folders can be found in the repository:

- The `Data` folder contains the song and log datasets.
- The `media` folder contains diagramms used in this document to represent the data model and the etl process.
- The `create_tables.py` script contains the code necessary for creating the database and its tables.
- The `sql_queries.py` contains the different SQL queries used by the other scripts.
- The `etl.py` script contains the code necessary for running the pipeline.
- The `etl.ipynb` Notebook was used for coding in an interactive way the `etl.py` script.
- The `test.ipynb` Notebook includes some SQL queries for debugging i.e. testing the content of the tables at any time.


## Database schema and ETL pipeline design

The data has been modeled using a star schema, which consists of one or more fact tables referencing any number of dimension tables.

- The fact table contains record measurements or metrics for a specific event. In our case it's the log of which songs have been played.
- Dimension tables define characteristics of those events. Dimension tables usually contain a much smaller number of records than the fact table.  

A characteristic of using fact and dimensional tables is that a unique primary key for each dimension table is included in the fact table. 

The following image shows the relationship between the fact and dimension tables:
![data model](/media/Project1_DataModellingPostgreSQL-Data-Model.drawio.png)

Star Schemas are a form of denormalized data modelling and have a number of benefits:

- Simpler queries: join-logic is much simpler.
- Query performance gains
- Fast aggregations

The main dissadvantages of using star schemas are:

- Lack of flexibility: Since star schemas are built for a purpose, it is not as flexible as normalized data models.
- Lack of data-integrity: Data is redundant due to its denormalized nature. Inserts and updates can result in data anomalities.


Picture of the ETL pipeline:
![ETL Pipeline](/media/Project1_DataModellingPostgreSQL-ETL.drawio.png)

## Example queries and results

Provide example queries and results for song play analysis.

Number of times the songs by an artist have been played during 13th of November 2018:

`SELECT a.name, count(*)
FROM songplays F
INNER JOIN time T ON (F.start_time = T.start_time) 
INNER JOIN artists A ON (F.artist_id = A.artist_id)
WHERE A.name = 'Casual' AND T.year = 2018 AND T.month = 11 AND T.day = 13

`