### Data Modeling with Postgresql
---
This project uses the Million Song Dataset to create a database for a fictional company sparkify. For this project a star schema database was created using an ETL pipeline, which was designed for OLAP purposes, e.g. run CRUD queries using Postgresql. So that the fictional company sparkify is able to answer basic questions, such as when are the most logins happening, what are the most popular songs, what is the average listening time to a song etc. 
However this database only covers the actions of users, when listening to songs and not transactions or payments or any other monetary related subject. Therefore it is really just designed to answer questions regarding customer behaviour inside the app.
### Running the program
---
To successfully run the pipeline, the following steps must be done in order:
- first load the data from the Million Song Dataset in .json format into the folder data. Be aware that the Million Song Dataset delivers two types of file to be processed. First one is the song data, which must be saved in the data/song and the log data which must be stored in the data/log folder
- run create_tables.py . This creates the database on you local machine (be aware you have to set the respective database connection in create_tables() yourself). If you have run the entire program already be aware that this file, will drop all already existing tables and will recreate them.
- run etl.py this is the data pipeline, which manages data wrangling as well as the insert statements to your locally created database
This is it! Now you have created your own local database to run your queries on.

### Database Design
---
As stated above the database uses a star schema. This means the "songplay" table is the associated fact table, whereas "user_table", "song_table", "artist_table" and "time_table" , hold all the dimensional values. 

### Files
---
In this project you will find the following files/ folders, here is a quick explanation of them:
- data folder, here all your data from the Million Song Dataset will be saved to
- create_tables.py creates the database and the respective tables, all previously created tables and the database are always dropped, when running this file
- etl.py is the data pipeline for ETL process. It wrangles the data provided in .json format from the data folder
- etl.ipynb is a Jupyter Notebook used to create all the ETL processes. This notebook holds no value now, after the etl.py was completed, however it was used to implement the right SQL queries as well as to wrangle the data in a much simpler form
- test is a Jupyter Notebook to test all the tables, if they have been created correctly and with the appropiate INSERT statements
