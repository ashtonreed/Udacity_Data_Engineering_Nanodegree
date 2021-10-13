# Data Modeling with Postgres

The purpose of this database is to help a new startup called Sparkify analyze their songplay and user data. The goal of this project is to help the new music streaming app better understand what their users are listening to in order to offer a great music experience. 

This database follows a star schema with the Songplay table being the fact table and the Users, Songs, Artists, and Time tables being the dimension tables.

The etl process was prototyped in a Jupyter Notebook then copied over to a file called "etl.py", allowing it to be easily run from the terminal in order to create the tables in this database. The file "sql_queries.py" was necessary first so that "create_tables.py" could be run successfully to drop any existing tables and create the desired tables to their specifications. The "etl.py" file was the final file to be created and run for this project. It processed data from both json song files and json log files in order to create 5 separate tables. As a result, it is now much easier for the analytics team at Sparkify to query user or song data in order to better understand their customers.