# case_study
## About
This repository contains the schema and necessary scripts for creating a postgres database with the csv files found in `./data`. Furthermore, for implementing this case study I decided to run the databse in `Docker` due to the advantages of fast installation and capability of allowing developers use different operating systems. 

After following all the instructions described below a file named `FINAL_FORMAT.csv` will be created on the root directory and the schema `db_schema.sql` will be present on the postgres container under the database name: `first_dibs`.  

## Installation and Scripts Execution
Make sure to have docker:
    
    pip install docker

Pull postgres image from docker hub:

    docker pull postgres

Create docker container `postgres_case_study`:

    docker run -d --name postgres_case_study -p 5432:5432 -e POSTGRES_PASSWORD=password postgres:latest

Execute this command to make the script runnable:
    
    chmod u+x job.sh

Once we have a container we can execute the shell script that will create and populate the database known as first_dibs:

    ./job.sh

Note: All of the scripts take a couple of seconds to run except `populate_seller_addresses_xref.py` which takes **approx. 4 min**.

To access the postgres docker instance:

    docker exec -it postgres_case_study psql -U postgres

## Data WareHousing 
For a data warehousing considerations  we could use services such as Redshift or Snowflake, in this way we can have a scalability, query speed and low data warehousing costs.

For this case study I decided to create a table (representing the final format desired) in the postgres database but in reality if we had the opportunity to use a DW we could take into our advantage the fact these solutions exist on top of serveral databases and we could perfom a query similar to `master_query.sql` in our DW solution which can then be used for generating business intelligence and performing data analytics with tools such as Tableau.
