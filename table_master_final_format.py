import psycopg2
from psycopg2 import sql
import os
import config 

print('Creating TABLE final_format')

# Adding table final_format to database for Data Warehousing considerations
connection = psycopg2.connect(host=config.DB_HOST, dbname=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)
with connection.cursor() as cursor:
    sql_file = open('master_query.sql','r')
    cursor.execute(sql_file.read())
connection.commit()
connection.close()

print('Success! TABLE final_format was successfully created')
