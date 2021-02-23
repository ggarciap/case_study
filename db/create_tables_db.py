import psycopg2
import psycopg2.extras
from psycopg2 import sql
import sys
import os
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
import config 

# Creating DB
db_name  = config.DB_NAME
#db_name = 'test_template'

con = psycopg2.connect(host=config.DB_HOST, user=config.DB_USER, password=config.DB_PASS,port=config.DB_PORT)
con.autocommit = True

with con.cursor() as cur:
    cur.execute(sql.SQL('CREATE DATABASE {};').format(
        sql.Identifier(db_name)))
con.close()

# Adding tables 
connection = psycopg2.connect(host=config.DB_HOST, dbname=db_name, user=config.DB_USER, password=config.DB_PASS,port=config.DB_PORT)
with connection.cursor() as cursor:
    sql_file = open('db/db_schema.sql','r')
    cursor.execute(sql_file.read())
connection.commit()
connection.close()