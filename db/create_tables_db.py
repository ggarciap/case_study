import psycopg2
import psycopg2.extras
from psycopg2 import sql
import sys

sys.path.append('..')
import config 

# Creating DB
# db_name  = config.DB_NAME
db_name = 'test_template'

con = psycopg2.connect(host=config.DB_HOST, user=config.DB_USER, password=config.DB_PASS)
con.autocommit = True

with con.cursor() as cur:
    cur.execute(sql.SQL('CREATE DATABASE {};').format(
        sql.Identifier(db_name)))
con.close()

# Adding tables 
connection = psycopg2.connect(host=config.DB_HOST, dbname=db_name, user=config.DB_USER, password=config.DB_PASS)
with connection.cursor() as cursor:
    sql_file = open('db_schema.sql','r')
    cursor.execute(sql_file.read())
connection.commit()
connection.close()