import psycopg2 
import psycopg2.extras
import psycopg2.errors
import csv
import pandas as pd
import numpy as np
import sys

sys.path.append('..')

import config 

print('Populating seller_addresses, it might take a couple of seconds...')


connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)

cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
dib_sellers_files = ['seller_addresses']
unique_ids = []
for seller in dib_sellers_files:
    with open (f"../data/{seller}.csv") as f:
        reader = csv.reader(f)
        next(reader) # Avoiding Headers
        for row in reader:
            # print(row)
            id_element = row[0]
            state = row[1]
            country = row[2]
            
            if id_element == '' or id_element == 'NULL':
                id_element = None
            if state == '' or state == 'NULL':
                state = None   
            if country == '' or state == 'NULL':
                country = None   

            # Creating check for unique ids
            if id_element in unique_ids:
                continue
            else:
                unique_ids.append(id_element)

            try:
                
                cursor.execute("""
                        INSERT INTO seller_addresses (id_seller_addresses, state, country)
                        VALUES (%s, %s, %s)
                    """,(id_element, state, country))
            except Exception as err:
                print ("Oops! An exception has occured:", err)
                print ("Exception TYPE:", type(err))
                
connection.commit()
print('seller_addresses was successfully popluated!')