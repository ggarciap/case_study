import psycopg2 
import psycopg2.extras
import psycopg2.errors
import csv
import pandas as pd
import numpy as np
import sys

sys.path.append('..')

import config 

print('Populating stdib_dibssellers, it might take a couple of seconds...')

connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)
print(connection)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
dib_sellers_files = ['stdib1_dibssellers', 'stdib2_dibssellers', 'stdib3_dibssellers']
for seller in dib_sellers_files:
    with open (f"../data/{seller}.csv") as f:
        reader = csv.reader(f)
        next(reader) # Avoiding Headers
        for row in reader:
            # print(row)
            id_element = row[0]
            seller_pk = row[1]
            seller_status = row[2]
            seller_status_code = row[3]
            seller_rating = row[4]
            seller_date_registered = row[5]
            seller_date_contract_start = row[6]
            seller_distinguished = row[7]

            if seller_status == 'NULL' or  seller_status == '':
                seller_status = None
            
            if seller_status_code == 'NULL' or seller_status_code =='':
                seller_status_code = None
            
            if seller_rating == 'NULL' or seller_rating =='':
                seller_rating = None
            
            if seller_date_registered == 'NULL' or seller_date_registered == '':
                seller_date_registered = None

            if seller_date_contract_start == 'NULL' or seller_date_contract_start == '':
                seller_date_contract_start = None

            if seller_distinguished == 'NULL' or seller_distinguished == '':
                seller_distinguished = None
                

            try:
                cursor.execute("""
                        INSERT INTO stdib_dibssellers (id, seller_pk, seller_status, seller_status_code, seller_rating,
                        seller_date_registered, seller_date_contract_start, seller_distinguished)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,(id_element, seller_pk, seller_status, seller_status_code, seller_rating,
                        seller_date_registered, seller_date_contract_start, seller_distinguished))
            except Exception as err:
                print ("Oops! An exception has occured:", err)
                print ("Exception TYPE:", type(err))
                
connection.commit()
connection.close()
print('stdib_dibssellers was successfully popluated!')