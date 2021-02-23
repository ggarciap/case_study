import psycopg2 
import psycopg2.extras
import psycopg2.errors
import csv
import pandas as pd
import numpy as np
import sys

import os 
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

import config 
from datetime import datetime
#start_time = datetime.now()

print('Populating seller_addresses_xref, it might take a couple of minutes(approx. 4)...')


connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)
print(connection)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
dib_sellers_files = ['seller_address_xref']
for seller in dib_sellers_files:
    with open (f"./data/{seller}.csv") as f:
        reader = csv.reader(f)
        next(reader) # Avoiding Headers
        for row in reader:
            #print(row)
            id_element = row[0]
            seller_pk = row[1]
            address_id = row[2]
            address_status = row[3]
            address_type = row[4]

            if id_element =='NULL' or id_element == '':
                id_element = None
            
            if seller_pk == 'NULL' or seller_pk == '':
                seller_pk = None

            if address_id == 'NULL' or address_id == '':
                address_id = None  
            
            if address_status == 'NULL' or address_status == '':
                address_status = None

            if address_type == 'NULL' or address_type == '':
                address_type = None

            cursor.execute("""
                SELECT * FROM seller_addresses WHERE id_seller_addresses = %s 
            """,(id_element,))

            id_reference_table = cursor.fetchone()

            cursor.execute("""
                SELECT * FROM stdib_dibssellers WHERE seller_pk = %s 
            """,(seller_pk,))

            seller_pk_stdib_dibssellers = cursor.fetchone()

            if id_reference_table is None:
                try:
                    cursor.execute("""
                            INSERT INTO seller_addresses (id_seller_addresses, state, country)
                            VALUES (%s, %s, %s)
                        """,(id_element, None, None))
                except Exception as err:
                    print ("Oops! An exception has occured on seller_addresses op:", err)
                    print ("Exception TYPE:", type(err))
                    break

            if seller_pk_stdib_dibssellers is None:
                try:
                    cursor.execute("""
                            INSERT INTO stdib_dibssellers (id, seller_pk, seller_status,
                            seller_status_code, seller_rating, seller_date_registered,
                            seller_date_contract_start, seller_distinguished)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,(None, seller_pk, None, None, None, None, None, None))
                except Exception as err:
                    print ("Oops! An exception has occured on stdib_dibssellers op:", err)
                    print ("Exception TYPE:", type(err))
                    break
            
            # After making sure to have values present 
            try:
                cursor.execute("""
                        INSERT INTO seller_addresses_xref (id_seller_addresses_xref, seller_pk_seller_addresses_xref, address_id, address_status,address_type)
                        VALUES (%s, %s, %s, %s, %s)
                    """,(id_element, seller_pk, address_id, address_status, address_type))
            except Exception as err:
                print ("Oops! An exception has occured on seller_addresses_xref:", err)
                print ("Exception TYPE:", type(err))
                break

connection.commit()
connection.close()
# Duration: 0:04:08.431591
# end_time = datetime.now()
# print('Duration: {}'.format(end_time - start_time))
print('seller_addresses_xref was successfully popluated!\n')