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
#from datetime import datetime
# start_time = datetime.now()

print('Populating salesforce_contact_records, it might take a couple of seconds...')


connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS,port=config.DB_PORT)
print(connection)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
dib_sellers_files = ['salesforce_contact_records']
for seller in dib_sellers_files:
    with open (f"./data/{seller}.csv") as f:
        reader = csv.reader(f)
        next(reader) # Avoiding Headers
        for row in reader:
            #print(row)
            id_element = row[0]
            seller_pk = row[1]
            contact_id = row[2]


            if id_element =='NULL' or id_element == '':
                id_element = None
            
            if seller_pk == 'NULL' or seller_pk == '':
                seller_pk = None

            if contact_id == 'NULL' or contact_id == '':
                contact_id = None  



            cursor.execute("""
                SELECT * FROM stdib_dibssellers WHERE seller_pk = %s 
            """,(seller_pk,))

            seller_pk_stdib_dibssellers = cursor.fetchone()


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
                        INSERT INTO salesforce_contact_records (id_salesforce_contact_records, seller_pk_salesforce_contact_records, contact_id)
                        VALUES (%s, %s, %s)
                    """,(id_element, seller_pk, contact_id))
            except Exception as err:
                print ("Oops! An exception has occured on salesforce_contact_records op:", err)
                print ("Exception TYPE:", type(err))
                break

connection.commit()
connection.close()

# EDIT: Duration: 0:00:30.389694
# end_time = datetime.now()
# print('Duration: {}'.format(end_time - start_time))
print('salesforce_contact_records was successfully popluated!\n')