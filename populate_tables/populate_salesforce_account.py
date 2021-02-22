import psycopg2 
import psycopg2.extras
import psycopg2.errors
import csv
import pandas as pd
import numpy as np
import sys

sys.path.append('..')

import config 
from datetime import datetime
#start_time = datetime.now()

connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)
print(connection)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
dib_sellers_files = ['salesforce_account_records']
for seller in dib_sellers_files:
    with open (f"../data/{seller}.csv") as f:
        reader = csv.reader(f)
        next(reader) # Avoiding Headers
        for row in reader:
            #print(row)
            id_element = row[0]
            seller_pk = row[1]
            account_id = row[2]


            if id_element =='NULL' or id_element == '':
                id_element = None
            
            if seller_pk == 'NULL' or seller_pk == '':
                seller_pk = None

            if account_id == 'NULL' or account_id == '':
                account_id = None  



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
                        INSERT INTO salesforce_account_records (id_salesforce_account_records, seller_pk_salesforce_account_records, account_id)
                        VALUES (%s, %s, %s)
                    """,(id_element, seller_pk, account_id))
            except Exception as err:
                print ("Oops! An exception has occured on salesforce_account_records op:", err)
                print ("Exception TYPE:", type(err))
                break

connection.commit()
# EDIT: Duration: 0:00:30.623822
# end_time = datetime.now()
# print('Duration: {}'.format(end_time - start_time))