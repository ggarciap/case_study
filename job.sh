#!/bin/sh
python3 db/create_tables_db.py
python3 populate_tables/populate_dibssellers.py
python3 populate_tables/populate_seller_addresses.py
python3 populate_tables/populate_seller_addresses_xref.py
python3 populate_tables/populate_salesforce_account.py
python3 populate_tables/populate_salesforce_contact.py
python3 master_dibsellers.py
python3 table_master_final_format.py