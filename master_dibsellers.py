from sqlalchemy import create_engine
import pandas as pd
import numpy
import config
import os 
import csv
engine = create_engine('postgresql+psycopg2://'+ config.DB_USER + ':' + config.DB_PASS +'@'+ config.DB_HOST + '/first_dibs')
master_q = """
    WITH CT1 AS (
        SELECT  seller_pk ,
            seller_status ,
            seller_status_code,
            seller_rating,
            TIMEZONE('America/New_York',TO_TIMESTAMP(seller_date_registered))
            AS seller_registered,
            CASE WHEN seller_date_contract_start = 0 THEN NULL
            ELSE TIMEZONE('America/New_York',TO_TIMESTAMP(seller_date_contract_start)) 
            END
            AS "seller_agreement_start_date",
            CAST((
                CASE 
                    WHEN seller_distinguished IS NULL THEN FALSE
                    ELSE TRUE 
                END 
            ) AS INTEGER) AS "seller_destinguished"
        FROM stdib_dibssellers
            
    ),
    SELLER_DEF_T AS (
        SELECT country, state, seller_pk_seller_addresses_xref
        FROM seller_addresses LEFT JOIN seller_addresses_xref ON id_seller_addresses = address_id
        WHERE address_type LIKE 'SELLER_DEFAULT'
    ),
    SELLER_SHIP_T AS(
        SELECT country, state, seller_pk_seller_addresses_xref
        FROM seller_addresses LEFT JOIN seller_addresses_xref ON id_seller_addresses = address_id
        WHERE address_type LIKE 'SELLER_SHIPPING'
    ),
    CT2 AS(
       SELECT  CT1.seller_pk,
            CT1.seller_status,
            CT1.seller_status_code,
            CT1.seller_rating,
            CT1.seller_registered,
            CT1.seller_agreement_start_date,
            CT1.seller_destinguished,
            SELLER_DEF_T.state as seller_default_state,
            SELLER_DEF_T.country as seller_default_country,
            SELLER_SHIP_T.state as seller_shipping_state,
            SELLER_SHIP_T.country as seller_shipping_country
            
            
        FROM CT1 , SELLER_DEF_T, SELLER_SHIP_T
        WHERE seller_pk = SELLER_DEF_T.seller_pk_seller_addresses_xref
            AND seller_pk = SELLER_SHIP_T.seller_pk_seller_addresses_xref
    ),
    SELLER_BILL_T AS (
        SELECT country, state, seller_pk_seller_addresses_xref
        FROM seller_addresses LEFT JOIN seller_addresses_xref ON id_seller_addresses = address_id
        WHERE address_type LIKE 'SELLER_BILLING' 
    ),
    CT3 AS (
        SELECT  seller_pk,
            seller_status,
            seller_status_code,
            seller_rating,
            seller_registered,
            seller_agreement_start_date,
            seller_destinguished,
            seller_default_state,
            seller_default_country,
            seller_shipping_state,
            seller_shipping_country,
            state as seller_billing_state,
            country as seller_billing_country
        FROM CT2 LEFT JOIN SELLER_BILL_T ON seller_pk = seller_pk_seller_addresses_xref
    ),
    CT4 AS (
        SELECT seller_pk,
            seller_status,
            seller_status_code,
            seller_rating,
            seller_registered,
            seller_agreement_start_date,
            seller_destinguished,
            seller_default_state,
            seller_default_country,
            seller_shipping_state,
            seller_shipping_country,
            seller_billing_state,
            seller_billing_country,
            contact_id as seller_contact_id
        FROM CT3 LEFT JOIN salesforce_contact_records ON seller_pk = seller_pk_salesforce_contact_records

    )
    SELECT  seller_pk,
            seller_status,
            seller_status_code,
            seller_rating,
            seller_registered,
            seller_agreement_start_date,
            seller_destinguished,
            seller_default_state,
            seller_default_country,
            seller_shipping_state,
            seller_shipping_country,
            seller_billing_state,
            seller_billing_country,
            seller_contact_id,
            account_id as seller_account_id
    FROM CT4 LEFT JOIN salesforce_account_records ON seller_pk = seller_pk_salesforce_account_records
    LIMIT 10;


"""
temp_df_master = pd.read_sql_query(master_q, engine)
with open('FINAL_FORMAT.csv', 'w') as file:
    pass
print(temp_df_master)
temp_df_master.to_csv('FINAL_FORMAT.csv', index=False)