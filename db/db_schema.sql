

CREATE TABLE stdib_dibssellers (
    id INTEGER,
    seller_pk TEXT PRIMARY KEY,
    seller_status TEXT, 
    seller_status_code TEXT,
    seller_rating  TEXT, 
    seller_date_registered BIGINT,
    seller_date_contract_start BIGINT,
    seller_distinguished TEXT
);


CREATE TABLE seller_addresses (
    id_seller_addresses INTEGER,
    state TEXT,
    country TEXT,
    PRIMARY KEY (id_seller_addresses)
);

CREATE TABLE seller_addresses_xref (
    id_seller_addresses_xref INTEGER,
    seller_pk_seller_addresses_xref TEXT,
    address_id INTEGER,
    address_status TEXT,
    address_type TEXT,
    PRIMARY KEY (id_seller_addresses_xref, seller_pk_seller_addresses_xref),
    FOREIGN KEY (id_seller_addresses_xref) REFERENCES seller_addresses(id_seller_addresses),
    FOREIGN KEY (seller_pk_seller_addresses_xref) REFERENCES stdib_dibssellers(seller_pk)

);


CREATE TABLE salesforce_contact_records (
    id_salesforce_contact_records INTEGER,
    seller_pk_salesforce_contact_records TEXT,
    contact_id TEXT,
    PRIMARY KEY (seller_pk_salesforce_contact_records),
    CONSTRAINT fk_salesforce_contact_records FOREIGN KEY (seller_pk_salesforce_contact_records) REFERENCES stdib_dibssellers (seller_pk)
);

CREATE TABLE salesforce_account_records (
    id_salesforce_account_records INTEGER,
    seller_pk_salesforce_account_records TEXT,
    account_id TEXT,
    PRIMARY KEY (seller_pk_salesforce_account_records),
    CONSTRAINT fk_salesforce_account_records FOREIGN KEY (seller_pk_salesforce_account_records) REFERENCES stdib_dibssellers (seller_pk)
);

