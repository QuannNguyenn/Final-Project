import pyodbc
import pandas as pd
import numpy as np

# Connect directly to dwh
server = r'QUAN-CORNER\SQLEXPRESS02'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE=dwh;Trusted_Connection=yes;TrustServerCertificate=yes;'
conn = pyodbc.connect(conn_str, autocommit=True)
cursor = conn.cursor()

# read transformation layer data
transactions_df = pd.read_sql('SELECT * FROM transformation.transactions_data ORDER BY id', conn)
users_df = pd.read_sql('SELECT * FROM transformation.users_data ORDER BY id', conn)
cards_df = pd.read_sql('SELECT * FROM transformation.cards_data ORDER BY id', conn)
mcc_df = pd.read_sql('SELECT * FROM transformation.mcc_data ORDER BY code', conn)

# =====================================================================================
# CREATE DATE DIMENSION
# =====================================================================================
print("Creating date dimension...")

transactions_df['date'] = pd.to_datetime(transactions_df['date'])

dim_date = transactions_df[['date']].drop_duplicates().copy()
dim_date = dim_date.sort_values('date').reset_index(drop=True)
dim_date.insert(0, 'date_key', range(1, len(dim_date) + 1))

dim_date['year'] = dim_date['date'].dt.year
dim_date['month'] = dim_date['date'].dt.month
dim_date['day'] = dim_date['date'].dt.day
dim_date['day_of_week'] = dim_date['date'].dt.day_name()
dim_date['is_weekend'] = dim_date['day_of_week'].isin(['Saturday', 'Sunday']).map({True: 'True', False: 'False'})

date_map = dict(zip(dim_date['date'], dim_date['date_key']))
transactions_df['date_key'] = transactions_df['date'].map(date_map)

# =====================================================================================
# CREATE MERCHANT DIMENSION
# =====================================================================================
print("Creating merchant dimension...")

dim_merchant = transactions_df[[
    'merchant_id',
    'merchant_city',
    'merchant_state',
    'zip',
    'in_US'
]].drop_duplicates().copy().reset_index(drop=True)

dim_merchant.insert(0, 'merchant_key', range(1, len(dim_merchant) + 1))

merchant_map = dict(zip(
    zip(
        dim_merchant['merchant_id'],
        dim_merchant['merchant_city'],
        dim_merchant['merchant_state'],
        dim_merchant['zip'],
        dim_merchant['in_US']
    ),
    dim_merchant['merchant_key']
))

transactions_df['merchant_key'] = transactions_df.apply(
    lambda row: merchant_map.get((
        row['merchant_id'],
        row['merchant_city'],
        row['merchant_state'],
        row['zip'],
        row['in_US']
    )),
    axis=1
)

# =====================================================================================
# ADD SURROGATE KEYS TO EXISTING DIMENSIONS AND FACT
# =====================================================================================
mcc_df.insert(0, 'mcc_key', range(1, len(mcc_df) + 1))
users_df.insert(0, 'user_key', range(1, len(users_df) + 1))
cards_df.insert(0, 'card_key', range(1, len(cards_df) + 1))
transactions_df.insert(0, 'transaction_key', np.arange(1, len(transactions_df) + 1))

print("Surrogate keys added to Dimensions and Fact in Python.")

# =====================================================================================
# MAP DIMENSION KEYS INTO FACT TABLE
# =====================================================================================

# MCC mapping
mcc_map = dict(zip(mcc_df['code'], mcc_df['mcc_key']))
transactions_df['mcc_key'] = transactions_df['mcc'].map(mcc_map)

# Users mapping
users_map = dict(zip(users_df['id'], users_df['user_key']))
transactions_df['user_key'] = transactions_df['client_id'].map(users_map)
cards_df['user_key'] = cards_df['client_id'].map(users_map)

# Cards mapping
cards_map = dict(zip(cards_df['id'], cards_df['card_key']))
transactions_df['card_key'] = transactions_df['card_id'].map(cards_map)

print("Mapping surrogate keys to transactions completed.")

# =====================================================================================
# DROP REDUNDANT NATURAL KEYS / DESCRIPTIVE COLUMNS FROM FACT
# =====================================================================================
cols_to_drop = [
    'client_id',
    'card_id',
    'mcc',
    'date',
    'merchant_id',
    'merchant_city',
    'merchant_state',
    'zip',
    'in_US'
]
transactions_df.drop(columns=cols_to_drop, inplace=True)

cards_df.drop(columns=['client_id'], inplace=True)

# =====================================================================================
# REORDER COLUMNS
# =====================================================================================
fact_order = [
    'transaction_key',   # PK
    'id',                # original natural transaction id
    'date_key',          # FK
    'user_key',          # FK
    'card_key',          # FK
    'mcc_key',           # FK
    'merchant_key',      # FK
    'amount',
    'use_chip',
    'errors',
    'is_error'
]

cards_order = [
    'card_key',  # PK
    'id',
    'user_key',  # FK
    'card_brand',
    'card_type',
    'card_number',
    'expires',
    'has_chip',
    'num_cards_issued',
    'credit_limit',
    'acct_open_date',
    'year_pin_last_changed',
    'card_on_dark_web',
    'issuer_bank_name',
    'issuer_bank_state',
    'issuer_bank_type',
    'issuer_risk_rating'
]

transactions_df = transactions_df[fact_order]
cards_df = cards_df[cards_order]

# =====================================================================================
# CREATE CURATED SCHEMA
# =====================================================================================
cursor.execute("""
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'curated')
BEGIN
    EXEC('CREATE SCHEMA [curated]')
END
""")

# =====================================================================================
# DROP TABLES IN CORRECT ORDER
# =====================================================================================
print("Dropping tables to ensure a fresh start...")
cursor.execute("IF OBJECT_ID('curated.fact_transactions', 'U') IS NOT NULL DROP TABLE curated.fact_transactions")
cursor.execute("IF OBJECT_ID('curated.dim_cards', 'U') IS NOT NULL DROP TABLE curated.dim_cards")
cursor.execute("IF OBJECT_ID('curated.dim_users', 'U') IS NOT NULL DROP TABLE curated.dim_users")
cursor.execute("IF OBJECT_ID('curated.dim_mcc', 'U') IS NOT NULL DROP TABLE curated.dim_mcc")
cursor.execute("IF OBJECT_ID('curated.dim_merchant', 'U') IS NOT NULL DROP TABLE curated.dim_merchant")
cursor.execute("IF OBJECT_ID('curated.dim_date', 'U') IS NOT NULL DROP TABLE curated.dim_date")
conn.commit()

# =====================================================================================
# CREATE AND LOAD DIM_DATE
# =====================================================================================
cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('curated.dim_date') AND type = 'U')
    BEGIN
        CREATE TABLE [curated].[dim_date] (
            date_key INT PRIMARY KEY,
            full_date DATETIME,
            year INT,
            month INT,
            day INT,
            day_of_week VARCHAR(20),
            is_weekend VARCHAR(10)
        )
    END
""")

data_list = [
    tuple(None if pd.isna(val) else val for val in row)
    for row in dim_date[['date_key', 'date', 'year', 'month', 'day', 'day_of_week', 'is_weekend']]
    .itertuples(index=False, name=None)
]

insert_query = """
    INSERT INTO curated.dim_date
    (date_key, full_date, year, month, day, day_of_week, is_weekend)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""

cursor.fast_executemany = True
print(f"Loading {len(data_list)} rows directly...")
cursor.executemany(insert_query, data_list)
conn.commit()
print("dim_date Success!")

# =====================================================================================
# CREATE AND LOAD DIM_MERCHANT
# =====================================================================================
cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('curated.dim_merchant') AND type = 'U')
    BEGIN
        CREATE TABLE [curated].[dim_merchant] (
            merchant_key INT PRIMARY KEY,
            merchant_id INT,
            merchant_city VARCHAR(100),
            merchant_state VARCHAR(100),
            zip VARCHAR(20),
            in_US VARCHAR(20)
        )
    END
""")

data_list = [
    tuple(None if pd.isna(val) else val for val in row)
    for row in dim_merchant.itertuples(index=False, name=None)
]

insert_query = """
    INSERT INTO curated.dim_merchant
    (merchant_key, merchant_id, merchant_city, merchant_state, zip, in_US)
    VALUES (?, ?, ?, ?, ?, ?)
"""

cursor.fast_executemany = True
print(f"Loading {len(data_list)} rows directly...")
cursor.executemany(insert_query, data_list)
conn.commit()
print("dim_merchant Success!")

# =====================================================================================
# CREATE AND LOAD DIM_MCC
# =====================================================================================
cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('curated.dim_mcc') AND type = 'U')
    BEGIN
        CREATE TABLE [curated].[dim_mcc] (
            mcc_key INT PRIMARY KEY,
            code INT,
            description VARCHAR(255),
            notes VARCHAR(255),
            updated_by VARCHAR(255)
        )
    END
""")

data_list = [
    tuple(None if pd.isna(val) else val for val in row)
    for row in mcc_df.itertuples(index=False, name=None)
]

insert_query = """
    INSERT INTO curated.dim_mcc
    (mcc_key, code, description, notes, updated_by)
    VALUES (?, ?, ?, ?, ?)
"""

cursor.fast_executemany = True
print(f"Loading {len(data_list)} rows directly...")
cursor.executemany(insert_query, data_list)
conn.commit()
print("dim_mcc Success!")

# =====================================================================================
# CREATE AND LOAD DIM_USERS
# =====================================================================================
cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('curated.dim_users') AND type = 'U')
    BEGIN
        CREATE TABLE [curated].[dim_users] (
            user_key INT PRIMARY KEY,
            id INT,
            current_age INT,
            retirement_age INT,
            birth_year INT,
            birth_month INT,
            birth_month_name VARCHAR(20),
            gender VARCHAR(20),
            address VARCHAR(255),
            latitude FLOAT,
            longitude FLOAT,
            per_capita_income INT,
            yearly_income INT,
            total_debt INT,
            credit_score INT,
            num_credit_cards INT,
            employment_status VARCHAR(20),
            education_level VARCHAR(20)
        )
    END
""")

data_list = [
    tuple(None if pd.isna(val) else val for val in row)
    for row in users_df.itertuples(index=False, name=None)
]

insert_query = """
    INSERT INTO curated.dim_users
    (user_key, id, current_age, retirement_age, birth_year, birth_month, birth_month_name, gender, address, latitude, longitude, per_capita_income, yearly_income, total_debt, credit_score, num_credit_cards, employment_status, education_level)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

cursor.fast_executemany = True
print(f"Loading {len(data_list)} rows directly...")
cursor.executemany(insert_query, data_list)
conn.commit()
print("dim_users Success!")

# =====================================================================================
# CREATE AND LOAD DIM_CARDS
# =====================================================================================
cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('curated.dim_cards') AND type = 'U')
    BEGIN
        CREATE TABLE [curated].[dim_cards] (
            card_key INT PRIMARY KEY,
            id INT,
            user_key INT,
            card_brand VARCHAR(255),
            card_type VARCHAR(255),
            card_number VARCHAR(20),
            expires DATE,
            has_chip VARCHAR(10),
            num_cards_issued INT,
            credit_limit INT,
            acct_open_date DATE,
            year_pin_last_changed INT,
            card_on_dark_web VARCHAR(10),
            issuer_bank_name VARCHAR(255),
            issuer_bank_state VARCHAR(20),
            issuer_bank_type VARCHAR(20),
            issuer_risk_rating VARCHAR(20),
            CONSTRAINT FK_CLIENT FOREIGN KEY (user_key) REFERENCES curated.dim_users(user_key)
        )
    END
""")

data_list = [
    tuple(None if pd.isna(val) else val for val in row)
    for row in cards_df.itertuples(index=False, name=None)
]

insert_query = """
    INSERT INTO curated.dim_cards
    (card_key, id, user_key, card_brand, card_type, card_number, expires, has_chip, num_cards_issued, credit_limit, acct_open_date, year_pin_last_changed, card_on_dark_web, issuer_bank_name, issuer_bank_state, issuer_bank_type, issuer_risk_rating)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

cursor.fast_executemany = True
print(f"Loading {len(data_list)} rows directly...")
cursor.executemany(insert_query, data_list)
conn.commit()
print("dim_cards Success!")

# =====================================================================================
# CREATE FACT TABLE
# =====================================================================================
cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('curated.fact_transactions') AND type = 'U')
    BEGIN
        CREATE TABLE [curated].[fact_transactions] (
            transaction_key INT PRIMARY KEY,
            id INT,
            date_key INT,
            user_key INT,
            card_key INT,
            mcc_key INT,
            merchant_key INT,
            amount FLOAT,
            use_chip VARCHAR(50),
            errors VARCHAR(255),
            is_error VARCHAR(20),
            CONSTRAINT FK_DATE_TABLE FOREIGN KEY (date_key) REFERENCES curated.dim_date(date_key),
            CONSTRAINT FK_MCC_TABLE FOREIGN KEY (mcc_key) REFERENCES curated.dim_mcc(mcc_key),
            CONSTRAINT FK_CARD_TABLE FOREIGN KEY (card_key) REFERENCES curated.dim_cards(card_key),
            CONSTRAINT FK_CLIENT_TABLE FOREIGN KEY (user_key) REFERENCES curated.dim_users(user_key),
            CONSTRAINT FK_MERCHANT_TABLE FOREIGN KEY (merchant_key) REFERENCES curated.dim_merchant(merchant_key)
        )
    END
""")

# =====================================================================================
# LOAD FACT TABLE
# =====================================================================================
temp_csv_path = r"G:\temp_data_engineering\temp_cleaned_fact.csv"

# Ensure surrogate key columns are proper nullable integers first
key_cols = [
    'transaction_key',
    'date_key',
    'user_key',
    'card_key',
    'mcc_key',
    'merchant_key'
]

for col in key_cols:
    transactions_df[col] = pd.to_numeric(transactions_df[col], errors='coerce').astype('Int64')

print("Exporting cleaned data to temporary CSV...")
transactions_df.to_csv(temp_csv_path, index=False, header=False, sep='|', encoding='utf-8')
print("Export complete.")

bulk_insert_query = f"""
BULK INSERT curated.fact_transactions
FROM '{temp_csv_path}'
WITH (FIELDTERMINATOR = '|', ROWTERMINATOR = '0x0a', TABLOCK);
"""

try:
    print("Starting SQL Bulk Insert...")
    cursor.execute(bulk_insert_query)
    conn.commit()
    print("Fact table Success!")
except Exception as e:
    print(f"Bulk Insert Failed: {e}")
finally:
    import os
    os.remove(temp_csv_path)
    conn.close()
