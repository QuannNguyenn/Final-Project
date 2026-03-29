import pyodbc

# Setup BASE connection
server = r'QUAN-CORNER\SQLEXPRESS02'
base_conn = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};Trusted_Connection=yes;TrustServerCertificate=yes;'

# STEP 1: CREATE DATABASE (Connect to 'master')
print("Checking Database...")
# We add DATABASE=master just for this step in order to create a new data warehouse
conn = pyodbc.connect(base_conn + "DATABASE=master;", autocommit=True)
cursor = conn.cursor()

cursor.execute("""
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'dwh')
BEGIN
    CREATE DATABASE [dwh]
END""")
conn.close()

# STEP 2: CREATE SCHEMA & TABLE (Connect to DWH)
print("Connecting to dwh...")
# We add DATABASE=DWH here
conn = pyodbc.connect(base_conn + "DATABASE=dwh;", autocommit=True)
cursor = conn.cursor()

# Create Schema
print("Checking Schema 'ingestion'...")
# Create the schema if it hasn't yet been created
cursor.execute("""               
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'ingestion')
BEGIN
    EXEC('CREATE SCHEMA [ingestion]')
END""")

print("Checking Schema 'transformation'...")
# Create the schema if it hasn't yet been created
cursor.execute("""               
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'transformation')
BEGIN
    EXEC('CREATE SCHEMA [transformation]')
END""")

# Create Table
print("Checking Table 'transactions_data'...")
# Create the table if it hasn't yet been created and drop the old table
# type = 'U' specifies the type of object we are looking for is a table
# We don't drop the tables here since we assume the structure of the tables remain consistant.
cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('ingestion.transactions_data') AND type = 'U')
        BEGIN
            CREATE TABLE [ingestion].[transactions_data] (
                id NVARCHAR(255),
                date NVARCHAR(255),
                client_id NVARCHAR(255),
                card_id NVARCHAR(255), 
                amount NVARCHAR(255), 
                use_chip NVARCHAR(255),
                merchant_id NVARCHAR(255), 
                merchant_city NVARCHAR(255),
                merchant_state NVARCHAR(255),
                zip NVARCHAR(255),
                mcc NVARCHAR(255),
                errors NVARCHAR(255)
            )   
        END
    """)


print("Checking Table 'users_data'...")
cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('ingestion.users_data') AND type = 'U')
        BEGIN
            CREATE TABLE [ingestion].[users_data] (
                id NVARCHAR(255),
                current_age NVARCHAR(255), 
                retirement_age NVARCHAR(255), 
                birth_year NVARCHAR(255),
                birth_month NVARCHAR(255),
                gender NVARCHAR(255),
                address NVARCHAR(255),
                latitude NVARCHAR(255),
                longitude NVARCHAR(255), 
                per_capita_income NVARCHAR(255),
                yearly_income NVARCHAR(255),
                total_debt NVARCHAR(255),
                credit_score NVARCHAR(255),
                num_credit_cards NVARCHAR(255),
                employment_status NVARCHAR(255),
                education_level NVARCHAR(255)
            )
        END
    """)


print("Checking Table 'cards_data'...")
cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('ingestion.cards_data') AND type = 'U')
        BEGIN
            CREATE TABLE [ingestion].[cards_data] (
                id NVARCHAR(255),
                client_id NVARCHAR(255),
                card_brand NVARCHAR(255),
                card_type NVARCHAR(255),
                card_number NVARCHAR(255),
                expires NVARCHAR(255),
                cvv NVARCHAR(255),
                has_chip NVARCHAR(255),
                num_cards_issued NVARCHAR(255),
                credit_limit NVARCHAR(255),
                acct_open_date NVARCHAR(255),
                year_pin_last_changed NVARCHAR(255),
                card_on_dark_web NVARCHAR(255),
                issuer_bank_name NVARCHAR(255),
                issuer_bank_state NVARCHAR(255),
                issuer_bank_type NVARCHAR(255),
                issuer_risk_rating NVARCHAR(255)
            )
        END
    """)


print("Checking Table 'mcc_data'...")
cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('ingestion.mcc_data') AND type = 'U')
        BEGIN
            CREATE TABLE [ingestion].[mcc_data] (
                code NVARCHAR(255),
                description NVARCHAR(255),
                notes NVARCHAR(255),
                updated_by NVARCHAR(255)
            )
        END
    """)

print("\nEmpty Table Created")
print("\nFinished")

conn.close()
