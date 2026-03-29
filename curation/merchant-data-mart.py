import pyodbc

server = r'QUAN-CORNER\SQLEXPRESS02'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE=dwh;Trusted_Connection=yes;TrustServerCertificate=yes;'
conn = pyodbc.connect(conn_str, autocommit=True)
cursor = conn.cursor()


#----------------------------------------------------------------------------------------
# CREATE MARTS SCHEMA IF NOT YET

cursor.execute("""
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'merchant_marts')
BEGIN
    EXEC('CREATE SCHEMA [merchant_marts]')
END
""")

#----------------------------------------------------------------------------------------
# DROP EXISTING MART TABLES

print("Dropping marts tables to ensure there is no duplicated data")

# Drop fact first due to FK dependencies
cursor.execute("IF OBJECT_ID('merchant_marts.fact_transactions', 'U') IS NOT NULL DROP TABLE merchant_marts.fact_transactions")
cursor.execute("IF OBJECT_ID('merchant_marts.dim_mcc', 'U') IS NOT NULL DROP TABLE merchant_marts.dim_mcc")
cursor.execute("IF OBJECT_ID('merchant_marts.dim_merchant', 'U') IS NOT NULL DROP TABLE merchant_marts.dim_merchant")
cursor.execute("IF OBJECT_ID('merchant_marts.dim_date', 'U') IS NOT NULL DROP TABLE merchant_marts.dim_date")

#----------------------------------------------------------------------------------------
# CREATE AND LOAD MARTS.DIM_DATE
## Date dimension table: Keep all as the specific month, day, or year can be used to perform fast query of merchant performance for a specific year, or day of a month, and full_date is useful in the case that they want to query between date ranges.

print("- Creating merchant_marts.dim_date ...")
cursor.execute("""
    SELECT
        date_key,
        full_date,
        [year],
        [month],
        [day],
        day_of_week,
        is_weekend
    INTO merchant_marts.dim_date
    FROM curated.dim_date
""")

#----------------------------------------------------------------------------------------
# CREATE AND LOAD MARTS.DIM_MERCHANT

## Merchant key  dimension table: Also keep all of the columns since one of the main objective is to see how revenue is distributed geographically between merchants. The merchant dimension have information on the merchant's city, state, zipcode, as well
## as the helper in_US would allow the department to see the sale distribution by these geographical characteristics. 

print("- Creating merchant_marts.dim_merchant ...")
cursor.execute("""
    SELECT
        merchant_key,
        merchant_id,
        merchant_city,
        merchant_state,
        zip,
        in_US
    INTO merchant_marts.dim_merchant
    FROM curated.dim_merchant
""")

#----------------------------------------------------------------------------------------
# CREATE AND LOAD MARTS.DIM_MCC

## Merchant category dimension table: Keep as the table will provide revenue information on different merchant category to identify high performing categories to focus on. However, we can drop the "notes" and the "updated_by" columns as they
## do not bring analytical value to the department's use case. 

print("- Creating merchant_marts.dim_mcc ...")
cursor.execute("""
    SELECT
        mcc_key,
        code,
        description
    INTO merchant_marts.dim_mcc
    FROM curated.dim_mcc
""")

#----------------------------------------------------------------------------------------
# CREATE MART FACT TABLE

## Transaction fact table: Transaction key, transaction id, date_key, mcc_key (merchant_category), merchant_key, amount, use_chip(payment_method), errors, is_error
## Amount is obviously for aggregation of revenue, use_chip can be useful for merchant analysis in determining things such as what kind of payment method is the most popular for a specific merchant.

print("- Creating merchant_marts.fact_transactions ...")
cursor.execute("""
    SELECT
        transaction_key,
        id AS transaction_id,
        date_key,
        mcc_key AS merchant_category_key,
        merchant_key,
        amount,
        use_chip AS payment_method,
        errors,
        is_error
    INTO merchant_marts.fact_transactions
    FROM curated.fact_transactions
""")

#----------------------------------------------------------------------------------------
# ADD PRIMARY KEYS

print("Finalizing Merchant Mart structure (Setting Keys)...")

cursor.execute("""
    ALTER TABLE merchant_marts.dim_date
    ADD CONSTRAINT PK_MART_DATE PRIMARY KEY (date_key)
""")

cursor.execute("""
    ALTER TABLE merchant_marts.dim_merchant
    ADD CONSTRAINT PK_MART_MERCHANT PRIMARY KEY (merchant_key)
""")

cursor.execute("""
    ALTER TABLE merchant_marts.dim_mcc
    ADD CONSTRAINT PK_MART_MCC PRIMARY KEY (mcc_key)
""")

cursor.execute("""
    ALTER TABLE merchant_marts.fact_transactions
    ADD CONSTRAINT PK_MART_FACT PRIMARY KEY (transaction_key)
""")

#----------------------------------------------------------------------------------------
# ADD FOREIGN KEYS

cursor.execute("""
    ALTER TABLE merchant_marts.fact_transactions
    ADD CONSTRAINT FK_MART_DATE FOREIGN KEY (date_key)
    REFERENCES merchant_marts.dim_date(date_key)
""")

cursor.execute("""
    ALTER TABLE merchant_marts.fact_transactions
    ADD CONSTRAINT FK_MART_MCC FOREIGN KEY (merchant_category_key)
    REFERENCES merchant_marts.dim_mcc(mcc_key)
""")

cursor.execute("""
    ALTER TABLE merchant_marts.fact_transactions
    ADD CONSTRAINT FK_MART_MERCHANT FOREIGN KEY (merchant_key)
    REFERENCES merchant_marts.dim_merchant(merchant_key)
""")

print("Merchant mart created successfully in schema [merchant_marts].")
conn.close()

'''# Test queries in sql 
## Get the top performing merchants
SELECT TOP 10
    mcc.description AS merchant_category,
    SUM(f.amount) AS total_revenue
FROM merchant_marts.fact_transactions f
JOIN merchant_marts.dim_mcc mcc
    ON f.merchant_category_key = mcc.mcc_key
GROUP BY mcc.description
ORDER BY total_revenue DESC;

## Highest performing merchant category
SELECT TOP 10
    mcc.description AS merchant_category,
    SUM(f.amount) AS total_revenue
FROM merchant_marts.fact_transactions f
JOIN merchant_marts.dim_mcc mcc
    ON f.merchant_category_key = mcc.mcc_key
GROUP BY mcc.description
ORDER BY total_revenue DESC;

## Top performing merchant categories by year
SELECT 
    d.year,
    mcc.description AS merchant_category,
    SUM(f.amount) AS total_revenue
FROM merchant_marts.fact_transactions f
JOIN merchant_marts.dim_mcc mcc
    ON f.merchant_category_key = mcc.mcc_key
JOIN merchant_marts.dim_date d
    ON f.date_key = d.date_key
GROUP BY d.year, mcc.description
ORDER BY d.year, total_revenue DESC;'''
