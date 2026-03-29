import pyodbc

# Connect directly to dwh
server = r'.\SQLEXPRESS'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE=dwh;Trusted_Connection=yes;TrustServerCertificate=yes;'
conn = pyodbc.connect(conn_str, autocommit=True)
cursor = conn.cursor()

# Create schemas for data marts
cursor.execute("""               
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'finance_marts')
BEGIN
    EXEC('CREATE SCHEMA [finance_marts]')
END""")

# DROP OLD TABLES 
tables = ['fact_finance', 'dim_date_finance', 'dim_merchant_finance', 'dim_mcc_finance']
for t in tables:
    cursor.execute(f"IF OBJECT_ID('finance_marts.{t}', 'U') IS NOT NULL DROP TABLE finance_marts.{t}")

# CREATE PHYSICAL DIMENSIONS (With limited columns for security)
# Date dimension
print("- Creating Dim Date for Finance...")
cursor.execute("""
    SELECT *
    INTO finance_marts.dim_date_finance 
    FROM curated.dim_date
""") 

# merchant dimension
print("- Creating Dim Merchant for Finance...")
cursor.execute("""
    SELECT *
    INTO finance_marts.dim_merchant_finance 
    FROM curated.dim_merchant
""")

# mcc dimension
# We drop notes and updated_by since these are useless for finance analysis
# And rename some attributes for finance team to understand them
print("- Creating Dim MCC for Finance...")
cursor.execute("""
    SELECT mcc_key AS merchant_category_key, code, description
    INTO finance_marts.dim_mcc_finance 
    FROM curated.dim_mcc
""")

# finance fact table
print("- Creating Finance Fact Table for Finance...")
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
    INTO finance_marts.fact_finance
    FROM curated.fact_transactions
""")

print("Finalizing Finance Mart structure (Setting Keys)...")

# SET THE PRIMARY KEYS (Binding the Dimensions)
cursor.execute("ALTER TABLE finance_marts.dim_date_finance ADD CONSTRAINT PK_FinDate PRIMARY KEY (date_key)")
cursor.execute("ALTER TABLE finance_marts.dim_merchant_finance ADD CONSTRAINT PK_FinMerchant PRIMARY KEY (merchant_key)")
cursor.execute("ALTER TABLE finance_marts.dim_mcc_finance ADD CONSTRAINT PK_FinMCC PRIMARY KEY (merchant_category_key)")

# SET THE PRIMARY KEY FOR THE FACT TABLE
cursor.execute("ALTER TABLE finance_marts.fact_finance ADD CONSTRAINT PK_FinFact PRIMARY KEY (transaction_key)")

# SET THE FOREIGN KEYS 
# This tells SQL how the tables relate to each other.

cursor.execute("""
    ALTER TABLE finance_marts.fact_finance 
    ADD CONSTRAINT FK_FinFact_Date FOREIGN KEY (date_key) REFERENCES finance_marts.dim_date_finance(date_key)
""")

cursor.execute("""
    ALTER TABLE finance_marts.fact_finance 
    ADD CONSTRAINT FK_FinFact_Merchant FOREIGN KEY (merchant_key) REFERENCES finance_marts.dim_merchant_finance(merchant_key)
""")

cursor.execute("""
    ALTER TABLE finance_marts.fact_finance 
    ADD CONSTRAINT FK_FinFact_MCC FOREIGN KEY (merchant_category_key) REFERENCES finance_marts.dim_mcc_finance(merchant_category_key)
""")

print("Finance Data marts is created!")

# # List of Finance Team specific reports
# finance_reports = [
#     ("Total Revenue by Month", """
#         SELECT d.year, d.month, SUM(f.amount) AS total_revenue
#         FROM finance_mart.fact_finance f
#         JOIN finance_mart.dim_date_finance d ON f.date_key = d.date_key
#         GROUP BY d.year, d.month
#         ORDER BY d.year, d.month;
#     """),
    
#     ("Percentage of Transactions are Refunds", """
#         SELECT 
#             COUNT(CASE WHEN amount < 0 THEN 1 END) AS refund_count,
#             COUNT(*) AS total_transactions,
#             (COUNT(CASE WHEN amount < 0 THEN 1 END) * 100.0 / COUNT(*)) AS refund_percentage
#         FROM finance_mart.fact_finance;
#     """),
    
#     ("Top 10 States by Revenue", """
#         SELECT TOP 10 m.merchant_state, SUM(f.amount) AS total_revenue
#         FROM finance_mart.fact_finance f
#         JOIN finance_mart.dim_merchant_finance m ON f.merchant_key = m.merchant_key
#         GROUP BY m.merchant_state
#         ORDER BY total_revenue DESC;
#     """),
    
#     ("Highest Spending Merchant Categories", """
#         SELECT TOP 10 mcc.description, SUM(f.amount) AS total_spending
#         FROM finance_mart.fact_finance f
#         JOIN finance_mart.dim_mcc_finance mcc ON f.merchant_category_key = mcc.merchant_category_key
#         GROUP BY mcc.description
#         ORDER BY total_spending DESC;
#     """)
# ]

# print("Generating Finance Team Reports")

# for title, sql in finance_reports:
#     print(f"\n{title}")
#     cursor.execute(sql)
    
#     # Simplified printing using | as a separator for data rows
#     rows = cursor.fetchall()
#     for row in rows:
#         print(" | ".join(str(val) for val in row))
        
conn.close()