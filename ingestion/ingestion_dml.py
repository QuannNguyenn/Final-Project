import pyodbc
import pandas as pd


# Connect directly to DWH
server = r'QUAN-CORNER\SQLEXPRESS02'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE=dwh;Trusted_Connection=yes;TrustServerCertificate=yes;'
conn = pyodbc.connect(conn_str, autocommit=True)
cursor = conn.cursor()

# Clean old data
# Ensures that the tables are empty each time we run
print("Cleaning old data from all tables...")
cursor.execute("TRUNCATE TABLE ingestion.transactions_data")
cursor.execute("TRUNCATE TABLE ingestion.users_data")
cursor.execute("TRUNCATE TABLE ingestion.cards_data")
cursor.execute("TRUNCATE TABLE ingestion.mcc_data")

# STEP 3: BULK INSERT DATA
# Replace the path here (The 'r' is important!)
folder_path = r"G:\Users\Quan\Downloads\Dataset-final-project\Dataset-final-project"

print("Starting Bulk Inserts...")

# 1. Transactions
cursor.execute(f"BULK INSERT ingestion.transactions_data FROM '{folder_path}\\transactions_data.csv' WITH (FORMAT='CSV', FIRSTROW=2, CODEPAGE='65001', ROWTERMINATOR='0x0a')")
print("- transactions_data loaded.")

# 2. Users
cursor.execute(f"BULK INSERT ingestion.users_data FROM '{folder_path}\\users_data.csv' WITH (FORMAT='CSV', FIRSTROW=2, CODEPAGE='65001', ROWTERMINATOR='0x0a')")
print("- users_data loaded.")

# 3. Cards
cursor.execute(f"BULK INSERT ingestion.cards_data FROM '{folder_path}\\cards_data.csv' WITH (FORMAT='CSV', FIRSTROW=2, CODEPAGE='65001', ROWTERMINATOR='0x0a')")
print("- cards_data loaded.")

# 4. MCC
cursor.execute(f"BULK INSERT ingestion.mcc_data FROM '{folder_path}\\mcc_data.csv' WITH (FORMAT='CSV', FIRSTROW=2, CODEPAGE='65001', ROWTERMINATOR='0x0a')")
print("- mcc_data loaded.")

print("\nFinished")

conn.close()
