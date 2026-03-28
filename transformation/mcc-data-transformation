import pyodbc
import pandas as pd
import numpy as np
import re
from word2number import w2n


# Initializing the transformation process
server = r'QUAN-CORNER\SQLEXPRESS02'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE=dwh;Trusted_Connection=yes;TrustServerCertificate=yes;'
conn = pyodbc.connect(conn_str, autocommit=True)
cursor = conn.cursor()

# importing as pandas df from sql server
mcc_data_df = pd.read_sql("SELECT * FROM ingestion.mcc_data" , conn)

mcc_data_df.head(10)

#----------------------------------------------------------------------------------------------------------
# Transforming columns as necessary
## Code (merchant id)
### There are inconsistency in the code format that needs to be addressed. We only want to keep the 4 identifying digit
### In the last 2 rows, there are note and comment which are just description of the data. It won't be necessary to keep it for analytics therefore it should be removed
### After the first 2 cleaning steps, we noticed that there are duplicate id rows with identical data, hence it needs further cleaning (only 1 unique id)
def clean_mcc_code(col):
    col = col.astype(str).str.strip()
    
    return (
        col
        .str.extract(r'(\d{4})', expand=False)
        .pipe(pd.to_numeric, errors='coerce')
        .astype("Int64")  
    )

mcc_data_df["code"] = clean_mcc_code(mcc_data_df["code"])
mcc_data_df = mcc_data_df[mcc_data_df["code"].notna()]

duplicates = (
    mcc_data_df[mcc_data_df["code"].duplicated(keep=False)]
    .sort_values("code")
)

### See the duplicated rows
print(duplicates)

### Remove duplicate
mcc_data_df = mcc_data_df.drop_duplicates(subset=["code"], keep="first")

## Description
### To make easier reading and querying, standardize the text format for description so that only the first letter of each word is capitalized except small words like and, or, etc. (avoiding all capitalized text)
### Also we need to address the leading spaces  

def description_clean(col):
    col = (
        col.astype(str)
        .str.replace("_", " ", regex=False)   
        .str.strip()
        .str.lower()
        .str.title()
    )
    
    # Lowercase small words
    return col.str.replace(
        r'\b(And|Or|Of|The|In|On|At|For|To)\b',
        lambda x: x.group(0).lower(),
        regex=True
    )

mcc_data_df["description"] = description_clean(mcc_data_df["description"])

mcc_data_df.head(10)

## Notes
mcc_data_df["notes"].unique()
### For better categorization, we can convert null values to "none" instead of not available, as it can be understood that there is simply no note for this specific id, not that it is not available
mcc_data_df["notes"] = mcc_data_df["notes"].fillna("none")


## Updated by
mcc_data_df["updated_by"].unique()
### The updated_by column have hidden \r character that was not seen in excel or the sql server (seen when loading it into pandas df). If left as is it will lead to querying error (querying rows where name = john won't work)
### For better categorization, we can convert null values "unknown". It is better than not available because unknown suggests that someone updated the data, but we simply don't know who, it makes it more interpretable this way. 
mcc_data_df["updated_by"] = (
    mcc_data_df["updated_by"]
    .astype(str)
    .str.replace(r"[\r\n\t]+", "", regex=True)
    .str.strip()
    .replace("", pd.NA)   # convert empty → NA
    .fillna("unknown")    # fill NAs as unknown     
)

# This replaces pd.NA with standard Python None
mcc_data_df["code"] = mcc_data_df["code"].astype(object).where(mcc_data_df["code"].notnull(), None)

#---------------------------------------------------------------------------------------------------------------
# Loading transformed data into transformation layer schema
cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('transformation.mcc_data') AND type = 'U')
    BEGIN
        CREATE TABLE [transformation].[mcc_data] (
            code INT,
            description VARCHAR(255),
            notes VARCHAR(255),
            updated_by VARCHAR(255)
            )
        END
""")

#---------------------------------------------------------------------------------------------------------------
## Ingesting the data into the dimensional model

## Function to load the df into sql
cursor.execute("TRUNCATE TABLE transformation.mcc_data")

# Insert saving
df_to_load = mcc_data_df.where(mcc_data_df.notnull(), None)
data_list = [
    tuple(None if pd.isna(val) else val for val in row) 
    for row in mcc_data_df.itertuples(index=False, name=None)
]

insert_query = """
    INSERT INTO transformation.mcc_data
    (code, description, notes, updated_by) 
    VALUES (?, ?, ?, ?)
"""
cursor.fast_executemany = True

print(f"Loading {len(data_list)} rows directly...")
cursor.executemany(insert_query, data_list)
conn.commit()
print("Success!") 



conn.close()
