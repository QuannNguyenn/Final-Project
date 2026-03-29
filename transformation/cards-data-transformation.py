import pyodbc
import pandas as pd
import numpy as np
from word2number import w2n
import re

server = r'QUAN-CORNER\SQLEXPRESS02'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE=dwh;Trusted_Connection=yes;TrustServerCertificate=yes;'
conn = pyodbc.connect(conn_str, autocommit=True)
cursor = conn.cursor()

# importing as pandas df from sql server
cards_data_df = pd.read_sql("SELECT * FROM ingestion.cards_data", conn)

# Looking for null in the table
## 2 columns cards_brand and card_type have null values

# Only card_brand and card_type have null values, replace null values with "Not Available"
cards_data_df["card_brand"] = cards_data_df["card_brand"].fillna("Not Available")
cards_data_df["card_type"] = cards_data_df["card_type"].fillna("Not Available")

# Check again

# Checking for inconsistency in formatting for text columns
cards_data_df["card_brand"].unique()
cards_data_df["card_type"].unique()
cards_data_df["has_chip"].unique()
cards_data_df["year_pin_last_changed"].unique()
cards_data_df["card_on_dark_web"].unique()
cards_data_df["issuer_bank_name"].unique()
cards_data_df["issuer_bank_state"].unique()
cards_data_df["issuer_bank_type"].unique()
cards_data_df["issuer_risk_rating"].unique()

## Columns that are found to have inconsistent formating are: card_brand, card_type, issuer_bank_name, issuer_bank_state, issuer_bank_type, and issuer_risk_rating

# ------------------------------------------------------------------------
# Cleaning the naming scheme of inconsistent columns
## card_brand
def clean_card_brand(col):
    col = col.astype(str).str.strip().str.lower()

    return col.replace({
        # VISA
        r"^v$": "Visa",
        r"^visa.*": "Visa",
        r"^vissa.*": "Visa",
        r"^vvisa.*": "Visa",
        r"^vis$": "Visa",
        r"^v!sa.*": "Visa",
        r"^visa-card.*": "Visa",

        # MASTERCARD
        r"^master.*card.*": "Mastercard",
        r"^mastercard.*": "Mastercard",
        r"^master card.*": "Mastercard",
        r"^master\s+card.*": "Mastercard",

        # AMEX
        r"^amex.*": "Amex",
        r"^ame\s*x.*": "Amex",

        # DISCOVER
        r"^discover.*": "Discover",
        r"^dis\s*cover.*": "Discover",

        # UNKNOWN → Not Available
        r"^unknown$": "Not Available",
        r"^not available$": "Not Available"
    }, regex=True)

cards_data_df["card_brand"] = clean_card_brand(cards_data_df["card_brand"])

cards_data_df["card_brand"].unique()

## card_type
def clean_card_type(col):
    col = col.astype(str).str.strip().str.lower()

    return col.replace({
        # CREDIT 
        r"^credit.*": "Credit",
        r"^cr.*": "Credit",
        r"^cc$": "Credit",
        r".*credit.*": "Credit",

        # PREPAID
        r".*pre\s*paid.*": "Prepaid",
        r".*prepaid.*": "Prepaid",
        r".*prepiad.*": "Prepaid",
        r"^ppd$": "Prepaid",
        r"^dpp$": "Prepaid",
        r"^db-pp$": "Prepaid",
        r"^dp$": "Prepaid",

        # DEBIT
        r"^debit.*": "Debit",
        r"^deb.*": "Debit",
        r"^db$": "Debit",
        r"^d$": "Debit",
        r".*debit.*": "Debit",
        r".*bank debit.*": "Debit",

        # UNKNOWN
        r"^unknown$": "Not Available",
        r"^not available$": "Not Available"
    }, regex=True)

cards_data_df["card_type"] = clean_card_type(cards_data_df["card_type"])

cards_data_df["card_brand"].unique()

## issuer_bank_name
def clean_bank_name(col):
    col = col.astype(str).str.strip().str.lower()

    return col.replace({
        # --- WELLS FARGO ---
        r".*wells\s*fargo.*": "Wells Fargo",

        # --- CITI ---
        r"^citi.*": "Citi",

        # --- CHASE ---
        r".*jpmorgan\s*chase.*": "Chase",
        r".*jp\s*morgan\s*chase.*": "Chase",
        r".*chase.*": "Chase",

        # --- BANK OF AMERICA ---
        r".*bank\s*of\s*america.*": "Bank of America",
        r".*bk\s*of\s*america.*": "Bank of America",

        # --- CAPITAL ONE ---
        r".*capital\s*one.*": "Capital One",

        # --- U.S. BANK ---
        r".*u\.?s\.?\s*bank.*": "U.S. Bank",
        r".*u\.?s\.?\s*bk.*": "U.S. Bank",

        # --- DISCOVER ---
        r".*discover.*": "Discover",

        # --- PNC ---
        r".*pnc.*": "PNC",

        # --- ALLY ---
        r".*ally.*": "Ally",

        # --- TRUIST ---
        r".*truist.*": "Truist",

        # --- UNKNOWN ---
        r"^unknown$": "Not Available",
        r"^not available$": "Not Available"
    }, regex=True)

cards_data_df["issuer_bank_name"] = clean_bank_name(cards_data_df["issuer_bank_name"])

cards_data_df["issuer_bank_name"].unique()

## Isssuer_bank_state
def clean_state(col):
    col = col.astype(str).str.strip().str.lower()

    return col.replace({
        # --- CALIFORNIA ---
        r"^california$": "CA",
        r"^ca$": "CA",

        # --- NEW YORK ---
        r"^new york$": "NY",
        r"^ny$": "NY",

        # --- NORTH CAROLINA ---
        r"^north carolina$": "NC",
        r"^nc$": "NC",

        # --- VIRGINIA ---
        r"^virginia$": "VA",
        r"^va$": "VA",

        # --- MINNESOTA ---
        r"^minnesota$": "MN",
        r"^mn$": "MN",

        # --- ILLINOIS ---
        r"^illinois$": "IL",
        r"^il$": "IL",

        # --- PENNSYLVANIA ---
        r"^pennsylvania$": "PA",
        r"^pa$": "PA",

        # --- MICHIGAN ---
        r"^michigan$": "MI",
        r"^mi$": "MI"
    }, regex=True)

cards_data_df["issuer_bank_state"] = clean_state(cards_data_df["issuer_bank_state"])

cards_data_df["issuer_bank_state"].unique()

## Issuer bank type
def clean_bank_type(col):
    col = col.astype(str).str.strip().str.lower()

    return col.replace({
        # --- NATIONAL ---
        r".*national.*": "National",

        # --- REGIONAL ---
        r".*regional.*": "Regional",

        # --- ONLINE ---
        r".*online.*": "Online",

        # --- UNKNOWN (optional) ---
        r"^unknown$": "Not Available",
        r"^not available$": "Not Available"
    }, regex=True)

cards_data_df["issuer_bank_type"] = clean_bank_type(cards_data_df["issuer_bank_type"])

cards_data_df["issuer_bank_type"].unique()


## Issuer risk rating
def clean_risk_rating(col):
    col = col.astype(str).str.strip().str.lower()

    return col.replace({
        # --- LOW ---
        r".*low.*": "Low",

        # --- MEDIUM ---
        r".*med.*": "Medium",

        # --- UNKNOWN (optional) ---
        r"^unknown$": "Not Available",
        r"^not available$": "Not Available"
    }, regex=True)


cards_data_df["issuer_risk_rating"] = clean_risk_rating(cards_data_df["issuer_risk_rating"])

cards_data_df["issuer_risk_rating"].unique()


# ------------------------------------------------------------------------
# Fixing numeric columns

## Card number 
### Card numbers are float values, so we need to remove the decimal point as well as the digit that comes after it
def clean_card_number(x):
    if pd.isna(x):
        return None

    # Convert to string
    x = str(x).strip()

    # If there's a decimal → keep only part before '.'
    if "." in x:
        x = x.split(".")[0]

    # Remove all non-digit characters
    digits = ''.join(ch for ch in x if ch.isdigit())

    return digits if digits else None

cards_data_df["card_number"] = cards_data_df["card_number"].apply(clean_card_number) # Remove the decimal and digit

## id
duplicates = cards_data_df[
    cards_data_df["id"].duplicated(keep=False)
].sort_values("id")

duplicates[["id"]]

print(duplicates)
priority = {"Low": 0, "Medium": 1}

cards_data_df = (
    cards_data_df.assign(risk_order=cards_data_df["issuer_risk_rating"].map(priority))
    .sort_values(["id", "risk_order"])
    .drop_duplicates(subset="id", keep="first")
    .drop(columns="risk_order")
)


duplicates_check = cards_data_df[
    cards_data_df["id"].duplicated(keep=False)
].sort_values("id")

print(duplicates_check)
### There are id duplicates. Upon further inspection, duplicated ids have the same client, brand, card number and cvv, indicating the same card. Because of this, we are motivated to remove the duplicated the row.
### The suggested logic is that the row with the highest issuer risk rating to be removed.

## CVV
### In the dimensional model, there would be no use case for CVV data at all, as it does not provide analytical value, and contain highly sensitive information. Hence we are motivated to drop CVV in the transformation process
cards_data_df = cards_data_df.drop(columns=["cvv"])

## credit_limit

def clean_credit_limit(col):
    s = col.astype(str).str.strip().str.lower()

    s = s.replace({
        "error_value": pd.NA,
        "limit_unknown": pd.NA,
        "unknown": pd.NA,
        "not available": pd.NA
    })

    cleaned_text = s.str.replace(r"[$,\s]", "", regex=True)

    k_mask = cleaned_text.str.fullmatch(r"-?\d+(\.\d+)?k", na=False)
    k_numeric = pd.to_numeric(
        cleaned_text.where(k_mask).str.replace("k", "", regex=False),
        errors="coerce"
    ) * 1000

    normal_numeric = pd.to_numeric(cleaned_text.where(~k_mask), errors="coerce")

    result = k_numeric.fillna(normal_numeric)

    remaining_mask = result.isna() & s.notna()
    if remaining_mask.any():
        from word2number import w2n

        def try_word_to_num(x):
            try:
                return w2n.word_to_num(str(x))
            except Exception:
                return np.nan

        word_numeric = s.where(remaining_mask).apply(try_word_to_num)
        result = result.fillna(word_numeric)

    result = result.abs()

    # force numeric again before Int64 cast
    result = pd.to_numeric(result, errors="coerce")

    return result.round().astype("Int64")

cards_data_df["credit_limit"] = clean_credit_limit(cards_data_df["credit_limit"])

cards_data_df['credit_limit'] = cards_data_df['credit_limit'].astype(object).where(cards_data_df['credit_limit'].notnull(), None)

cards_data_df.head(20)

### From the list, we can see that there are k values, error_value, limit_unknown and written number (ten thousand). We need to normalize this. For error_value and limit_unknown, it will be converted to 'Not Available',
### and for the k values and writte number, they will be converted to their numeric form

## has_chip
cards_data_df["has_chip"].unique()
### Since its already consistent, there is no need to transform 

## year_pin_last_changed
cards_data_df["year_pin_last_changed"].unique()
### Since its already consistent, there is no need to transform 

## card_on_dark_web
cards_data_df["card_on_dark_web"].unique()
### Since its already consistent, there is no need to transform 

## acct_open_date
def clean_date_flexible(col, allow_future=True, reference_year=2026):
    original = col.copy()
    s = col.astype(str).str.strip()

    missing_values = {"not available", "unknown", "none", "null", "", "nan"}
    s = s.mask(s.str.lower().isin(missing_values), pd.NA)

    def parse_value(val):
        if pd.isna(val):
            return pd.NaT

        v = str(val).strip()

        # 1) Month-Year text format like Sep-02
        m_mon_yy = re.fullmatch(r"([A-Za-z]{3})-(\d{2})", v)
        if m_mon_yy:
            mon = m_mon_yy.group(1)
            yy = int(m_mon_yy.group(2))
            year = 2000 + yy if yy <= 26 else 1900 + yy
            return pd.to_datetime(f"01-{mon}-{year}", format="%d-%b-%Y", errors="coerce")

        # 2) Expiry format like 04/24 or 4/24
        m_mm_yy = re.fullmatch(r"(\d{1,2})/(\d{2})", v)
        if m_mm_yy:
            mm = int(m_mm_yy.group(1))
            yy = int(m_mm_yy.group(2))
            year = 2000 + yy if yy <= 26 else 1900 + yy
            if 1 <= mm <= 12:
                return pd.Timestamp(year=year, month=mm, day=1)
            return pd.NaT

        # 3) Day-Month format like 6-Jan
        m_day_mon = re.fullmatch(r"(\d{1,2})-([A-Za-z]{3})", v)
        if m_day_mon:
            return pd.to_datetime(f"{v}-{reference_year}", format="%d-%b-%Y", errors="coerce")

        # 4) Full numeric date like 01-01-98
        m_dd_mm_yy = re.fullmatch(r"(\d{2})-(\d{2})-(\d{2})", v)
        if m_dd_mm_yy:
            dd = int(m_dd_mm_yy.group(1))
            mm = int(m_dd_mm_yy.group(2))
            yy = int(m_dd_mm_yy.group(3))
            year = 2000 + yy if yy <= 26 else 1900 + yy
            try:
                return pd.Timestamp(year=year, month=mm, day=dd)
            except ValueError:
                return pd.NaT

        # 5) ISO / full normal dates as fallback
        parsed = pd.to_datetime(v, errors='coerce', dayfirst=False)
        if pd.notna(parsed):
            return parsed

        return pd.NaT

    parsed = s.apply(parse_value)

    if not allow_future:
        today = pd.Timestamp.today().normalize()
        parsed = parsed.where(parsed <= today, pd.NaT)

    invalid_mask = parsed.isna() & s.notna()
    invalid_vals = original[invalid_mask].unique()

    return parsed, invalid_vals

cards_data_df["acct_open_date"], inv_acct = clean_date_flexible(
    cards_data_df["acct_open_date"], allow_future=False
)
### acct_open_date has a lot of formatting inconsistency. The function adressed all of those inconsistencies by converting the 4 cases into the standardized date format (refer to function above)
### future dates for acct_open_date doesn't make any sense, so along with unknown or not available rows, they are converted to NaT (The choice for this is that there is no analytical value in categorizing
### Not Available, and it makes it easier to ingest the data back into SQL)


## expires
cards_data_df["expires"], inv_exp = clean_date_flexible(
    cards_data_df["expires"], allow_future=True
)

### Used the same function as acct_open_date, though its formatting is way cleaner where it only have the month - year format
cards_data_df.head(10)

cards_data_df["acct_open_date"] = cards_data_df["acct_open_date"].where(cards_data_df["acct_open_date"].notna(), None)
cards_data_df["expires"] = cards_data_df["expires"].where(cards_data_df["expires"].notna(), None)

#---------------------------------------------------------------------------------------------------------------
# Loading transformed data into transformation layer schema
cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('transformation.cards_data') AND type = 'U')
    BEGIN
        CREATE TABLE [transformation].[cards_data] (
            id INT,
            client_id INT,
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
            issuer_risk_rating VARCHAR(20)
            )   
        END
""")

#---------------------------------------------------------------------------------------------------------------
# Ingesting the data into the dimensional model
## Function to load the df into sql
cursor.execute("TRUNCATE TABLE transformation.cards_data")

# Insert saving
df_to_load = cards_data_df.where(cards_data_df.notnull(), None)
data_list = [
    tuple(None if pd.isna(val) else val for val in row) 
    for row in cards_data_df.itertuples(index=False, name=None)
]

insert_query = """
    INSERT INTO transformation.cards_data
    (id, client_id, card_brand, card_type, card_number, expires, has_chip, num_cards_issued, credit_limit, acct_open_date, year_pin_last_changed, card_on_dark_web, issuer_bank_name, issuer_bank_state, issuer_bank_type,issuer_risk_rating) 
    VALUES (?, ?, ?, ?,?, ?, ?, ?,?, ?, ?, ?,?, ?, ?, ?)
"""
cursor.fast_executemany = True

print(f"Loading {len(data_list)} rows directly...")
cursor.executemany(insert_query, data_list)
conn.commit()
print("Success!") 



conn.close()
