import pandas as pd
import chardet
import csv
from pandas.errors import EmptyDataError
from typing import Iterable


def detect_encoding(file_path, sample_size=10000):
    """Detect file encoding using chardet."""
    with open(file_path, 'rb') as f:
        raw_data = f.read(sample_size)
    result = chardet.detect(raw_data)
    return result.get('encoding'), result.get('confidence', 0.0)


def read_csv_with_fallback(file_path, sep=";", detected_encoding=None, fallback_encodings: Iterable[str] | None = None, empty_ok: bool = False, **kwargs):
    """Read a CSV with fallback encodings instead of trusting chardet blindly."""
    if fallback_encodings is None:
        fallback_encodings = ["utf-8", "utf-8-sig", "cp1252", "latin1", "iso-8859-1", "cp1250"]

    encodings_to_try = []
    if detected_encoding:
        encodings_to_try.append(detected_encoding)
    encodings_to_try.extend(fallback_encodings)

    # Remove duplicates while preserving order
    encodings_to_try = list(dict.fromkeys(encodings_to_try))

    last_error = None
    for encoding in encodings_to_try:
        try:
            df = pd.read_csv(file_path, sep=sep, encoding=encoding, **kwargs)
            print(f"Successfully read {file_path} with encoding: {encoding}")
            return df, encoding
        except UnicodeDecodeError as exc:
            print(f"Failed reading {file_path} with encoding {encoding}: {exc}")
            last_error = exc
        except EmptyDataError:
            print(f"File is empty or has no columns: {file_path}")
            if empty_ok:
                return pd.DataFrame(), encoding
            raise

    if last_error is not None:
        raise last_error
    raise ValueError(f"Could not decode file: {file_path}")

def merge_financing_files(bank_account_path, credit_card_path, output_file_path, paypal_path=None):
    # 🔍 Detect encoding of the bank account file
    bank_encoding, bank_confidence = detect_encoding(bank_account_path)
    print(f"Detected encoding for bank account file: {bank_encoding} (confidence: {bank_confidence})")

    # 🔍 Detect encoding of the credit card file
    credit_card_encoding, credit_card_confidence = detect_encoding(credit_card_path)
    print(f"Detected encoding for credit card file: {credit_card_encoding} (confidence: {credit_card_confidence})")

    # Read CSV files using detected encoding as a hint, with robust fallbacks
    bank_account_df, bank_used_encoding = read_csv_with_fallback(
        bank_account_path,
        sep=";",
        detected_encoding=bank_encoding,
        on_bad_lines='skip',
        engine='python'
    )
    credit_card_df, credit_card_used_encoding = read_csv_with_fallback(
        credit_card_path,
        sep=";",
        detected_encoding=credit_card_encoding,
        on_bad_lines='skip',
        engine='python'
    )
    print(f"Final encoding used for bank account file: {bank_used_encoding}")
    print(f"Final encoding used for credit card file: {credit_card_used_encoding}")

    paypal_df = pd.DataFrame()
    if paypal_path:
        try:
            paypal_df, paypal_used_encoding = read_csv_with_fallback(
                paypal_path,
                sep=",",
                detected_encoding="utf-8-sig",
                on_bad_lines="skip",
                engine="python",
                empty_ok=True
            )
            if not paypal_df.empty:
                paypal_df.columns = paypal_df.columns.str.strip()
                print(f"Final encoding used for PayPal file: {paypal_used_encoding}")
                print("PayPal columns after cleaning:", list(paypal_df.columns))
            else:
                print(f"PayPal file is empty: {paypal_path}")
        except EmptyDataError:
            print(f"PayPal file is empty and will be ignored: {paypal_path}")
            paypal_df = pd.DataFrame()
    

    # Convert monetary columns from comma to point format
    if 'Betrag' in bank_account_df.columns:
        bank_account_df['Betrag'] = bank_account_df['Betrag'].astype(str).str.replace(",", ".").astype(float)
    if 'Buchungsbetrag' in credit_card_df.columns:
        credit_card_df['Buchungsbetrag'] = credit_card_df['Buchungsbetrag'].astype(str).str.replace(",", ".").astype(float)
    if 'Gross' in paypal_df.columns:
        paypal_df['Gross'] = paypal_df['Gross'].astype(str).str.replace(",", ".").astype(float)

    # Parse dates with the appropriate formats for each file
    if 'Date' in paypal_df.columns:
        paypal_df['Date'] = pd.to_datetime(paypal_df['Date'], format='%d/%m/%Y', errors='coerce').dt.strftime('%Y-%m-%d')
    if 'Buchungstag' in bank_account_df.columns:
        bank_account_df['Buchungstag'] = pd.to_datetime(bank_account_df['Buchungstag'], format='%d.%m.%y', errors='coerce').dt.strftime('%Y-%m-%d')
    if 'Valutadatum' in bank_account_df.columns:
        bank_account_df['Valutadatum'] = pd.to_datetime(bank_account_df['Valutadatum'], format='%d.%m.%y', errors='coerce').dt.strftime('%Y-%m-%d')
    if 'Belegdatum' in credit_card_df.columns:
        credit_card_df['Belegdatum'] = pd.to_datetime(credit_card_df['Belegdatum'], format='%d.%m.%y', errors='coerce').dt.strftime('%Y-%m-%d')
    if 'Buchungsdatum' in credit_card_df.columns:
        credit_card_df['Buchungsdatum'] = pd.to_datetime(credit_card_df['Buchungsdatum'], format='%d.%m.%y', errors='coerce').dt.strftime('%Y-%m-%d')

    # Map columns to the unified structure
    def series_or_na(df: pd.DataFrame, col: str) -> pd.Series:
        """Return a Series aligned to df.index; if col missing, return NA Series of same length."""
        if col in df.columns:
            return df[col]
        return pd.Series([pd.NA] * len(df), index=df.index)

    bank_account_output = pd.DataFrame(index=bank_account_df.index)
    bank_account_output["Belegdatum"] = series_or_na(bank_account_df, "Buchungstag")
    bank_account_output["Transaktionsdatum"] = series_or_na(bank_account_df, "Valutadatum")
    bank_account_output["Buchungsbetrag"] = series_or_na(bank_account_df, "Betrag")
    bank_account_output["Transaktionspartner"] = series_or_na(bank_account_df, "Beguenstigter/Zahlungspflichtiger")
    bank_account_output["Beschreibung"] = series_or_na(bank_account_df, "Verwendungszweck")
    bank_account_output["IBAN"] = series_or_na(bank_account_df, "Kontonummer/IBAN")
    bank_account_output["BIC"] = series_or_na(bank_account_df, "BIC (SWIFT-Code)")
    bank_account_output["Kategorie"] = series_or_na(bank_account_df, "Kategorie")

    credit_card_output = pd.DataFrame(index=credit_card_df.index)
    credit_card_output["Belegdatum"] = series_or_na(credit_card_df, "Belegdatum")
    credit_card_output["Transaktionsdatum"] = series_or_na(credit_card_df, "Buchungsdatum")
    credit_card_output["Buchungsbetrag"] = series_or_na(credit_card_df, "Buchungsbetrag")
    credit_card_output["Transaktionspartner"] = series_or_na(credit_card_df, "Transaktionsbeschreibung")
    credit_card_output["Beschreibung"] = series_or_na(credit_card_df, "Transaktionsbeschreibung Zusatz")
    credit_card_output["IBAN"] = pd.NA
    credit_card_output["BIC"] = pd.NA
    credit_card_output["Kategorie"] = pd.NA

    if paypal_path and not paypal_df.empty:
        paypal_output = pd.DataFrame(index=paypal_df.index)
        paypal_output["Belegdatum"] = series_or_na(paypal_df, "Date")
        paypal_output["Transaktionsdatum"] = series_or_na(paypal_df, "Date")
        paypal_output["Buchungsbetrag"] = series_or_na(paypal_df, "Gross")
        paypal_output["Transaktionspartner"] = series_or_na(paypal_df, "Name")
        paypal_output["Beschreibung"] = series_or_na(paypal_df, "Subject")
        paypal_output["IBAN"] = pd.NA
        paypal_output["BIC"] = pd.NA
        paypal_output["Kategorie"] = pd.NA
    else:
        paypal_output = pd.DataFrame()

    # Combine all outputs
    merged_output = pd.concat([bank_account_output, credit_card_output, paypal_output], ignore_index=True)
    merged_output.to_csv(output_file_path, index=False)
    print(f"Merged file saved to {output_file_path}")

def clean_dataframe(df):
    """ Cleans dataframe by standardizing formats and filling missing data. """
    df.columns = df.columns.str.strip()
    df["Transaktionsdatum"] = df["Transaktionsdatum"].fillna(df["Belegdatum"])
    for col in ["Transaktionspartner", "Beschreibung"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)
    df["Buchungsbetrag"] = df["Buchungsbetrag"].astype(str).str.replace(",", ".").astype(float)
    return df

def merge_latest_files(latest_file_path, last_latest_file_path, output_file_path):
    """ Merges two files and sorts them by date. """
    def detect_delimiter(file_path):
        with open(file_path, "r", encoding="ISO-8859-1") as f:
            first_line = f.readline().strip()
        if not first_line:
            return ";"
        return "," if "," in first_line else ";"

    latest_delimiter = detect_delimiter(latest_file_path)
    last_latest_delimiter = detect_delimiter(last_latest_file_path)

    try:
        latest_df = pd.read_csv(latest_file_path, sep=latest_delimiter, encoding="ISO-8859-1")
    except EmptyDataError:
        print(f"Latest file is empty: {latest_file_path}")
        latest_df = pd.DataFrame(columns=["Belegdatum", "Transaktionsdatum", "Buchungsbetrag", "Beschreibung", "Transaktionspartner", "IBAN", "BIC", "Kategorie"])

    try:
        last_latest_df = pd.read_csv(last_latest_file_path, sep=last_latest_delimiter, encoding="ISO-8859-1")
    except EmptyDataError:
        print(f"Previous merged file is empty: {last_latest_file_path}")
        last_latest_df = pd.DataFrame(columns=["Belegdatum", "Transaktionsdatum", "Buchungsbetrag", "Beschreibung", "Transaktionspartner", "IBAN", "BIC", "Kategorie"])

    latest_df = clean_dataframe(latest_df)
    last_latest_df = clean_dataframe(last_latest_df)

    required_columns = ["Belegdatum", "Transaktionsdatum", "Buchungsbetrag", "Beschreibung", "Transaktionspartner", "IBAN", "BIC", "Kategorie"]
    for col in required_columns:
        if col not in latest_df.columns:
            latest_df[col] = pd.NA
        if col not in last_latest_df.columns:
            last_latest_df[col] = pd.NA

    # Concatenate dataframes, with latest_df first to prioritize its values
    combined_df = pd.concat([latest_df, last_latest_df], ignore_index=True)
    
    # Columns used to identify unique transactions
    id_columns = ["Belegdatum", "Transaktionsdatum", "Buchungsbetrag"] #, "Beschreibung"
    
    # Remove duplicates based on id columns, keeping first occurrence (from latest_df)
    merged_df = combined_df.drop_duplicates(subset=id_columns, keep='last')
    
    # Convert dates and sort
    merged_df["Belegdatum"] = pd.to_datetime(merged_df["Belegdatum"], format="%Y-%m-%d", errors="coerce")
    merged_df["Transaktionsdatum"] = pd.to_datetime(merged_df["Transaktionsdatum"], format="%Y-%m-%d", errors="coerce")
    merged_df = merged_df.sort_values(by=["Belegdatum", "Transaktionsdatum"], ascending=[False, False])

    # Save the sorted file
    merged_df.to_csv(output_file_path, sep=";", index=False, encoding="ISO-8859-1")
    print(f"✅ Merged and sorted file saved to {output_file_path}")

def summarize_by_month_and_category(input_file, output_file, categories_to_include):
    """
    Summarizes transactions by month and category, halves shared categories, and saves the breakdown.
    """
    import pandas as pd

    # Detect delimiter
    def detect_delimiter(file_path):
        with open(file_path, "r", encoding="ISO-8859-1") as f:
            return "," if "," in f.readline() else ";"

    delimiter = detect_delimiter(input_file)

    # Load data
    df = pd.read_csv(input_file, sep=delimiter, encoding="ISO-8859-1")
    df.columns = df.columns.str.strip()

    # Convert dates and amounts
    df["Belegdatum"] = pd.to_datetime(df["Belegdatum"], format="%Y-%m-%d", errors="coerce")
    df["Jahr-Monat"] = df["Belegdatum"].dt.strftime("%Y-%m")
    df["Buchungsbetrag"] = df["Buchungsbetrag"].astype(str).str.replace(",", ".").astype(float)

    # Filter by included categories
    df_filtered = df[df["Kategorie"].isin(categories_to_include)].copy()

    # Define shared categories to divide by 2
    shared_categories = ["Strom", "Wasser", "Internet"]

    # Adjust values for shared categories
    df_filtered["Betrag_adjusted"] = df_filtered.apply(
        lambda row: row["Buchungsbetrag"] / 2 if row["Kategorie"] in shared_categories else row["Buchungsbetrag"],
        axis=1
    )

    # Group by month and category
    breakdown = df_filtered.groupby(["Jahr-Monat", "Kategorie"])["Betrag_adjusted"].sum().reset_index()

    # Sort by date and category
    breakdown = breakdown.sort_values(by=["Jahr-Monat", "Kategorie"], ascending=[False, True])

    # Save to file
    breakdown.to_csv(output_file, sep=";", index=False, encoding="ISO-8859-1")
    print(f"✅ Breakdown saved to {output_file}")
    print("\n🔍 Breakdown by Month and Category:\n", breakdown)

'''
# Example usage
input_file = "/Users/valentinweyer/Downloads/merged_final_06_04_2025.csv"
output_monthly_summary_file = "/Users/valentinweyer/Downloads/monthly_summary_filtered_06_04_2025.csv"

# 🔍 Only include transactions in these categories
categories_to_include = ["Lebensmittel", "Internet", "Strom", "Wasser", "Bildung", "Transport"]
#summarize_by_month_and_category(input_file, output_monthly_summary_file, categories_to_include)
'''

def summarize_transactions_by_month_filtered(input_file, output_monthly_summary_file, categories_to_include):
    """
    Filters transactions by category, halves shared costs, and sums amounts per month.
    """
    import pandas as pd

    # Detect delimiter
    def detect_delimiter(file_path):
        with open(file_path, "r", encoding="ISO-8859-1") as f:
            return "," if "," in f.readline() else ";"

    delimiter = detect_delimiter(input_file)

    # Load data
    df = pd.read_csv(input_file, sep=delimiter, encoding="ISO-8859-1")
    df.columns = df.columns.str.strip()

    # Convert dates and amounts
    df["Belegdatum"] = pd.to_datetime(df["Belegdatum"], format="%Y-%m-%d", errors="coerce")
    df["Jahr-Monat"] = df["Belegdatum"].dt.strftime("%Y-%m")
    df["Buchungsbetrag"] = df["Buchungsbetrag"].astype(str).str.replace(",", ".").astype(float)

    # Filter categories
    df_filtered = df[df["Kategorie"].isin(categories_to_include)].copy()

    # Define shared-cost categories
    shared_categories = ["Strom", "Wasser", "Internet"]

    # Adjust shared costs
    df_filtered["Buchungsbetrag_adjusted"] = df_filtered.apply(
        lambda row: row["Buchungsbetrag"] / 2 if row["Kategorie"] in shared_categories else row["Buchungsbetrag"],
        axis=1
    )

    # Group by month and sum
    monthly_summary = df_filtered.groupby("Jahr-Monat")["Buchungsbetrag_adjusted"].sum().reset_index()
    monthly_summary = monthly_summary.sort_values(by="Jahr-Monat", ascending=False)

    # Save and print
    monthly_summary.to_csv(output_monthly_summary_file, sep=";", index=False, encoding="ISO-8859-1")
    print(f"✅ Filtered monthly summary with shared costs saved to {output_monthly_summary_file}")
    print("\n🔹 Filtered Monthly Summary:\n", monthly_summary)





root_folder = "/Users/valentinweyer/Dropbox/Valentin/Projekte/Finance_Tracker/Files/"
#latest_file = f"{root_folder}/merged_final_06_04_2025.csv"

bank_account_path = f"{root_folder}Konto_Stand_04_07_2026.CSV"
credit_card_path = f"{root_folder}Karte_Stand_04_07_2026.CSV"
paypal_path = f"{root_folder}Download.CSV"
output_file_path = f"{root_folder}merged_04_07_2026.csv"

last_latest_file = f"{root_folder}merged_15_06_2026.csv"



def merge(bank_account_path, credit_card_path, root_folder_path, last_latest_file_path, output_file_path):
    merge_financing_files(bank_account_path, credit_card_path, output_file_path, paypal_path=paypal_path)
    merge_latest_files(output_file_path, last_latest_file_path, output_file_path)



merge(bank_account_path, credit_card_path, root_folder, last_latest_file, output_file_path)

#user now manually fills in the categories in the merged file, then we can summarize by month and category

categories_to_include = ["Lebensmittel", "Internet", "Strom", "Wasser", "Bildung", "Transport", "Hotel"]
output_monthly_summary_file = root_folder + "monthly_summary_04_07_2026.csv"

summarize_transactions_by_month_filtered(output_file_path, output_monthly_summary_file, categories_to_include)
summarize_by_month_and_category(output_file_path, root_folder + "monthly_summary_categories_04_07_2026.csv", categories_to_include)


