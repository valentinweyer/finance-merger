"""
Finance Tracker combiner
=========================

Merges bank, credit card, and (optionally) PayPal CSV exports into a single
running ledger, then produces monthly spending summaries.

How to use
----------
1. Export your statements and drop them into the "Files/incoming" folder
   (next to this script), using these filenames:
     - Bank account:  Konto_Stand_DD_MM_YYYY.CSV
     - Credit card:   Karte_Stand_DD_MM_YYYY.CSV
     - PayPal:        Download.CSV (or Download*.csv)
   If multiple bank/credit-card files are present, the one with the newest
   date in its filename is used. PayPal export isn't required.

2. Run the script from a terminal:
     python "Finance Tracker combiner.py"

3. What it does:
     - Reads the latest bank + credit card (+ PayPal) exports.
     - Normalizes columns/dates/amounts into a common format.
     - Merges the new transactions into "Files/ledger.csv", removing
       duplicates (matched on date + amount) so re-running is safe.
     - Backs up the previous ledger to "Files/backups/" before overwriting.
     - Writes two summary files:
         - Files/monthly_summary.csv            (total per month)
         - Files/monthly_summary_categories.csv (total per month per category)
       Only categories listed in CATEGORIES_TO_INCLUDE are summarized, and
       amounts in SHARED_CATEGORIES (e.g. shared utility bills) are halved.

4. Categorizing transactions:
     Blank "Kategorie" cells are filled automatically when the same
     Transaktionspartner was labeled at least AUTO_CATEGORIZE_MIN_MATCHES
     times, always with the same category (the run prints each fill).
     Open "Files/ledger.csv" and fill in the "Kategorie" column for any rows
     that are still blank (the script tells you how many are missing after
     each run). Re-run the script afterwards to refresh the summaries with the
     newly categorized rows.

Folder layout (auto-created under "Files/"):
     incoming/  - drop new bank/card/PayPal exports here
     archive/   - optional: old merged_*.csv files used to seed a fresh ledger
     backups/   - automatic timestamped backups of ledger.csv
     ledger.csv - the running, deduplicated transaction ledger
"""

import pandas as pd
import chardet
import csv
import re
import shutil
from datetime import datetime
from pathlib import Path
from pandas.errors import EmptyDataError
from typing import Iterable

FILES_DIR = Path(__file__).resolve().parent / "Files"
INCOMING_DIR = FILES_DIR / "incoming"
ARCHIVE_DIR = FILES_DIR / "archive"
LEDGER_PATH = FILES_DIR / "ledger.csv"
BACKUPS_DIR = FILES_DIR / "backups"

DATE_REGEX = r"(\d{2})_(\d{2})_(\d{4})"
DATE_FORMAT = "%d_%m_%Y"

CATEGORIES_TO_INCLUDE = ["Lebensmittel", "Internet", "Strom", "Wasser", "Bildung", "Transport", "Hotel"]
SHARED_CATEGORIES = []

# A partner's category is reused only after it was labeled this many times, always the same way
AUTO_CATEGORIZE_MIN_MATCHES = 3


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

def auto_categorize(ledger_path, min_matches=AUTO_CATEGORIZE_MIN_MATCHES):
    """
    Fills blank 'Kategorie' cells from the same Transaktionspartner's earlier labels.
    Partners with fewer than min_matches labels, or with conflicting labels, stay blank.
    """
    df = pd.read_csv(ledger_path, sep=";", encoding="ISO-8859-1", dtype=str, keep_default_na=False)
    partner = df["Transaktionspartner"].str.strip()
    category = df["Kategorie"].str.strip()

    # Empty partners ("" or "nan" from clean_dataframe) would lump unrelated rows together
    has_partner = ~partner.isin(["", "nan"])
    labeled = has_partner & category.ne("")
    stats = category[labeled].groupby(partner[labeled]).agg(["nunique", "size", "first"])
    rules = stats.loc[(stats["nunique"] == 1) & (stats["size"] >= min_matches), "first"]

    to_fill = has_partner & category.eq("") & partner.isin(rules.index)
    if not to_fill.any():
        print("Auto-categorize: no blank rows matched a known partner.")
        return 0

    df.loc[to_fill, "Kategorie"] = partner[to_fill].map(rules)
    df.to_csv(ledger_path, sep=";", index=False, encoding="ISO-8859-1")

    filled = df[to_fill].groupby(["Transaktionspartner", "Kategorie"]).size()
    print(f"Auto-categorized {to_fill.sum()} rows from partners labeled at least {min_matches}x:")
    for (name, cat), count in filled.items():
        print(f"  {count:>3}x  {name} -> {cat}")
    return int(to_fill.sum())

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

    # Adjust values for shared categories
    df_filtered["Betrag_adjusted"] = df_filtered.apply(
        lambda row: row["Buchungsbetrag"] / 2 if row["Kategorie"] in SHARED_CATEGORIES else row["Buchungsbetrag"],
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

    # Adjust shared costs
    df_filtered["Buchungsbetrag_adjusted"] = df_filtered.apply(
        lambda row: row["Buchungsbetrag"] / 2 if row["Kategorie"] in SHARED_CATEGORIES else row["Buchungsbetrag"],
        axis=1
    )

    # Group by month and sum
    monthly_summary = df_filtered.groupby("Jahr-Monat")["Buchungsbetrag_adjusted"].sum().reset_index()
    monthly_summary = monthly_summary.sort_values(by="Jahr-Monat", ascending=False)

    # Save and print
    monthly_summary.to_csv(output_monthly_summary_file, sep=";", index=False, encoding="ISO-8859-1")
    print(f"✅ Filtered monthly summary with shared costs saved to {output_monthly_summary_file}")
    print("\n🔹 Filtered Monthly Summary:\n", monthly_summary)





def latest_file_by_date(directory: Path, glob_pattern: str, exclude: Path | None = None) -> Path | None:
    """Return the file matching glob_pattern whose filename encodes the newest DD_MM_YYYY date."""
    candidates = []
    for path in directory.glob(glob_pattern):
        if exclude is not None and path.resolve() == exclude.resolve():
            continue
        match = re.search(DATE_REGEX, path.name)
        if not match:
            continue
        try:
            date = datetime.strptime(match.group(0), DATE_FORMAT)
        except ValueError:
            continue
        candidates.append((date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


def find_paypal_file(directory: Path) -> Path | None:
    """PayPal exports aren't date-stamped; use Download.CSV, or the newest Download*.csv if several exist."""
    exact = directory / "Download.CSV"
    if exact.exists():
        return exact
    candidates = list(directory.glob("Download*.csv")) + list(directory.glob("Download*.CSV"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def merge(bank_account_path, credit_card_path, output_file_path, paypal_path=None):
    merge_financing_files(bank_account_path, credit_card_path, output_file_path, paypal_path=paypal_path)


def run():
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    bank_account_path = latest_file_by_date(INCOMING_DIR, "Konto_Stand_*.CSV")
    credit_card_path = latest_file_by_date(INCOMING_DIR, "Karte_Stand_*.CSV")
    paypal_path = find_paypal_file(INCOMING_DIR)

    if bank_account_path is None or credit_card_path is None:
        print(f"Could not find both a Konto_Stand_*.CSV and Karte_Stand_*.CSV export in {INCOMING_DIR}. Nothing to do.")
        return

    print(f"Using bank export: {bank_account_path.name}")
    print(f"Using credit card export: {credit_card_path.name}")
    print(f"Using PayPal export: {paypal_path.name if paypal_path else '(none found)'}")

    ledger_exists = LEDGER_PATH.exists()
    if ledger_exists:
        BACKUPS_DIR.mkdir(exist_ok=True)
        backup_path = BACKUPS_DIR / f"ledger_{datetime.now().strftime('%d_%m_%Y_%H%M')}.csv"
        shutil.copy2(LEDGER_PATH, backup_path)
        print(f"Backed up existing ledger to {backup_path}")
    else:
        seed_path = latest_file_by_date(ARCHIVE_DIR, "merged_*.csv")
        if seed_path is not None:
            shutil.copy2(seed_path, LEDGER_PATH)
            print(f"Seeded ledger from {seed_path.name}")

    freshly_merged_path = FILES_DIR / "_new_export_merge.csv"
    merge(bank_account_path, credit_card_path, freshly_merged_path, paypal_path=paypal_path)

    if LEDGER_PATH.exists():
        merge_latest_files(freshly_merged_path, LEDGER_PATH, LEDGER_PATH)
    else:
        merged_df = clean_dataframe(pd.read_csv(freshly_merged_path, sep=","))
        merged_df.to_csv(LEDGER_PATH, sep=";", index=False, encoding="ISO-8859-1")
        print(f"Created new ledger at {LEDGER_PATH}")
    freshly_merged_path.unlink(missing_ok=True)

    auto_categorize(LEDGER_PATH)

    ledger_df = pd.read_csv(LEDGER_PATH, sep=";", encoding="ISO-8859-1")
    uncategorized = ledger_df["Kategorie"].isna().sum()

    summary_path = FILES_DIR / "monthly_summary.csv"
    summary_by_category_path = FILES_DIR / "monthly_summary_categories.csv"
    summarize_transactions_by_month_filtered(LEDGER_PATH, summary_path, CATEGORIES_TO_INCLUDE)
    summarize_by_month_and_category(LEDGER_PATH, summary_by_category_path, CATEGORIES_TO_INCLUDE)

    print(f"\n{len(ledger_df)} total transactions in ledger, {uncategorized} without a Kategorie.")
    if uncategorized:
        print(f"Fill in 'Kategorie' for the missing rows in {LEDGER_PATH}, then re-run to refresh the summaries.")
    print(f"Summaries written to {summary_path.name} and {summary_by_category_path.name}")


if __name__ == "__main__":
    run()


