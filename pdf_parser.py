import pandas as pd
import camelot
import csv
import re
from pypdf import PdfReader
from io import BytesIO
from typing import List, Dict, Tuple


def check_number_of_pages(pdf_file: str) -> int:
    """
    Count number of pages of PDF file
    """
    reader = PdfReader(pdf_file)
    num_pages = len(reader.pages)
    return num_pages

def read_and_store_to_csv(pdf_file_path: str, csv_file_path: str = 'combined_table.csv') -> None:
    """
    Parse EVD PDF file, save it to raw CSV
    """
    tables = camelot.read_pdf(
        pdf_file_path, 
        pages='all', 
        flavor='lattice', 
        process_background=True
    )

    # Extract the DataFrames
    dfs = [table.df for table in tables]
    # Concatenate all DataFrames into one
    combined_df = pd.concat(dfs, ignore_index=True)
    # Optionally, save to CSV
    combined_df.to_csv(csv_file_path, index=False)
    return len(dfs)

def append_camelot_missing_to_csv(pdf_file: str, missing_page_number: int, raw_csv_file: str = 'combined_table.csv') -> None:
    tables = camelot.read_pdf(
        pdf_file, 
        pages=f'{missing_page_number}', 
        flavor='stream',
    )

    # Extract the DataFrames
    df = tables[0].df

    # Remove header row(s) like "Seite 4 von 4 ..." and reset index
    df = df[~df[0].str.contains("Seite", na=False)].reset_index(drop=True)

    # Append lines to CSV file
    with open(raw_csv_file, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        
        for row in df.itertuples(index=False):
            line = "\n".join([str(x) for x in row if x])
            writer.writerow([line])    

def modify_csv(input_csv: str = 'combined_table.csv', output_csv: str = 'combined_table_modified.csv'):
    """
    Reads a CSV file, modifies it by:
      1. Prefixing lines that start with 'Mengeneinheit' with '17w '
      2. Removing everything starting from '18 DOKUMENT – ZERTIFIKAT' (inclusive)

    :param input_csv: Path to the input CSV file
    :param output_csv: Path to the output CSV file
    """
    with open(input_csv, "r", encoding="utf-8") as infile, open(output_csv, "w", encoding="utf-8") as outfile:
        for line in infile:

            # Remove all double quotes
            line = line.replace('"', '')

            # Strip only trailing newline for checking
            stripped = line.lstrip()

            # If we hit the Dokument section, stop writing completely
            if stripped.startswith('18 DOKUMENT – ZERTIFIKAT'):
                break
            
            # If line starts with Mengeneinheit, prefix it
            if stripped.startswith("Mengeneinheit"):
                outfile.write("17w " + stripped)
            else:
                outfile.write(line)



# Regexes
KEY_RE = re.compile(r'^(17(?:\.\d+)?[A-Za-z])(?:\s+(.*))?$', re.IGNORECASE)
# Matches lines like: "17.1 PACKSTÜCKE" or "17 PACKSTUECKE" (with or without Ü)
PACK_HEADER_RE = re.compile(r'^(17(?:\.\d+)?)\s+PACKST[ÜU]CKE\b', re.IGNORECASE)
SEGMENT_HEADER_RE = re.compile(r'(?im)^\s*"?\s*17 POSITIONSDATEN\b')


def normalize_line(s: str) -> str:
    return s.strip().strip('"').strip()


def split_into_segments(raw_text: str) -> List[str]:
    """Return list of text segments each starting with '17 POSITIONSDATEN'."""
    starts = [m.start() for m in SEGMENT_HEADER_RE.finditer(raw_text)]
    if not starts:
        return []
    segments = []
    for idx, start in enumerate(starts):
        end = starts[idx + 1] if idx + 1 < len(starts) else len(raw_text)
        segments.append(raw_text[start:end])
    return segments


def parse_segment(segment: str) -> Dict:
    """
    Parse a single '17 POSITIONSDATEN' segment and return structured dict:
    {
      "POSITIONSDATEN e-VD/v-e-VD": { ... },
      "PACKSTÜCKE": { ... }  # optional
    }
    """
    lines = [normalize_line(l) for l in segment.replace('\r', '\n').splitlines() if normalize_line(l) != '']

    # drop initial header line if present
    if lines and lines[0].upper().startswith("17 POSITIONSDATEN"):
        lines = lines[1:]

    mapping: Dict[str, str] = {}
    pack_mapping: Dict[str, str] = {}
    pending_values: List[str] = []
    i = 0
    while i < len(lines):
        # If this line is a PACKSTÜCKE header, enable pack mode and skip it
        if PACK_HEADER_RE.match(lines[i]):
            i += 1
            # don't append this header to pending_values — it is a structural marker
            continue

        # collect consecutive keys starting at i (keys are like: 17e Label  or 17.1a Label)
        key_group: List[Tuple[str, str]] = []
        while i < len(lines):
            m = KEY_RE.match(lines[i])
            if not m:
                break
            code = m.group(1)                # e.g. "17e" or "17.1a"
            label = (m.group(2).strip() if m.group(2) and m.group(2).strip() else code)  # fallback to code if label missing
            key_group.append((code, label))
            i += 1

        # If no keys found, collect non-key lines as pending values (but skip PACK header lines)
        if not key_group:
            while i < len(lines) and not KEY_RE.match(lines[i]) and not SEGMENT_HEADER_RE.match(lines[i]) and not PACK_HEADER_RE.match(lines[i]):
                pending_values.append(lines[i])
                i += 1
            continue

        # collect values up to the next key/header/pack header
        val_group: List[str] = []
        while i < len(lines) and not KEY_RE.match(lines[i]) and not SEGMENT_HEADER_RE.match(lines[i]) and not PACK_HEADER_RE.match(lines[i]):
            val_group.append(lines[i])
            i += 1

        # available values = leftover pending + newly read values
        values_available = pending_values + val_group

        # map keys→values in order, pad missing values with ""
        for idx, (code, label) in enumerate(key_group):
            if idx < len(values_available):
                val = values_available[idx]
            else:
                val = ""
            if code.lower().startswith("17.1"):  # pack keys go under PACKSTÜCKE
                pack_mapping[label] = val
            else:
                mapping[label] = val

        # leftover values (if any) remain pending for next key group
        pending_values = values_available[len(key_group):]

    article_obj = {"POSITIONSDATEN e-VD/v-e-VD": mapping}
    if pack_mapping:
        article_obj["PACKSTÜCKE"] = pack_mapping
    # (optional) If you want to surface leftover values for debugging:
    if pending_values:
        article_obj["_UNMAPPED_VALUES"] = pending_values
    return article_obj


def parse_articles(raw_text: str) -> List[Dict]:
    segments = split_into_segments(raw_text)
    if not segments:
        raise ValueError("No '17 POSITIONSDATEN' blocks found in input.")
    return [parse_segment(seg) for seg in segments]


def load_and_flatten(records: List[Dict]) -> pd.DataFrame:
    """
    Load a list of nested dicts and flatten it into a pandas DataFrame.
    Nested keys are combined with '_' to make unique column names.
    """
    flat_records = []

    for record in records:
        flat_record = {}

        for section_dict in record.values():
            for key, value in section_dict.items():
                flat_record[key] = value
        flat_records.append(flat_record)

    df = pd.DataFrame(flat_records)
    return df

def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_fix = ["Menge", "Bruttomasse", "Nettomasse", "Alkoholgehalt", "Positionsnummer", "Anzahl der Packstücke"]
    for col in cols_to_fix:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(".", "", regex=False)   # remove thousand separators
            .str.replace(",", ".", regex=False)  # replace decimal comma
            .astype(float)
        )

    df["Positionsnummer"] = df["Positionsnummer"].astype(int)
    df["Anzahl der Packstücke"] = df["Anzahl der Packstücke"].astype(int)
    df["Alkoholmenge"] = df["Menge"] * (df["Alkoholgehalt"] / 100)
    df["Alkoholmenge"] = df["Alkoholmenge"]

    df = df.rename(columns={"Verbrauchsteuer-Produktcode": "Produktcode"})
    return df


def dataframe_to_excel_bytes(df: pd.DataFrame) -> bytes:
    """
    Convert a pandas DataFrame into an Excel file (bytes object).
    """

    cols_to_compute_total = ["Menge", "Bruttomasse", "Nettomasse", "Anzahl der Packstücke", "Alkoholmenge", "Alkoholgehalt"]

    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        workbook = writer.book
        bold_format = workbook.add_format({"bold": True, "bg_color": "#D9E1F2", "num_format": "###0.000"})
        num_format = workbook.add_format({"num_format": "###0.000"})  # 3 decimals

        def write_with_totals(dataframe: pd.DataFrame, sheet_name: str):
            """Helper to write dataframe + totals row with formatting"""
            df_copy = dataframe.copy()

            # Compute totals
            totals = df_copy[cols_to_compute_total].sum().to_dict()
            totals_row = {col: totals.get(col, "") for col in cols_to_compute_total}

            # Force integer total for Anzahl der Packstücke
            if "Anzahl der Packstücke" in totals_row and totals_row["Anzahl der Packstücke"] != "":
                totals_row["Anzahl der Packstücke"] = int(totals_row["Anzahl der Packstücke"])

            totals_row.update({c: "" for c in df_copy.columns if c not in cols_to_compute_total})
            totals_row["Produktcode"] = "TOTAL"
            totals_row["Alkoholgehalt"] = ""


            # Append totals row
            df_copy = pd.concat([df_copy, pd.DataFrame([totals_row])], ignore_index=True)

            # Write data
            df_copy.to_excel(writer, index=False, sheet_name=sheet_name)

            # Apply formatting
            worksheet = writer.sheets[sheet_name]
            total_row_idx = len(df_copy)  # Excel row index is 1-based because of header

            # Anzahl der Packstücke in TOTAL format
            anzahl_total_format = workbook.add_format({"bold": True, "bg_color": "#D9E1F2", "num_format": "###0"})
            anzahl_col_idx = df_copy.columns.get_loc("Anzahl der Packstücke")  # zero-based index
            # Get the value from df_copy
            anzahl_total_value = df_copy.iloc[-1, anzahl_col_idx]

            # Bold + highlight total row
            worksheet.set_row(total_row_idx, None, bold_format)

            # Format numeric columns
            for col_idx, col_name in enumerate(df_copy.columns):
                if col_name in cols_to_compute_total:
                    if col_name == "Anzahl der Packstücke":
                        # Integer format for this column
                        int_format = workbook.add_format({"num_format": "###0"})
                        worksheet.set_column(col_idx, col_idx, 15, int_format)
                        df_copy[col_name, len(df_copy)] = df_copy[col_name].astype(int)
                    else:
                        worksheet.set_column(col_idx, col_idx, 15, num_format)

            # Overwrite just that one cell with correct format
            worksheet.write(total_row_idx, anzahl_col_idx, anzahl_total_value, anzahl_total_format)

        # First sheet = All records
        write_with_totals(df, "All")

        # Separate sheets per Produktcode
        for produktcode, group in df.groupby("Produktcode"):
            sheet_name = str(produktcode)[:31]  # Excel sheet names max 31 chars
            write_with_totals(group, sheet_name)

    return output.getvalue()