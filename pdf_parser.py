import pandas as pd
import camelot
import json
import re
from io import BytesIO
from typing import List, Dict, Tuple


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

def prefix_mengeneinheit(input_csv: str = 'combined_table.csv', output_csv: str = 'combined_table_modified.csv'):
    """
    Reads a CSV file, and if a line starts with 'Mengeneinheit',
    it prefixes it with '17w ' and writes the result to a new CSV file.

    :param input_csv: Path to the input CSV file
    :param output_csv: Path to the output CSV file
    """
    with open(input_csv, "r", encoding="utf-8") as infile, open(output_csv, "w", encoding="utf-8") as outfile:
        for line in infile:
            # Strip only trailing newline for checking
            stripped = line.lstrip()
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
                # Combine section and key to make unique column name
                #col_name = f"{section_name}_{key}"
                flat_record[key] = value
        flat_records.append(flat_record)

    df = pd.DataFrame(flat_records)
    return df


def dataframe_to_excel_bytes(df: pd.DataFrame) -> bytes:
    """
    Convert a pandas DataFrame into an Excel file (bytes object).
    """
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    return output.getvalue()



# KEY_RE = re.compile(r'^(17(?:\.\d+)?[A-Za-z])(?:\s+(.*))?$')  # group1=code (e.g. 17e or 17.1a), group2=label if present
# HEADER_RE = re.compile(r'(?m)^"?\s*17 POSITIONSDATEN', flags=0)


# def normalize_line(s: str) -> str:
#     return s.strip().strip('"').strip()


# def split_into_segments(raw_text: str) -> List[str]:
#     """Return list of text segments each starting with '17 POSITIONSDATEN'."""
#     matches = list(re.finditer(r'(?m)^"?\s*17 POSITIONSDATEN', raw_text))
#     if not matches:
#         return []
#     segments = []
#     for idx, m in enumerate(matches):
#         start = m.start()
#         end = matches[idx + 1].start() if idx + 1 < len(matches) else len(raw_text)
#         segments.append(raw_text[start:end])
#     return segments


# def parse_segment(segment: str) -> Dict:
#     """
#     Parse a single '17 POSITIONSDATEN' segment and return structured dict:
#     {
#       "POSITIONSDATEN e-VD/v-e-VD": { ... },
#       "PACKSTÜCKE": { ... }  # optional
#     }
#     """
#     lines = [normalize_line(l) for l in segment.replace('\r', '\n').splitlines() if normalize_line(l) != '']

#     # drop initial header line if present
#     if lines and lines[0].upper().startswith("17 POSITIONSDATEN"):
#         lines = lines[1:]

#     mapping: Dict[str, str] = {}
#     pack_mapping: Dict[str, str] = {}
#     pending_values: List[str] = []

#     i = 0
#     while i < len(lines):
#         # collect consecutive keys starting at i
#         key_group: List[Tuple[str, str]] = []
#         while i < len(lines):
#             m = KEY_RE.match(lines[i])
#             if not m:
#                 break
#             code = m.group(1)                 # e.g. "17e" or "17.1a"
#             label = m.group(2) if m.group(2) and m.group(2).strip() else code  # fallback to code if label missing
#             key_group.append((code, label))
#             i += 1

#         # if no keys here, then treat consecutive non-key lines as pending values and continue
#         if not key_group:
#             # collect lines until next key/header and push them into pending_values
#             while i < len(lines) and not KEY_RE.match(lines[i]) and not lines[i].upper().startswith("17 POSITIONSDATEN"):
#                 pending_values.append(lines[i])
#                 i += 1
#             continue

#         # collect values up to the next key/header (but do not assume there are exactly len(key_group) values)
#         val_group: List[str] = []
#         while i < len(lines) and not KEY_RE.match(lines[i]) and not lines[i].upper().startswith("17 POSITIONSDATEN"):
#             val_group.append(lines[i])
#             i += 1

#         # available values are pending_values (leftovers) + newly read val_group
#         values_available = pending_values + val_group

#         # map first len(key_group) values to keys, pad with "" if needed
#         for idx, (code, label) in enumerate(key_group):
#             if idx < len(values_available):
#                 val = values_available[idx]
#             else:
#                 val = ""
#             # decide whether it is packstück (17.1...) or regular (17...)
#             if code.startswith("17.1") and "17.1 PACKSTÜCKE" not in code:
#                 pack_mapping[label] = val
#             else:
#                 mapping[label] = val

#         # leftover values (beyond what we consumed) become new pending_values
#         pending_values = values_available[len(key_group):]

#     # After finishing the segment, if there are pending_values left, put them into an __unmapped_values key for debugging
#     article_obj = {"POSITIONSDATEN e-VD/v-e-VD": mapping}
#     if pack_mapping:
#         article_obj["PACKSTÜCKE"] = pack_mapping
#     if pending_values:
#         # include extras so you can inspect them (optional)
#         article_obj["_UNMAPPED_VALUES"] = pending_values

#     return article_obj


# def parse_articles(raw_text: str) -> List[Dict]:
#     segments = split_into_segments(raw_text)
#     if not segments:
#         raise ValueError("No '17 POSITIONSDATEN' blocks found in input.")
#     articles = [parse_segment(seg) for seg in segments]
#     return articles


















# def parse_articles(raw_text: str):
#     """
#     Parse Camelot-extracted raw CSV text into structured articles.
#     """

#     def normalize_line(s):
#         return s.strip().strip('"').strip()

#     def is_key_line(line):
#         # Keys start with 17 + letter OR 17.<digit><letter>
#         return bool(re.match(r"^17(\.\d+)?[a-z]?\b", line))

#     def parse_key(line):
#         # Strip the numeric prefix like '17e ' → 'Bruttomasse'
#         parts = line.split(maxsplit=1)
#         if len(parts) == 2:
#             return parts[1].strip()
#         return parts[0].strip()

#     # start parsing from the first 17 POSITIONSDATEN
#     first_idx = raw_text.find("17 POSITIONSDATEN")
#     if first_idx == -1:
#         raise ValueError("No '17 POSITIONSDATEN' found in text")
#     raw_text = raw_text[first_idx:]

#     # split into article segments
#     segments = re.split(r'(?=\n?"?17 POSITIONSDATEN)', raw_text)
#     segments = [seg for seg in segments if "17 POSITIONSDATEN" in seg]

#     articles = []
#     for seg in segments:
#         seg = seg.replace("\r", "\n").strip().strip('"')
#         lines = [normalize_line(l) for l in seg.splitlines() if normalize_line(l)]

#         # drop the leading "17 POSITIONSDATEN ..." header
#         if lines and lines[0].startswith("17 POSITIONSDATEN"):
#             lines = lines[1:]

#         mapping = {}
#         pack_mapping = {}
#         in_pack = False

#         i = 0
#         while i < len(lines):
#             # collect consecutive keys
#             key_group = []
#             while i < len(lines) and is_key_line(lines[i]):
#                 raw_key = lines[i]
#                 key = parse_key(raw_key)
#                 if raw_key.startswith("17.1"):
#                     in_pack = True
#                 key_group.append(key)
#                 i += 1

#             # collect same number of values (or fewer)
#             val_group = []
#             while i < len(lines) and not is_key_line(lines[i]):
#                 # stop if we encounter next header
#                 if lines[i].startswith("17 POSITIONSDATEN"):
#                     break
#                 val_group.append(lines[i])
#                 i += 1

#             # pad values if fewer than keys
#             while len(val_group) < len(key_group):
#                 val_group.append("")

#             # map keys→values
#             for k, v in zip(key_group, val_group):
#                 if in_pack:
#                     pack_mapping[k] = v
#                 else:
#                     mapping[k] = v

#         article_obj = {"POSITIONSDATEN e-VD/v-e-VD": mapping}
#         if pack_mapping:
#             article_obj["PACKSTÜCKE"] = pack_mapping
#         articles.append(article_obj)

#     return articles







# def parse_articles(raw_text: str):
#     """
#     Parse Camelot-extracted raw CSV text into structured articles starting
#     from the first '17 POSITIONSDATEN' section.
#     """
#     def normalize_line(s):
#         return s.strip().strip('"').strip()

#     def is_label_line(line):
#         # treat lines starting with 17 / 17.1 or "Mengeneinheit" as labels
#         if line.startswith("17") or line.startswith("17.1") or line == "Mengeneinheit":
#             return True
#         return False

#     def parse_label(line):
#         # Strip numbering and keep only field name
#         if line.startswith("17"):
#             parts = line.split(None, 1)
#             return parts[1].strip() if len(parts) > 1 else parts[0].strip()
#         return line

#     # start parsing from the first 17 POSITIONSDATEN
#     first_idx = raw_text.find("17 POSITIONSDATEN")
#     if first_idx == -1:
#         raise ValueError("No '17 POSITIONSDATEN' found in text")
#     raw_text = raw_text[first_idx:]

#     # split into segments (articles)
#     segments = re.split(r'(?=\n?"?17 POSITIONSDATEN)', raw_text)
#     segments = [seg for seg in segments if "17 POSITIONSDATEN" in seg]

#     articles = []
#     for seg in segments:
#         seg = seg.replace("\r", "\n").strip().strip('"')
#         lines = [normalize_line(l) for l in seg.splitlines() if normalize_line(l)]

#         # drop the leading "17 POSITIONSDATEN ..." header
#         if lines and lines[0].startswith("17 POSITIONSDATEN"):
#             lines = lines[1:]

#         mapping = {}
#         pack_mapping = {}
#         current_labels = []
#         pack_mode = False
#         i = 0
#         while i < len(lines):
#             line = lines[i]

#             # entering packstücke mode
#             if line.upper().startswith("17.1") or "PACKSTÜCKE" in line.upper():
#                 pack_mode = True

#             if is_label_line(line):
#                 key = parse_label(line)
#                 # skip PACKSTÜCKE header itself
#                 if key.upper().startswith("PACKSTÜCKE"):
#                     pack_mode = True
#                     i += 1
#                     continue
#                 current_labels.append(key)
#                 i += 1
#                 continue
#             else:
#                 # collect values until next label
#                 values = []
#                 while i < len(lines) and not is_label_line(lines[i]):
#                     values.append(lines[i])
#                     i += 1
#                 # map collected values to collected labels
#                 for idx, lbl in enumerate(current_labels):
#                     val = values[idx] if idx < len(values) else ""
#                     if pack_mode:
#                         pack_mapping[lbl] = val
#                     else:
#                         mapping[lbl] = val
#                 current_labels = []

#         article_obj = {"POSITIONSDATEN e-VD/v-e-VD": mapping}
#         if pack_mapping:
#             article_obj["PACKSTÜCKE"] = pack_mapping
#         articles.append(article_obj)

#     return articles



# def parse_csv_to_dict(csv_file_path: str = 'combined_table.csv', save_as_json: bool = False, json_file_path: str = 'positions.json') -> dict:
#     """
#     Parse raw CSV, and output to a structured dictionary/JSON
#     """
#     df = pd.read_csv(csv_file_path, header=None)

#     # Helpers
#     KEY_LABELS_POS = {
#         "17a": "Positionsnummer",
#         "17b": "Verbrauchsteuer-Produktcode",
#         "17c": "KN-Code",
#         "17d": "Menge",
#         "Mengeneinheit": "Mengeneinheit",
#         "17e": "Bruttomasse",
#         "17f": "Nettomasse",
#         "17g": "Alkoholgehalt",
#         "17h": "Grad Plato",
#         "17o": "Dichte",
#         "17p": "Warenbeschreibung",
#         "17q": "17q",
#     }
#     KEY_ORDER_POS = ["17a","17b","17c","17d","Mengeneinheit","17e","17f","17g","17h","17o","17p","17q"]

#     KEY_LABELS_PACK = {
#         "17.1a": "Art der Packstücke",
#         "17.1b": "Anzahl der Packstücke",
#         "17.1c": "Kennzeichen des Verschlusses",
#     }
#     KEY_ORDER_PACK = ["17.1a","17.1b","17.1c"]

#     def is_pos_key_line(line: str) -> Tuple[bool, str]:
#         """Return (is_key, key_code_or_token) for POSITIONSDATEN."""
#         line = line.strip()
#         if line == "Mengeneinheit":
#             return True, "Mengeneinheit"
#         m = re.match(r"^(17[a-z0-9]{0,2})(?:\s+.*)?$", line)
#         if m and m.group(1) in KEY_LABELS_POS:
#             return True, m.group(1)
#         return False, ""

#     def is_pack_key_line(line: str) -> Tuple[bool, str]:
#         """Return (is_key, key_code) for PACKSTÜCKE."""
#         line = line.strip()
#         m = re.match(r"^(17\.1[a-z])(?:\s+.*)?$", line)
#         if m and m.group(1) in KEY_LABELS_PACK:
#             return True, m.group(1)
#         return False, ""

#     def parse_block_generic(text: str, key_labels: Dict[str,str], key_order: List[str], 
#                             is_key_line_func) -> Dict[str, str]:
#         """Generic parser: keys first, then values in the same count; may repeat in groups."""
#         lines = [l.strip() for l in text.split("\n") if l.strip()]
#         # Drop the header/title line (first line is the section title)
#         if lines and ("POSITIONSDATEN" in lines[0] or "PACKSTÜCKE" in lines[0]):
#             lines = lines[1:]
        
#         result = { key_labels[k]: "" for k in key_order }  # defaults
#         pending_keys: List[str] = []
#         last_filled_label = None
#         mode = "keys"  # or "values"
        
#         for ln in lines:
#             is_key, key_code = is_key_line_func(ln)
#             if is_key:
#                 # If we were still expecting values for an earlier key group, switch back to keys (new group).
#                 pending_keys.append(key_code)
#                 mode = "keys"
#             else:
#                 # Value line
#                 if pending_keys:
#                     # If we were in keys mode, now we switch to values
#                     if mode != "values":
#                         mode = "values"
#                     # Assign values to pending keys sequentially
#                     key_code_to_fill = pending_keys.pop(0)
#                     label = key_labels[key_code_to_fill]
#                     # If the field already has content, append (multi-line value)
#                     if result[label]:
#                         result[label] += " " + ln
#                     else:
#                         result[label] = ln
#                     last_filled_label = label
#                 else:
#                     # Continuation of the last filled field (multi-line)
#                     if last_filled_label is not None:
#                         result[last_filled_label] += " " + ln
#                     else:
#                         # No known key yet; ignore stray value lines
#                         pass
        
#         return result

#     def parse_positions_and_packs(df: pd.DataFrame):
#         blocks = df[1].tolist()
#         parsed = []
#         i = 0
#         while i < len(blocks):
#             cell = blocks[i]
#             if isinstance(cell, str) and cell.strip().startswith("17 POSITIONSDATEN"):
#                 pos = parse_block_generic(cell, KEY_LABELS_POS, KEY_ORDER_POS, is_pos_key_line)
#                 pack = {}
#                 # Look ahead to the next block for PACKSTÜCKE
#                 if i + 1 < len(blocks):
#                     next_cell = blocks[i+1]
#                     if isinstance(next_cell, str) and next_cell.strip().startswith("17.1 PACKSTÜCKE"):
#                         pack = parse_block_generic(next_cell, KEY_LABELS_PACK, KEY_ORDER_PACK, is_pack_key_line)
#                         i += 1  # consume the pack block
#                 parsed.append({
#                     "POSITIONSDATEN e-VD/v-e-VD": pos,
#                     "PACKSTÜCKE": pack if pack else {v: "" for v in KEY_LABELS_PACK.values()}
#                 })
#             i += 1
#         return parsed
    
#     records = parse_positions_and_packs(df)

#     if save_as_json:
#         # Save to JSON
#         with open(json_file_path, "w", encoding="utf-8") as f:
#             json.dump(records, f, ensure_ascii=False, indent=2)

#     return records



# def parse_csv_to_dict(csv_file_path: str = 'combined_table.csv',
#                       save_as_json: bool = False,
#                       json_file_path: str = 'positions.json') -> List[Dict]:
#     """
#     Parse raw CSV exported from Camelot into a list of structured dicts.
#     Fixes an ordering bug where later key-lines appearing while values were still
#     pending could cause values to be assigned to the wrong earlier key.
#     """
#     df = pd.read_csv(csv_file_path, header=None)

#     # Helpers (your existing labels)
#     KEY_LABELS_POS = {
#         "17a": "Positionsnummer",
#         "17b": "Verbrauchsteuer-Produktcode",
#         "17c": "KN-Code",
#         "17d": "Menge",
#         "Mengeneinheit": "Mengeneinheit",
#         "17e": "Bruttomasse",
#         "17f": "Nettomasse",
#         "17g": "Alkoholgehalt",
#         "17h": "Grad Plato",
#         "17o": "Dichte",
#         "17p": "Warenbeschreibung",
#         "17q": "17q",
#     }
#     KEY_ORDER_POS = ["17a","17b","17c","17d","Mengeneinheit","17e","17f","17g","17h","17o","17p","17q"]

#     KEY_LABELS_PACK = {
#         "17.1a": "Art der Packstücke",
#         "17.1b": "Anzahl der Packstücke",
#         "17.1c": "Kennzeichen des Verschlusses",
#     }
#     KEY_ORDER_PACK = ["17.1a","17.1b","17.1c"]

#     def is_pos_key_line(line: str) -> Tuple[bool, str]:
#         line = line.strip()
#         if line == "Mengeneinheit":
#             return True, "Mengeneinheit"
#         m = re.match(r"^(17(\.1)?[a-z0-9]{0,2})(?:\s+.*)?$", line)
#         if m and m.group(1) in KEY_LABELS_POS:
#             return True, m.group(1)
#         return False, ""

#     def is_pack_key_line(line: str) -> Tuple[bool, str]:
#         line = line.strip()
#         m = re.match(r"^(17\.1[a-z0-9]*)(?:\s+.*)?$", line)
#         if m and m.group(1) in KEY_LABELS_PACK:
#             return True, m.group(1)
#         return False, ""

#     def parse_block_generic(text: str, key_labels: Dict[str,str], key_order: List[str],
#                             is_key_line_func) -> Dict[str, str]:
#         """
#         Generic parser that:
#           - collects key tokens first (e.g. 17a, 17b, ...)
#           - then assigns subsequent value lines to them in order
#         Fix: if we encounter a new key while still in 'values' mode and some keys are
#         still pending, we flush those pending keys (leave empty) before accepting the new keys.
#         """
#         lines = [l.strip() for l in text.split("\n") if l.strip()]
#         # Drop the header/title line (first line is the section title)
#         if lines and ("POSITIONSDATEN" in lines[0] or "PACKSTÜCKE" in lines[0]):
#             lines = lines[1:]

#         # initialize all expected labels as empty strings (keeps column consistency)
#         result = { key_labels[k]: "" for k in key_order }
#         pending_keys: List[str] = []
#         last_filled_label = None
#         mode = "keys"  # or "values"

#         for ln in lines:
#             is_key, key_code = is_key_line_func(ln)
#             if is_key:
#                 # If we are currently in values mode and there are still pending keys,
#                 # that means those earlier keys didn't get a value — flush them as empty.
#                 if mode == "values" and pending_keys:
#                     for pk in pending_keys:
#                         lbl = key_labels.get(pk, pk)
#                         if result.get(lbl, "") == "":
#                             result[lbl] = ""  # explicit empty
#                     pending_keys = []

#                 # now append the new key
#                 pending_keys.append(key_code)
#                 mode = "keys"
#             else:
#                 # Value line
#                 if pending_keys:
#                     # first value encountered for the pending queue: switch to 'values' mode
#                     if mode != "values":
#                         mode = "values"
#                     # assign to the oldest pending key
#                     key_code_to_fill = pending_keys.pop(0)
#                     label = key_labels[key_code_to_fill]
#                     if result[label]:
#                         result[label] += " " + ln
#                     else:
#                         result[label] = ln
#                     last_filled_label = label
#                 else:
#                     # continuation of the last filled field (multi-line)
#                     if last_filled_label is not None:
#                         result[last_filled_label] += " " + ln
#                     else:
#                         # stray value without key; ignore (or you could collect separately)
#                         pass

#         # end loop — any remaining pending keys didn't get values; leave them empty
#         for pk in pending_keys:
#             lbl = key_labels.get(pk, pk)
#             if result.get(lbl, "") == "":
#                 result[lbl] = ""

#         return result

#     def parse_positions_and_packs(df: pd.DataFrame):
#         blocks = df[1].tolist()
#         parsed = []
#         i = 0
#         while i < len(blocks):
#             cell = blocks[i]
#             if isinstance(cell, str) and cell.strip().startswith("17 POSITIONSDATEN"):
#                 pos = parse_block_generic(cell, KEY_LABELS_POS, KEY_ORDER_POS, is_pos_key_line)
#                 pack = {}
#                 # Look ahead to the next block for PACKSTÜCKE
#                 if i + 1 < len(blocks):
#                     next_cell = blocks[i+1]
#                     if isinstance(next_cell, str) and "17.1 PACKSTÜCKE" in next_cell:
#                         pack = parse_block_generic(next_cell, KEY_LABELS_PACK, KEY_ORDER_PACK, is_pack_key_line)
#                         i += 1  # consume the pack block
#                 parsed.append({
#                     "POSITIONSDATEN e-VD/v-e-VD": pos,
#                     "PACKSTÜCKE": pack if pack else {v: "" for v in KEY_LABELS_PACK.values()}
#                 })
#             i += 1
#         return parsed

#     records = parse_positions_and_packs(df)

#     if save_as_json:
#         with open(json_file_path, "w", encoding="utf-8") as f:
#             json.dump(records, f, ensure_ascii=False, indent=2)

#     return records


# def parse_csv_to_dict(csv_file_path: str = 'combined_table.csv', save_as_json: bool = False, json_file_path: str = 'positions.json') -> dict:
#     df = pd.read_csv(csv_file_path, header=None)

#     KEY_LABELS_POS = {
#         "17a": "Positionsnummer",
#         "17b": "Verbrauchsteuer-Produktcode",
#         "17c": "KN-Code",
#         "17d": "Menge",
#         "Mengeneinheit": "Mengeneinheit",
#         "17e": "Bruttomasse",
#         "17f": "Nettomasse",
#         "17g": "Alkoholgehalt",
#         "17h": "Grad Plato",
#         "17o": "Dichte",
#         "17p": "Warenbeschreibung",
#         "17q": "17q",
#     }
#     KEY_ORDER_POS = list(KEY_LABELS_POS.keys())

#     KEY_LABELS_PACK = {
#         "17.1a": "Art der Packstücke",
#         "17.1b": "Anzahl der Packstücke",
#         "17.1c": "Kennzeichen des Verschlusses",
#     }
#     KEY_ORDER_PACK = list(KEY_LABELS_PACK.keys())

#     def is_pos_key_line(line: str) -> Tuple[bool, str]:
#         line = line.strip()
#         if line == "Mengeneinheit":
#             return True, "Mengeneinheit"
#         m = re.match(r"^(17[a-z0-9]{0,2})(?:\s+.*)?$", line)
#         if m and m.group(1) in KEY_LABELS_POS:
#             return True, m.group(1)
#         return False, ""

#     def is_pack_key_line(line: str) -> Tuple[bool, str]:
#         line = line.strip()
#         m = re.match(r"^(17\.1[a-z])(?:\s+.*)?$", line)
#         if m and m.group(1) in KEY_LABELS_PACK:
#             return True, m.group(1)
#         return False, ""

#     def parse_block_generic(text: str, key_labels: Dict[str, str], key_order: List[str], is_key_line_func) -> Dict[str, str]:
#         lines = [l.strip() for l in text.split("\n") if l.strip()]
#         if lines and ("POSITIONSDATEN" in lines[0] or "PACKSTÜCKE" in lines[0]):
#             lines = lines[1:]

#         result = {key_labels[k]: "" for k in key_order}
#         pending_keys: List[str] = []
#         last_filled_label = None

#         for ln in lines:
#             is_key, key_code = is_key_line_func(ln)
#             if is_key:
#                 pending_keys.append(key_code)
#             else:
#                 if pending_keys:
#                     key_code_to_fill = pending_keys.pop(0)
#                     label = key_labels[key_code_to_fill]
#                     if result[label]:
#                         result[label] += " " + ln
#                     else:
#                         result[label] = ln
#                     last_filled_label = label
#                 elif last_filled_label is not None:
#                     result[last_filled_label] += " " + ln

#         return result

#     def merge_incomplete_records(records: List[dict]) -> List[dict]:
#         """Merge POSITIONSDATEN split across multiple blocks"""
#         merged = []
#         buffer = None

#         for rec in records:
#             pos = rec["POSITIONSDATEN e-VD/v-e-VD"]
#             # Check if this record is incomplete (missing Warenbeschreibung & 17q)
#             if buffer:
#                 # Merge buffer with this one
#                 for k, v in pos.items():
#                     if v and not buffer[k]:  # fill only missing
#                         buffer[k] = v
#                 merged.append({"POSITIONSDATEN e-VD/v-e-VD": buffer,
#                                "PACKSTÜCKE": rec["PACKSTÜCKE"]})
#                 buffer = None
#             else:
#                 # If Warenbeschreibung & 17q are empty, it might be a split start
#                 if not pos["Warenbeschreibung"] and not pos["17q"]:
#                     buffer = pos
#                 else:
#                     merged.append(rec)

#         # In case last record was not merged
#         if buffer:
#             merged.append({"POSITIONSDATEN e-VD/v-e-VD": buffer,
#                            "PACKSTÜCKE": {v: "" for v in KEY_LABELS_PACK.values()}})

#         return merged

#     def parse_positions_and_packs(df: pd.DataFrame):
#         blocks = df[1].tolist()
#         parsed = []
#         i = 0
#         while i < len(blocks):
#             cell = blocks[i]
#             if isinstance(cell, str) and cell.strip().startswith("17 POSITIONSDATEN"):
#                 pos = parse_block_generic(cell, KEY_LABELS_POS, KEY_ORDER_POS, is_pos_key_line)
#                 pack = {}
#                 if i + 1 < len(blocks):
#                     next_cell = blocks[i + 1]
#                     if isinstance(next_cell, str) and next_cell.strip().startswith("17.1 PACKSTÜCKE"):
#                         pack = parse_block_generic(next_cell, KEY_LABELS_PACK, KEY_ORDER_PACK, is_pack_key_line)
#                         i += 1
#                 parsed.append({
#                     "POSITIONSDATEN e-VD/v-e-VD": pos,
#                     "PACKSTÜCKE": pack if pack else {v: "" for v in KEY_LABELS_PACK.values()}
#                 })
#             i += 1

#         # Merge broken ones
#         return merge_incomplete_records(parsed)

#     records = parse_positions_and_packs(df)
#     return records