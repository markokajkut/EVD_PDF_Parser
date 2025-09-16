import streamlit as st
import os
import time
import warnings
from pathlib import Path
from io import BytesIO
from typing import List, Dict
from pdf_parser import *

warnings.filterwarnings(
    "ignore",
    message=r".*ARC4 has been moved.*"
)

# ---- Streamlit UI ----
st.set_page_config(page_title="PDF Table Extractor", layout="wide")

# Authentication (simple demo version)
USERNAME = st.secrets['authentication']['username']
PASSWORD = st.secrets['authentication']['password']

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if "df_to_show" not in st.session_state:
    st.session_state.df_to_show = None

if not st.session_state.authenticated:
    st.sidebar.subheader("Login")
    username = st.sidebar.text_input("Username")
    password = st.sidebar.text_input("Password", type="password")
    login = st.sidebar.button("Login", type="primary")
    info_placeholder = st.empty()
    if login:
        if username == USERNAME and password == PASSWORD:
            st.session_state.authenticated = True
            info_placeholder.success("Login successful ✅")
            #time.sleep(3)
            #info_placeholder.empty()
            st.rerun()
        else:
            st.sidebar.error("Invalid username or password ❌")
            st.stop()

else:
    clean_df = None
    # ---- Main App ----
    st.sidebar.header("Upload PDF")
    with st.sidebar.form("Upload PDF"):
        uploaded_pdf = st.file_uploader("Choose a PDF", type=["pdf"])
        form_submit_button = st.form_submit_button("Submit", type="primary")
        if form_submit_button:
            
            # Parsing part
            # Parse PDF with Camelot
            read_and_store_to_csv(uploaded_pdf, 'parsed_pdf.csv')
            # Modify CSV for better parsing
            modify_csv('parsed_pdf.csv', 'parsed_pdf_modified.csv')
            # Structure to dictionary
            raw_text = Path('parsed_pdf_modified.csv').read_text(encoding="utf-8")
            articles = parse_articles(raw_text)
            # Clean of unmapped values
            articles = [{k: v for k, v in d.items() if k != "_UNMAPPED_VALUES"} for d in articles]
            # Create dataframe from the data
            clean_df = load_and_flatten(articles)
            # Process dataframe
            clean_df = process_dataframe(clean_df)
            st.session_state.df_to_show = clean_df

            files_to_delete = ['parsed_pdf.csv', 'parsed_pdf_modified.csv']
            for file in files_to_delete:
                if os.path.exists(file):  # check if file exists
                    os.remove(file)
                    print(f"File {file} deleted successfully")
                else:
                    print(f"File {file} not found")

            # read_and_store_to_csv(uploaded_pdf, 'combined_table_test.csv')
            # positions_dict = parse_csv_to_dict('combined_table_test.csv')
            # clean_df = load_and_flatten(positions_dict)

    st.subheader("Extracted DataFrame")
    if st.session_state.df_to_show is not None:
        st.dataframe(st.session_state.df_to_show, width='stretch')
        # Excel download
        excel_bytes = dataframe_to_excel_bytes(st.session_state.df_to_show)
        with st.columns(8)[7]:
            st.download_button(
                label="📥 Download Excel",
                data=excel_bytes,
                file_name="extracted.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary"
            )
    else: 
        st.write("No data loaded")

    logout = st.sidebar.button("Logout", type="primary")
    if logout:
        st.session_state.authenticated = False
        st.rerun()

