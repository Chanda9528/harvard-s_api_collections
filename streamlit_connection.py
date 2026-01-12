%%writefile streamlit_connection.py

import streamlit as st
import requests
import pymysql
import time

API_KEY = "76fc507d-66dd-44b8-b5eb-c350f65fa1c7"
BASE_URL = "https://api.harvardartmuseums.org/object"
RECORD_LIMIT = 500  # demo value for live evaluation

def get_connection():
    return pymysql.connect(
        host="localhost",
        user="root",
        password="",
        database="harvard"
    )

# API Fetch 
def fetch_artifacts(classification):
    records = []
    page = 1
    while len(records) < RECORD_LIMIT:
        params = {
            "apikey": API_KEY,
            "classification": classification,
            "size": 100,
            "page": page
        }
        response = requests.get(BASE_URL, params=params)
        if response.status_code != 200:
            break
        data = response.json()
        if "records" not in data:
            break
        records.extend(data["records"])
        page += 1
        time.sleep(0.1)
    return records[:RECORD_LIMIT]

#  ETL / Transform 
def transform_metadata(records):
    metadata = []
    for r in records:
        metadata.append({
            "id": r.get("id"),
            "title": r.get("title") or "N/A",
            "culture": r.get("culture") or "N/A",
            "period": r.get("period") or "N/A",
            "century": r.get("century") or "N/A",
            "department": r.get("department") or "N/A",
            "classification": r.get("classification") or "N/A"
        })
    return metadata

#  SQL Insert 
def insert_metadata(metadata):
    conn = get_connection()
    cur = conn.cursor()
    for record in metadata:
        cur.execute("""
            INSERT IGNORE INTO artifact_metadata
            (id, title, culture, period, century, department, classification)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (
            record["id"], record["title"], record["culture"],
            record["period"], record["century"], record["department"],
            record["classification"]
        ))
    conn.commit()
    cur.close()
    conn.close()

#-----  STREAMLIT UI ------
st.title("Harvard’s Artifacts Collection Dashboard")
st.write("Live API data extraction and SQL analytics.")

classification = st.selectbox(
    "Select Classification",
    ["Paintings", "Sculpture", "Coins", "Drawings", "Jewelry"]
)

col1, col2, col3 = st.columns(3)

# Collect Data Button
with col1:
    if st.button("Collect Data"):
        with st.spinner("Fetching live data..."):
            records = fetch_artifacts(classification)
            st.session_state["records"] = records
            st.success(f"{len(records)} records fetched.")

# Show Data Button
with col2:
    if st.button("Show Data"):
        if "records" in st.session_state:
            metadata = transform_metadata(st.session_state["records"])
            # Show first 20 rows only
            st.table(metadata[:20])
        else:
            st.warning("Collect data first.")

# Insert into SQL Button
with col3:
    if st.button("Insert into SQL"):
        if "records" in st.session_state:
            metadata = transform_metadata(st.session_state["records"])
            insert_metadata(metadata)
            st.success("Data inserted into SQL table.")
        else:
            st.warning("No data to insert.")

# ----SQL Queries -------
st.subheader("SQL Queries")
query_options = {
    "Artifacts from 11th century (Byzantine)": 
        "SELECT title, culture FROM artifact_metadata WHERE century='11th century' AND culture='Byzantine'",
    "Unique cultures": 
        "SELECT DISTINCT culture FROM artifact_metadata",
    "Artifacts count per department": 
        "SELECT department, COUNT(*) FROM artifact_metadata GROUP BY department"
}

selected_query = st.selectbox("Select a query to run:", list(query_options.keys()))

if st.button("Run Query"):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query_options[selected_query])
    result = cur.fetchall()
    cur.close()
    conn.close()
    # Show results as list of dicts for table
    columns = [desc[0] for desc in cur.description] if cur.description else []
    output = [dict(zip(columns, row)) for row in result]
    st.table(output)
