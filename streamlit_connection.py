import streamlit as st
import pymysql

# MySQL connection
def get_connection():
    return pymysql.connect(
        host="localhost",
        user="root",
        password="",
        database="harvard"
    )

st.set_page_config(page_title="Harvard Artifacts Dashboard", layout="wide")

st.title("Harvard Artifacts Collection Dashboard")
st.write("Explore, collect, and analyze data from the Harvard Art Museums API.")

# Classification Dropdown
classification = st.selectbox(
    "Select Classification",
    ["Paintings", "Sculpture", "Coins", "Drawings", "Jewelry"]
)

# Buttons Row
col1, col2, col3 = st.columns(3)

with col1:
    if st.button("Collect Data"):
        st.write(f"Collecting data for {classification}... (handled in backend)")

with col2:
    if st.button("Show Data"):
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("SELECT id, title, culture, department, classification FROM artifact_metadata LIMIT 10;")
            data = cur.fetchall()
            if data:
                st.table(data)
            else:
                st.write("No records found.")
            cur.close()
            conn.close()
        except Exception as e:
            st.error(f"Error: {e}")

with col3:
    if st.button("Insert into SQL"):
        st.write(f"Inserting records for {classification}... (handled in backend)")

st.subheader("Query and Visualization Section")

# Query Options
query_options = [
    "Artifacts from 11th century (Byzantine culture)",
    "Unique cultures represented",
    "Artifacts from Archaic Period",
    "Titles ordered by accession year",
    "Artifacts count per department"
]

selected_query = st.selectbox("Select a query to run:", query_options)

if st.button("Run Query"):
    try:
        conn = get_connection()
        cur = conn.cursor()

        if selected_query == "Artifacts from 11th century (Byzantine culture)":
            cur.execute("SELECT title, culture, century FROM artifact_metadata WHERE century='11th century' AND culture='Byzantine';")
        elif selected_query == "Unique cultures represented":
            cur.execute("SELECT DISTINCT culture FROM artifact_metadata;")
        elif selected_query == "Artifacts from Archaic Period":
            cur.execute("SELECT title, period FROM artifact_metadata WHERE period='Archaic';")
        elif selected_query == "Titles ordered by accession year":
            cur.execute("SELECT title, accessionyear FROM artifact_metadata ORDER BY accessionyear DESC;")
        elif selected_query == "Artifacts count per department":
            cur.execute("SELECT department, COUNT(*) FROM artifact_metadata GROUP BY department;")

        result = cur.fetchall()
        if result:
            st.table(result)
        else:
            st.write("No results found.")

        cur.close()
        conn.close()
    except Exception as e:
        st.error(f"Error: {e}")
