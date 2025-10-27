# Harvard Artifacts Collection Dashboard

## Project Overview
Harvard Artifacts Collection Dashboard is a Python-based data analytics project that connects to the **Harvard Art Museums API**.  
It extracts, processes, and stores artifact data into a MySQL database, and provides an interactive Streamlit dashboard for analysis using SQL queries.

The project demonstrates end-to-end data handling — from API integration to SQL-based visualization.

## Steps to Run

1. **Run `harvard_backend.py`**  
   - This file connects to the Harvard Art Museums API.  
   - Extracts metadata, media, and color information of artifacts.  
   - Cleans and inserts the data into a MySQL database (`harvard`).  

2. **Run `streamlit_connection.py`**  
   - This file opens the interactive Streamlit dashboard at  
     `http://localhost:8501`  
   - You can view data, run SQL queries, and explore insights visually.

## Tools Used
- **Python** (for API integration and backend processing)  
- **PyMySQL** (for connecting Python with MySQL)  
- **MySQL** (for database storage and SQL queries)  
- **Streamlit** (for the interactive dashboard UI)  
- **Harvard Art Museums API** (for artifact data collection)

## Folder Structure
