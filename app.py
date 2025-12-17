# FINAL SINGLE-FILE STREAMLIT APP
# Harvard’s Artifacts Collection – ETL + SQL Analytics

import streamlit as st
import requests
import sqlite3
import pandas as pd

# ---------------- CONFIG ----------------
API_KEY = "e5810ef2-779c-4c1d-9d4b-3b8b5f1c3db0"
BASE_URL = "https://api.harvardartmuseums.org/object"
DB_NAME = "harvard_artifacts.db"

# ---------------- DB CONNECTION ----------------
def get_conn():
    return sqlite3.connect(DB_NAME)

# ---------------- TABLE CREATION ----------------
def create_tables():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS artifact_metadata (
        id INTEGER PRIMARY KEY,
        title TEXT,
        culture TEXT,
        period TEXT,
        century TEXT,
        medium TEXT,
        dimensions TEXT,
        description TEXT,
        department TEXT,
        classification TEXT,
        accessionyear INTEGER,
        accessionmethod TEXT
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS artifact_media (
        objectid INTEGER,
        imagecount INTEGER,
        mediacount INTEGER,
        colorcount INTEGER,
        rank INTEGER,
        datebegin INTEGER,
        dateend INTEGER
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS artifact_colors (
        objectid INTEGER,
        color TEXT,
        spectrum TEXT,
        hue TEXT,
        percent REAL,
        css3 TEXT
    )""")

    conn.commit()
    conn.close()

# ---------------- DATA COLLECTION ----------------
def fetch_data(classification, limit=2500):
    records = []
    page = 1

    while len(records) < limit:
        params = {
            "apikey": API_KEY,
            "classification": classification,
            "page": page,
            "size": 100
        }
        r = requests.get(BASE_URL, params=params).json()
        if "records" not in r or len(r["records"]) == 0:
            break
        records.extend(r["records"])
        page += 1

    return records[:limit]

# ---------------- INSERT DATA ----------------
def insert_data(data):
    conn = get_conn()
    cur = conn.cursor()

    for i in data:
        cur.execute("INSERT OR IGNORE INTO artifact_metadata VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", (
            i.get("id"), i.get("title"), i.get("culture"), i.get("period"),
            i.get("century"), i.get("medium"), i.get("dimensions"),
            i.get("description"), i.get("department"), i.get("classification"),
            i.get("accessionyear"), i.get("accessionmethod")
        ))

        cur.execute("INSERT INTO artifact_media VALUES (?,?,?,?,?,?,?)", (
            i.get("id"), i.get("imagecount"), i.get("mediacount"),
            i.get("colorcount"), i.get("rank"),
            i.get("datebegin"), i.get("dateend")
        ))

        for c in i.get("colors", []):
            cur.execute("INSERT INTO artifact_colors VALUES (?,?,?,?,?,?)", (
                i.get("id"), c.get("color"), c.get("spectrum"),
                c.get("hue"), c.get("percent"), c.get("css3")
            ))

    conn.commit()
    conn.close()

# ---------------- STREAMLIT UI ----------------
st.set_page_config(page_title="Harvard Artifacts", layout="wide")
st.title("🏛️ Harvard’s Artifacts Collection")

create_tables()

classification = st.text_input("Select your choice (e.g., Coins, Paintings, Jewelry)")

if st.button("Collect Data"):
    if classification.strip() == "":
        st.error("Enter a valid classification")
    else:
        data = fetch_data(classification)
        st.success(f"Fetched {len(data)} records")
        st.session_state["data"] = data

if "data" in st.session_state and len(st.session_state["data"]) > 0:
    st.subheader("Sample Data")
    st.dataframe(pd.DataFrame(st.session_state["data"]).head())

if st.button("Migrate to SQL"):
    if "data" in st.session_state:
        insert_data(st.session_state["data"])
        st.success("Data inserted into SQL")
    else:
        st.error("No data to insert")

# ---------------- SQL QUERIES (25 TOTAL) ----------------
queries = {
    "1. Artifacts from 11th century Byzantine culture": "SELECT * FROM artifact_metadata WHERE century='11th century' AND culture='Byzantine'",
    "2. Unique cultures": "SELECT DISTINCT culture FROM artifact_metadata",
    "3. Artifacts from Archaic Period": "SELECT * FROM artifact_metadata WHERE period='Archaic Period'",
    "4. Titles ordered by accession year": "SELECT title, accessionyear FROM artifact_metadata ORDER BY accessionyear DESC",
    "5. Artifact count per department": "SELECT department, COUNT(*) FROM artifact_metadata GROUP BY department",

    "6. Artifacts with more than 1 image": "SELECT objectid FROM artifact_media WHERE imagecount > 1",
    "7. Average rank": "SELECT AVG(rank) FROM artifact_media",
    "8. Colorcount > mediacount": "SELECT objectid FROM artifact_media WHERE colorcount > mediacount",
    "9. Artifacts between 1500-1600": "SELECT objectid FROM artifact_media WHERE datebegin >= 1500 AND dateend <= 1600",
    "10. Artifacts with no media": "SELECT COUNT(*) FROM artifact_media WHERE mediacount = 0",

    "11. Distinct hues": "SELECT DISTINCT hue FROM artifact_colors",
    "12. Top 5 colors": "SELECT color, COUNT(*) cnt FROM artifact_colors GROUP BY color ORDER BY cnt DESC LIMIT 5",
    "13. Avg percent per hue": "SELECT hue, AVG(percent) FROM artifact_colors GROUP BY hue",
    "14. Colors for artifact": "SELECT * FROM artifact_colors WHERE objectid = ?",
    "15. Total color entries": "SELECT COUNT(*) FROM artifact_colors",

    "16. Byzantine artifacts with hues": "SELECT m.title, c.hue FROM artifact_metadata m JOIN artifact_colors c ON m.id=c.objectid WHERE m.culture='Byzantine'",
    "17. Titles with hues": "SELECT m.title, c.hue FROM artifact_metadata m JOIN artifact_colors c ON m.id=c.objectid",
    "18. Titles, culture, rank where period not null": "SELECT m.title, m.culture, me.rank FROM artifact_metadata m JOIN artifact_media me ON m.id=me.objectid WHERE m.period IS NOT NULL",
    "19. Top 10 Grey artifacts": "SELECT DISTINCT m.title FROM artifact_metadata m JOIN artifact_colors c ON m.id=c.objectid WHERE c.hue='Grey' LIMIT 10",
    "20. Avg media per classification": "SELECT m.classification, AVG(me.mediacount) FROM artifact_metadata m JOIN artifact_media me ON m.id=me.objectid GROUP BY m.classification",

    "21. Artifacts without culture": "SELECT * FROM artifact_metadata WHERE culture IS NULL",
    "22. Artifacts before year 1000": "SELECT * FROM artifact_media WHERE datebegin < 1000",
    "23. Max color coverage": "SELECT objectid, MAX(percent) FROM artifact_colors GROUP BY objectid",
    "24. Departments with >50 artifacts": "SELECT department FROM artifact_metadata GROUP BY department HAVING COUNT(*) > 50",
    "25. Most common classification": "SELECT classification, COUNT(*) FROM artifact_metadata GROUP BY classification ORDER BY COUNT(*) DESC"
}

st.subheader("SQL Queries")
choice = st.selectbox("Select your choice", list(queries.keys()))

if st.button("Run Query"):
    conn = get_conn()
    if "?" in queries[choice]:
        aid = st.number_input("Enter Artifact ID", step=1)
        df = pd.read_sql_query(queries[choice], conn, params=(aid,))
    else:
        df = pd.read_sql_query(queries[choice], conn)
    conn.close()
    st.dataframe(df)
