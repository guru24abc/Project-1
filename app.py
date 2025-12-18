import sqlite3
import requests
import pandas as pd
import streamlit as st

# ------------------ CONFIG ------------------
API_KEY = "e5810ef2-779c-4c1d-9d4b-3b8b5f1c3db0"
BASE_URL = "https://api.harvardartmuseums.org/object"

# ------------------ DB SETUP ------------------
conn = sqlite3.connect("harvard_artifacts.db", check_same_thread=False)
cursor = conn.cursor()

def create_tables():
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS artifacts_metadata (
        id INTEGER PRIMARY KEY,
        title TEXT,
        culture TEXT,
        dated TEXT,
        period TEXT,
        division TEXT,
        medium TEXT,
        dimensions TEXT,
        department TEXT,
        description TEXT,
        classification TEXT,
        accessionyear INTEGER,
        accessionmethod TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS artifacts_media (
        objectid INTEGER,
        imagecount INTEGER,
        mediacount INTEGER,
        colorcount INTEGER,
        rank INTEGER,
        datebegin INTEGER,
        dateend INTEGER
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS artifacts_colors (
        objectid INTEGER,
        color TEXT,
        spectrum TEXT,
        hue TEXT,
        percent REAL,
        css3 TEXT
    )
    """)
    conn.commit()

create_tables()

# ------------------ API FETCH ------------------
def fetch_records(classification):
    all_records = []
    for page in range(1, 26):
        params = {
            "apikey": API_KEY,
            "classification": classification,
            "size": 100,
            "page": page,
            "hasimage": 1
        }
        res = requests.get(BASE_URL, params=params)
        if res.status_code != 200:
            break
        data = res.json().get("records", [])
        if not data:
            break
        all_records.extend(data)
    return all_records

# ------------------ TRANSFORM ------------------
def transform(records):
    meta, media, colors = [], [], []

    for r in records:
        meta.append({
            "id": r["id"],
            "title": r.get("title"),
            "culture": r.get("culture"),
            "dated": r.get("dated"),
            "period": r.get("period"),
            "division": r.get("division"),
            "medium": r.get("medium"),
            "dimensions": r.get("dimensions"),
            "department": r.get("department"),
            "description": r.get("description"),
            "classification": r.get("classification"),
            "accessionyear": r.get("accessionyear"),
            "accessionmethod": r.get("accessionmethod")
        })

        media.append({
            "objectid": r["objectid"],
            "imagecount": r["imagecount"],
            "mediacount": r["mediacount"],
            "colorcount": r["colorcount"],
            "rank": r["rank"],
            "datebegin": r["datebegin"],
            "dateend": r["dateend"]
        })

        for c in r.get("colors", []):
            colors.append({
                "objectid": r["objectid"],
                "color": c.get("color"),
                "spectrum": c.get("spectrum"),
                "hue": c.get("hue"),
                "percent": c.get("percent"),
                "css3": c.get("css3")
            })

    return meta, media, colors

# ------------------ INSERT ------------------
def insert_data(meta, media, colors):
    cursor.executemany("""
        INSERT OR IGNORE INTO artifacts_metadata VALUES
        (:id,:title,:culture,:dated,:period,:division,:medium,
         :dimensions,:department,:description,:classification,
         :accessionyear,:accessionmethod)
    """, meta)

    cursor.executemany("""
        INSERT INTO artifacts_media VALUES
        (:objectid,:imagecount,:mediacount,:colorcount,:rank,:datebegin,:dateend)
    """, media)

    cursor.executemany("""
        INSERT INTO artifacts_colors VALUES
        (:objectid,:color,:spectrum,:hue,:percent,:css3)
    """, colors)

    conn.commit()

# ------------------ UI ------------------
st.set_page_config(layout="wide")
st.markdown("<h1 style='text-align:center;'>🏛️ Harvard’s Artifacts Collection</h1>", unsafe_allow_html=True)

classification = st.text_input("Enter a classification:")

menu = st.radio(
    "",
    ["Select Your Choice", "Migrate to SQL", "SQL Queries"],
    horizontal=True
)

if "records" not in st.session_state:
    st.session_state.records = []

# -------- Collect Data --------
if st.button("Collect data"):
    if classification.strip() == "":
        st.error("Please enter a classification")
    else:
        st.session_state.records = fetch_records(classification)
        meta, media, colors = transform(st.session_state.records)

        c1, c2, c3 = st.columns(3)
        with c1:
            st.subheader("Metadata")
            st.json(meta)
        with c2:
            st.subheader("Media")
            st.json(media)
        with c3:
            st.subheader("Colours")
            st.json(colors)

# -------- Insert --------
if menu == "Migrate to SQL":
    st.subheader("Insert the collected data")
    if st.button("Insert"):
        if not st.session_state.records:
            st.error("No data collected")
        else:
            meta, media, colors = transform(st.session_state.records)
            insert_data(meta, media, colors)
            st.success("Data inserted successfully")

            st.subheader("Inserted Data:")

            st.markdown("### Artifacts Metadata")
            df1 = pd.read_sql("SELECT * FROM artifacts_metadata", conn)
            st.dataframe(df1)

            st.markdown("### Artifacts Media")
            df2 = pd.read_sql("SELECT * FROM artifacts_media", conn)
            st.dataframe(df2)

            st.markdown("### Artifacts Colors")
            df3 = pd.read_sql("SELECT * FROM artifacts_colors", conn)
            st.dataframe(df3)
# ------------------ SQL QUERIES ------------------
if menu == "SQL Queries":

    st.subheader("Run SQL Queries")

    queries = {
        "1. List all artifacts from the 11th century belonging to Byzantine culture": """
            SELECT * FROM artifacts_metadata
            WHERE culture='Byzantine' AND dated LIKE '%11%'
        """,

        "2. What are the unique cultures represented in the artifacts?": """
            SELECT DISTINCT culture FROM artifacts_metadata
        """,

        "3. List all artifacts from the Archaic Period": """
            SELECT * FROM artifacts_metadata
            WHERE period='Archaic Period'
        """,

        "4. List artifact titles ordered by accession year in descending order": """
            SELECT title, accessionyear
            FROM artifacts_metadata
            ORDER BY accessionyear DESC
        """,

        "5. How many artifacts are there per department?": """
            SELECT department, COUNT(*) AS total
            FROM artifacts_metadata
            GROUP BY department
        """,

        "6. Which artifacts have more than 1 image?": """
            SELECT * FROM artifacts_media
            WHERE imagecount > 1
        """,

        "7. What is the average rank of all artifacts?": """
            SELECT AVG(rank) AS avg_rank FROM artifacts_media
        """,

        "8. Which artifacts have a higher colorcount than mediacount?": """
            SELECT * FROM artifacts_media
            WHERE colorcount > mediacount
        """,

        "9. List all artifacts created between 1500 and 1600": """
            SELECT * FROM artifacts_media
            WHERE datebegin >= 1500 AND dateend <= 1600
        """,

        "10. How many artifacts have no media files?": """
            SELECT * FROM artifacts_media
            WHERE mediacount = 0
        """,

        "11. What are all the distinct hues used in the dataset?": """
            SELECT DISTINCT hue FROM artifacts_colors
        """,

        "12. What are the top 5 most used colors by frequency?": """
            SELECT color, COUNT(*) AS freq
            FROM artifacts_colors
            GROUP BY color
            ORDER BY freq DESC
            LIMIT 5
        """,

        "13. What is the average coverage percentage for each hue?": """
            SELECT hue, AVG(percent) AS avg_percent
            FROM artifacts_colors
            GROUP BY hue
        """,

        "14. List all colors used for a given artifact ID": """
            SELECT COUNT(*) AS total_colors FROM artifacts_colors
        """,

        "15. What is the total number of color entries in the dataset?": """
            SELECT m.title, c.hue
            FROM artifacts_metadata m
            JOIN artifacts_colors c ON m.id = c.objectid
        """,

        "16. List artifact titles and hues for all artifacts belonging to the Byzantine culture.": """
            SELECT m.title, c.hue
            FROM artifacts_metadata m
            JOIN artifacts_colors c ON m.id = c.objectid
            WHERE m.culture='Byzantine'
        """,

        "17. Titles with rank where period exists": """
            SELECT m.title, me.rank
            FROM artifacts_metadata m
            JOIN artifacts_media me ON m.id = me.objectid
            WHERE m.period IS NOT NULL
        """,

        "18. Top 10 ranked Grey artifacts": """
            SELECT m.title, me.rank
            FROM artifacts_metadata m
            JOIN artifacts_media me ON m.id = me.objectid
            JOIN artifacts_colors c ON m.id = c.objectid
            WHERE c.hue='Grey'
            ORDER BY me.rank
            LIMIT 10
        """,

        "19. Average media count per classification": """
            SELECT m.classification, AVG(me.mediacount) AS avg_media
            FROM artifacts_metadata m
            JOIN artifacts_media me ON m.id = me.objectid
            GROUP BY m.classification
        """,

        "20. Artifacts with maximum color count": """
            SELECT * FROM artifacts_media
            WHERE colorcount = (SELECT MAX(colorcount) FROM artifacts_media)
        """,

        # -------- Additional 5 Queries --------

        "21. Total artifacts count": """
            SELECT COUNT(*) AS total_artifacts FROM artifacts_metadata
        """,

        "22. Artifacts without culture": """
            SELECT * FROM artifacts_metadata
            WHERE culture IS NULL
        """,

        "23. Artifacts per classification": """
            SELECT classification, COUNT(*) AS total
            FROM artifacts_metadata
            GROUP BY classification
        """,

        "24. Earliest dated artifact": """
            SELECT * FROM artifacts_media
            ORDER BY datebegin ASC
            LIMIT 1
        """,

        "25. Artifacts with more colors than images": """
            SELECT * FROM artifacts_media
            WHERE colorcount > imagecount
        """
    }

    query_name = st.selectbox(
        "Select a query",
        list(queries.keys()),
        index=None,
        placeholder="Choose a query"
    )

    if query_name:
        df = pd.read_sql_query(queries[query_name], conn)
        st.dataframe(df)
