# STEP 1: CONNECT TO HARVARD API AND VERIFY RESPONSE
# %%
import requests

API_KEY = "76fc507d-66dd-44b8-b5eb-c350f65fa1c7"
url = "https://api.harvardartmuseums.org/classification"

params = {
    "apikey": API_KEY,
    "size": 100
}

response = requests.get(url, params)
response   # 200 indicates success

# %%
# STEP 2: CLASSIFICATIONS WHERE OBJECT COUNT >= 2500

# convert response to JSON format
data = response.json()

# check available keys (optional)
# print(data.keys())

# loop through each record and filter
print("Classifications with object count >= 2500:\n")

for record in data.get("records", []):
    if record.get("objectcount", 0) >= 2500:
        print(record.get("name"), "-", record.get("objectcount"))

# %%
# STEP 3: FETCH OBJECT RECORDS FOR A CHOSEN CLASSIFICATION

# choose a classification from the previous output (for example, "Vessels")
classification = "Vessels"

# API endpoint for object data
url = "https://api.harvardartmuseums.org/object"

# page size and starting page
page = 1
page_size = 100

# empty list to store all records
all_records = []

# run until you collect about 2500 records (25 pages × 100)
while len(all_records) < 2500:
    params = {
        "apikey": API_KEY,
        "classification": classification,
        "size": page_size,
        "page": page
    }

    response = requests.get(url, params=params)
    if response.status_code != 200:
        print("Request failed at page", page)
        break

    data = response.json()
    records = data.get("records", [])

    # stop if no more data
    if not records:
        print("No more records found.")
        break

    # add new records to list
    all_records.extend(records)
    print(f"Fetched page {page} | Total records so far: {len(all_records)}")

    page += 1

# final record count
print("\nTotal records fetched:", len(all_records))

# %%
# STEP 4: SPLIT THE DATA INTO 3 PARTS — METADATA, MEDIA, COLORS

metadata = []
media = []
colors = []

for i in all_records:
    # METADATA section
    metadata.append(dict(
        id=i.get("id"),
        title=i.get("title"),
        culture=i.get("culture"),
        period=i.get("period"),
        century=i.get("century"),
        medium=i.get("medium"),
        dimensions=i.get("dimensions"),
        description=i.get("description"),
        department=i.get("department"),
        classification=i.get("classification"),
        accessionyear=i.get("accessionyear"),
        accessionmethod=i.get("accessionmethod")
    ))

    # MEDIA section
    media.append(dict(
        objectid=i.get("id"),
        imagecount=i.get("imagecount"),
        mediacount=i.get("mediacount"),
        colorcount=i.get("colorcount"),
        rank=i.get("rank"),
        datebegin=i.get("datebegin"),
        dateend=i.get("dateend")
    ))

    # COLORS section (list inside each record)
    if i.get("colors"):
        for j in i["colors"]:
            colors.append(dict(
                objectid=i.get("id"),
                color=j.get("color"),
                spectrum=j.get("spectrum"),
                hue=j.get("hue"),
                percent=j.get("percent"),
                css3=j.get("css3")
            ))

print("Metadata records collected:", len(metadata))
print("Media records collected:", len(media))
print("Color records collected:", len(colors))

# %%

import pymysql
my_db=pymysql.connect(
host ="localhost",
    user="root",
    password=""
    )
print("connection sucessfull.")
# %%
my_cursor = my_db.cursor()

# Database creation
my_cursor.execute("CREATE DATABASE IF NOT EXISTS harvard")

print(" Database 'harvard' created or already exists.")

# %%
my_db = pymysql.connect(
    host="localhost",
    user="root",
    password="",
    database="harvard"
)

my_cursor = my_db.cursor()
print("Connected to 'harvard' database.")

# %%
# Create Table: artifact_metadata

my_cursor.execute("""
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
)
""")

print("Table 'artifact_metadata' created successfully.")

# %%
# Create Table: artifact_media 

my_cursor.execute("""
CREATE TABLE IF NOT EXISTS artifact_media (
    objectid INT PRIMARY KEY,
    imagecount INT,
    mediacount INT,
    colorcount INT,
    rank INT,
    datebegin INT,
    dateend INT,
    FOREIGN KEY (objectid) REFERENCES artifact_metadata(id)
)
""")

print(" Table 'artifact_media' created successfully.")

# %%
#  Create Table: artifact_colors 

my_cursor.execute("""
CREATE TABLE IF NOT EXISTS artifact_colors (
    objectid INTEGER NOT NULL,
    color TEXT,
    spectrum TEXT,
    hue TEXT,
    percent REAL,
    css3 TEXT,
    FOREIGN KEY (objectid) REFERENCES artifact_metadata(id),
    PRIMARY KEY (objectid, color(50))
)
""")

print(" Table 'artifact_colors' created successfully.")

# %%
# fetched Api data:
import requests

API_KEY = "76fc507d-66dd-44b8-b5eb-c350f65fa1c7"
url = "https://api.harvardartmuseums.org/object"

metadata = []
media = []
colors = []

page = 1
while len(metadata) < 2500:
    params = {
        "apikey": API_KEY,
        "size": 100,
        "page": page
    }

    response = requests.get(url, params=params)
    data = response.json()

    for i in data['records']:
        metadata.append(dict(
            id=i.get('id'),
            title=i.get('title'),
            culture=i.get('culture'),
            period=i.get('period'),
            century=i.get('century'),
            medium=i.get('medium'),
            dimensions=i.get('dimensions'),
            description=i.get('description'),
            department=i.get('department'),
            classification=i.get('classification'),
            accessionyear=i.get('accessionyear'),
            accessionmethod=i.get('accessionmethod')
        ))

        media.append(dict(
            objectid=i.get('id'),
            imagecount=i.get('imagecount'),
            mediacount=i.get('mediacount'),
            colorcount=i.get('colorcount'),
            rank=i.get('rank'),
            datebegin=i.get('datebegin'),
            dateend=i.get('dateend')
        ))

        color_details = i.get('colors')
        if color_details:
            for j in color_details:
                colors.append(dict(
                    objectid=i.get('id'),
                    color=j.get('color'),
                    spectrum=j.get('spectrum'),
                    hue=j.get('hue'),
                    percent=j.get('percent'),
                    css3=j.get('css3')
                ))

        if len(metadata) >= 2500:
            break

    print("Fetched records:", len(metadata))
    page += 1

print("Total metadata records:", len(metadata))
print("Total media records:", len(media))
print("Total color records:", len(colors))

# %%
#Data Insert in Table 1: artifact_metadata
inserted_count = 0

for record in metadata:
    insert_query = """
    INSERT IGNORE INTO artifact_metadata (
        id, title, culture, period, century, medium, dimensions,
        description, department, classification, accessionyear, accessionmethod
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        record.get("id"), record.get("title"), record.get("culture"), record.get("period"),
        record.get("century"), record.get("medium"), record.get("dimensions"),
        record.get("description"), record.get("department"), record.get("classification"),
        record.get("accessionyear"), record.get("accessionmethod")
    )

    try:
        my_cursor.execute(insert_query, values)
        inserted_count += 1
    except Exception as e:
        print("Skipped:", e)

my_db.commit()
print("Inserted into artifact_metadata:", inserted_count)

# %%
#Data Insert in Table 2: artifact_media
inserted_count = 0

for record in media:
    insert_query = """
    INSERT IGNORE INTO artifact_media (
        objectid, imagecount, mediacount, colorcount, rank, datebegin, dateend
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        record.get("objectid"), record.get("imagecount", 0), record.get("mediacount", 0),
        record.get("colorcount", 0), record.get("rank", 0),
        record.get("datebegin", 0), record.get("dateend", 0)
    )

    try:
        my_cursor.execute(insert_query, values)
        inserted_count += 1
    except Exception as e:
        print("Skipped:", e)

my_db.commit()
print("Inserted into artifact_media:", inserted_count)

# %%
#Data Insert in Table 3: artifact_colors
inserted_count = 0

for record in colors:
    insert_query = """
    INSERT IGNORE INTO artifact_colors (
        objectid, color, spectrum, hue, percent, css3
    ) VALUES (%s, %s, %s, %s, %s, %s)
    """
    values = (
        record.get("objectid"), record.get("color"), record.get("spectrum"),
        record.get("hue"), record.get("percent"), record.get("css3")
    )

    try:
        my_cursor.execute(insert_query, values)
        inserted_count += 1
    except Exception as e:
        print("Skipped:", e)

my_db.commit()
print("Inserted into artifact_colors:", inserted_count)

# %%
#OUERIES QUESTIONS:
# %%
#1: List all artifacts from the 11th century belonging to Byzantine culture
query = """
SELECT *
FROM artifact_metadata
WHERE century = '11th century'
  AND culture = 'Byzantine';
"""
my_cursor.execute(query)

# %%
#2. What are the unique cultures represented in the artifacts?
query = """
SELECT DISTINCT culture
FROM artifact_metadata
WHERE culture IS NOT NULL
ORDER BY culture;
"""

my_cursor.execute(query)
cultures = [row[0] for row in my_cursor.fetchall()]
print(cultures)

# %%
#3. List all artifacts from the Archaic Period.
query = """
SELECT id, title, culture, century, medium, classification
FROM artifact_metadata
WHERE period = 'Archaic';
"""
my_cursor.execute(query)
for row in my_cursor.fetchall():
    print(row)

# %%
#4. List artifact titles ordered by accession year in descending order
query = """
SELECT title, accessionyear
FROM artifact_metadata
WHERE accessionyear IS NOT NULL
ORDER BY accessionyear DESC;
"""

my_cursor.execute(query)
results = my_cursor.fetchall()

for row in results:
    print(row)

# %%
#5. How many artifacts are there per department?
query = """
SELECT department, COUNT(*) AS artifact_count
FROM artifact_metadata
WHERE department IS NOT NULL
GROUP BY department
ORDER BY artifact_count DESC;
"""

my_cursor.execute(query)
for row in my_cursor.fetchall():
    print(row)
# %%
#SQL OURIES FOR artifact_media
# %%
#6. Which artifacts have more than 1 image?
query = """
SELECT m.objectid, d.title, m.imagecount
FROM artifact_media AS m
JOIN artifact_metadata AS d ON m.objectid = d.id
WHERE m.imagecount > 1
ORDER BY m.imagecount DESC;
"""

my_cursor.execute(query)
for row in my_cursor.fetchall():
    print(row)
# %%
#7. What is the average rank of all artifacts?
query = """
SELECT AVG(rank) AS average_rank
FROM artifact_media
WHERE rank IS NOT NULL;
"""

my_cursor.execute(query)
avg_rank = my_cursor.fetchone()[0]
print("Average Rank of Artifacts:", avg_rank)

# %%
#8. Which artifacts have a higher colorcount than mediacount?
query = """
SELECT m.objectid, d.title, m.colorcount, m.mediacount
FROM artifact_media AS m
JOIN artifact_metadata AS d ON m.objectid = d.id
WHERE m.colorcount > m.mediacount;
"""

my_cursor.execute(query)
for row in my_cursor.fetchall():
    print(row)

# %%
#9. List all artifacts created between 1500 and 1600
query = """
SELECT m.objectid, d.title, m.datebegin, m.dateend
FROM artifact_media AS m
JOIN artifact_metadata AS d ON m.objectid = d.id
WHERE m.datebegin >= 1500 AND m.dateend <= 1600;
"""

my_cursor.execute(query)
for row in my_cursor.fetchall():
    print(row)
# %%
#10.How many artifacts have no media files?
query = """
SELECT COUNT(*) AS artifacts_without_media
FROM artifact_media
WHERE mediacount = 0 OR mediacount IS NULL;
"""

my_cursor.execute(query)
count = my_cursor.fetchone()[0]
print("Artifacts without media files:", count)
# %%
#OUERIES FOR artifact_colors Table
#%%
#11. What are all the distinct hues used in the dataset?
query = """
SELECT DISTINCT hue
FROM artifact_colors
WHERE hue IS NOT NULL
ORDER BY hue;
"""

my_cursor.execute(query)
hues = [row[0] for row in my_cursor.fetchall()]
print("Distinct Hues:", hues)

# %%
#12.What are the top 5 most used colors by frequency?
query = """
SELECT color, COUNT(*) AS frequency
FROM artifact_colors
WHERE color IS NOT NULL
GROUP BY color
ORDER BY frequency DESC
LIMIT 5;
"""

my_cursor.execute(query)
top_colors = my_cursor.fetchall()
for color, freq in top_colors:
    print(color, "-", freq)
# %%
#13.What is the average coverage percentage for each hue?
query = """
SELECT hue, AVG(percent) AS average_coverage
FROM artifact_colors
WHERE hue IS NOT NULL
GROUP BY hue
ORDER BY average_coverage DESC;
"""

my_cursor.execute(query)
for row in my_cursor.fetchall():
    print(row)

# %%
#14.List all colors used for a given artifact ID.
artifact_id = 12345  # replace with any ID

query = f"""
SELECT color, hue, percent, css3
FROM artifact_colors
WHERE objectid = {artifact_id};
"""

my_cursor.execute(query)
for row in my_cursor.fetchall():
    print(row)

# %%
#15.What is the total number of color entries in the dataset?
query = "SELECT COUNT(*) AS total_color_entries FROM artifact_colors;"
my_cursor.execute(query)
count = my_cursor.fetchone()[0]
print("Total number of color entries:", count)
# %%
#JOINS BASED QUERIES
#%%
#16.List artifact titles and hues for all artifacts belonging to the Byzantine culture.
query = """
SELECT m.title, c.hue
FROM artifact_metadata AS m
JOIN artifact_colors AS c
    ON m.id = c.objectid
WHERE m.culture = 'Byzantine'
  AND c.hue IS NOT NULL
ORDER BY m.title;
"""

my_cursor.execute(query)
for row in my_cursor.fetchall():
    print(row)
# %%
#17.List each artifact title with its associated hues.
query = """
SELECT m.title, c.hue
FROM artifact_metadata AS m
JOIN artifact_colors AS c
    ON m.id = c.objectid
WHERE c.hue IS NOT NULL
ORDER BY m.title;
"""

my_cursor.execute(query)
for row in my_cursor.fetchall():
    print(row)
# %%
#18.Get artifact titles, cultures, and media ranks where the period is not null
query = """
SELECT m.title, m.culture, me.rank
FROM artifact_metadata AS m
JOIN artifact_media AS me
    ON m.id = me.objectid
WHERE m.period IS NOT NULL
ORDER BY me.rank DESC;
"""

my_cursor.execute(query)
for row in my_cursor.fetchall():
    print(row)
# %%
#19.Find artifact titles ranked in the top 10 that include the color hue "Grey".
query = """
SELECT m.title, me.rank, c.hue
FROM artifact_metadata AS m
JOIN artifact_media AS me ON m.id = me.objectid
JOIN artifact_colors AS c ON m.id = c.objectid
WHERE c.hue = 'Grey'
ORDER BY me.rank DESC
LIMIT 10;
"""

my_cursor.execute(query)
for row in my_cursor.fetchall():
    print(row)
# %%
#20.How many artifacts exist per classification, and what is the average media count
#for each?
query = """
SELECT 
    m.classification,
    COUNT(m.id) AS artifact_count,
    AVG(me.mediacount) AS average_media_count
FROM artifact_metadata AS m
JOIN artifact_media AS me 
    ON m.id = me.objectid
WHERE m.classification IS NOT NULL
GROUP BY m.classification
ORDER BY artifact_count DESC;
"""

my_cursor.execute(query)
for row in my_cursor.fetchall():
    print(row)
# %%
print("backend part sucessfully done")