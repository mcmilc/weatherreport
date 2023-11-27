import os
from google.cloud import bigquery

root_dir = os.getenv("HOME")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(
    root_dir,
    "Activities",
    "projects",
    "southbayweather",
    "database",
    "bubbly-mission-402701-357b024147cf.json",
)

client = bigquery.Client()


sql_query = """
SELECT
    refresh_date AS Day,
    term AS Top_Term,
    rank,
FROM `bigquery-public-data.google_trends.top_terms`
LIMIT 1000
"""

query_job = client.query(sql_query)

for row in query_job.result():
    print(row)
