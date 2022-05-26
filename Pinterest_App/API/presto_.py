
import prestodb
import pandas as pd

connection = prestodb.dbapi.connect(
    host='localhost',
    catalog='cassandra',
    user='Simeon',
    port=8080,
    schema='api_data'
)

cur = connection.cursor()
cur.execute("SELECT * FROM pinterest_data")
rows = cur.fetchall()

# for row in rows:
#     print(row)

api_df = pd.DataFrame(rows)
print(api_df)