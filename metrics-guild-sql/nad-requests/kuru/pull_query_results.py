
import snowflake.connector
import pandas as pd
import os
import queries
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER')
    , password=os.getenv('SNOWFLAKE_PASSWORD')
    , account=os.getenv('SNOWFLAKE_ACCOUNT')
    , warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
    , database=os.getenv('SNOWFLAKE_DATABASE')
    , schema=os.getenv('SNOWFLAKE_SCHEMA')
)

# query = queries.top_mdc_tokens
# df = pd.read_sql(query, conn)
# print(df.head())
# df.to_csv('mdc_top_tokens.csv', index=False)

query = queries.daily_traffic
df = pd.read_sql(query, conn)
print(df.head())
df.to_csv('daily_traffic.csv', index=False)

conn.close()