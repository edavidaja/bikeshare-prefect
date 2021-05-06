#!/usr/bin/env python
import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

# Gets the version
ctx = snowflake.connector.connect(
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    account=os.getenv("DB_NAME"),
)
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()
