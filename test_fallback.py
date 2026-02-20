# This file is used to test the fallback database functionality in a local environment.
import sqlite3
conn = sqlite3.connect("fallback.db")
cursor = conn.cursor()
cursor.execute("SELECT id, payload FROM pending ORDER BY id ASC")
row = cursor.fetchone()
print(row)