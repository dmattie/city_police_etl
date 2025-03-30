import pandas as pd
import sqlite3
import os,sys

datamart="tickets_datamart.db"
try:
    os.remove(datamart)
except OSError:
    pass

conn = sqlite3.connect(datamart)
cur = conn.cursor()

# Create a table with appropriate column names and data types
cur.execute('''
CREATE TABLE IF NOT EXISTS DIM_Officer (
    officer_surrogate_key INTEGER PRIMARY KEY,
    officer_natural_key INTEGER ,
    OfficerName varchar(10),
    OfficerRank varchar(20)
);''')

cur.execute('''
create table if not exists DIM_Ticket_Type (
    ticket_type_surrogate_key integer primary key,
    ticket_type_natural_key varchar(10),
    Violation varchar(10),
    Fee integer)
''')
cur.execute('''
create table if not exists FACTS_Tickets (
   Officer_key integer references DIM_officer(officer_surrogate_key),
   Ticket_type_key integer references DIM_TicketT_Type(ticket_type_surrogate_key),
   Fee integer,
   PRIMARY KEY (Officer_key, Ticket_type_key))
''')

# existing_keys = cur.execute("SELECT surrogate_key FROM DIM_Officer").fetchall()
# existing_keys = {key[0] for key in existing_keys} 
# print(existing_keys)
# sys.exit(1)

tables=['DIM_Officer','DIM_Ticket_Type']
for table in tables:
    print(f"{table} ::::::: ")
    csv_file_path = f"data/{table}.csv"
    df = pd.read_csv(csv_file_path)

    for index, row in df.iterrows():
        num_columns = len(row)  # Get the number of columns
        question_marks = ', '.join(['?'] * num_columns)
        sql = f"INSERT INTO {table} values ({question_marks})"
        cur.execute(sql, row.values.tolist())         

# Commit the changes and close the connection
conn.commit()
conn.close()
