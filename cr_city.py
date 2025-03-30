import pandas as pd
import sqlite3
import os

# Step 1: Read the CSV file into a DataFrame
csv_file_path = 'data/Officer.csv' 
df = pd.read_csv(csv_file_path)

try:
    os.remove('city_police_database.db')
except OSError:
    pass

# Step 2: Create a SQLite database and a table
database_file_path = 'city_police_database.db'  # This will be your SQLite DB file
conn = sqlite3.connect(database_file_path)
cur = conn.cursor()

# Create a table with appropriate column names and data types
cur.execute('''
CREATE TABLE IF NOT EXISTS Officer (
    id INTEGER PRIMARY KEY ,
    OfficerName varchar(10),
    OfficerRank varchar(20)
);''')
cur.execute('''
CREATE TABLE IF NOT EXISTS Parking_Ticket_type (
    PTTypeID varchar(2) PRIMARY KEY,
    PTViolation varchar(20),
    PTFee integer
);
''')
cur.execute('''
CREATE TABLE IF NOT EXISTS Driving_Ticket_Type (
    DTTypeID varchar(2) PRIMARY KEY,
    DTViolation Varchar(20),
    DTFee integer);
''')
cur.execute('''
    CREATE TABLE IF NOT EXISTS Vehicle (
    VehicleLPN VARCHAR(10) PRIMARY KEY,
    VehicleMake0 VARCHAR(20),
    VehicleModel VARCHAR(20)
);            
''')
cur.execute('''
    CREATE TABLE IF NOT EXISTS PARKING_TICKET (
    PKID VARCHAR(10) PRIMARY KEY,
    PTDATE VARCHAR(10),
    OfficerID integer,
    VehicleLPN varhar(10),
    PTTypeID varchar(10)        
    )
''')
cur.execute('''
    CREATE TABLE IF NOT EXISTS DRIVING_TICKET (
    DTID VARCHAR(10) PRIMARY KEY,
    DTDate VARCHAR(10),
    OfficerID integer,
    DLN integer,
    VehicleLPN varchar(10),
    DTTypeID varchar(10)        
    )
''')
cur.execute('''
    Create table if not exists DRIVER(
    DLN integer primary key,
    DriverName varchar(20),
    DriverGender varchar(10),
    DriverBirthYear integer
    )            
''')

cur.execute('''
    create table if not exists VEHICLE_REGISTRATION(
    VehicleLPN varchar(10) primary key,
    VehicleMake varchar(10),
    VehicleModel varchar(10),
    VehicleYear integer,
    OwnerDLN integer,
    OwnerName varchar(10),
    OwnerGender varchar(10),
    OwnerBirthYear integer                    
    )
''')

tables=['Driver','Driving_Ticket_type','Driving_Ticket','officer','Parking_ticket_type','Parking_Ticket','Vehicle_Registration','Vehicle']
for table in tables:
    print(f"{table} ::::::: ")
    csv_file_path = f"data/{table}.csv"
    df = pd.read_csv(csv_file_path)

    for index, row in df.iterrows():
        num_columns = len(row)  # Get the number of columns
        question_marks = ', '.join(['?'] * num_columns)
        sql = f"INSERT INTO {table} values ({question_marks})"
        cur.execute(sql, row.values.tolist()) 


        # cur.execute('''
        # INSERT INTO Officer 
        # VALUES (?, ?, ?)
        # ''',row)# (row.iloc[0], row.iloc[1], row.iloc[2]))
        

# Commit the changes and close the connection
conn.commit()
conn.close()
