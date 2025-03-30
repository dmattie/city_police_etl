import pandas as pd
import sqlite3

ops_conn = sqlite3.connect("city_police_database.db")
query = "SELECT * FROM driving_ticket"
tickets = pd.read_sql_query(query, ops_conn)
ops_conn.close()

datamart_conn = sqlite3.connect("tickets_datamart.db")

dim_officer = pd.read_sql_query("SELECT * FROM DIM_Officer", datamart_conn)
dim_ticket_type = pd.read_sql_query(
    "SELECT * FROM DIM_Ticket_Type", datamart_conn
)

for index, ticket in tickets.iterrows():
    ticket_type_skey = dim_ticket_type.loc[
        dim_ticket_type['ticket_type_natural_key'] == ticket['DTTypeID'], 
        'ticket_type_surrogate_key'
    ].item()
    
    fee = dim_ticket_type.loc[
        dim_ticket_type['ticket_type_natural_key'] == ticket['DTTypeID'], 
        'Fee'
    ].item()
    
    officer_skey = dim_officer.loc[
        dim_officer['officer_natural_key'] == ticket['OfficerID'], 
        'officer_surrogate_key'
    ].item()

    sql = (
        f"INSERT INTO FACTS_Tickets (Officer_key, Ticket_type_key, Fee) "
        f"VALUES ({ticket_type_skey}, {officer_skey}, {fee})"
    )
    print(sql)
    datamart_conn.execute(sql)

facts = pd.read_sql_query("SELECT * FROM facts_tickets", datamart_conn)
print(facts)

# datamart_conn.commit()
datamart_conn.close()