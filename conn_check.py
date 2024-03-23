import pyodbc

def test_db_connection(server_name, database_name):
    try:
        # Replace 'DRIVER' with the appropriate driver for your SQL Server version
        conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server_name + ';DATABASE=' + database_name + ';Trusted_Connection=yes;')
        print("Connection successful!")
        conn.close()
    except pyodbc.Error as e:
        print("Connection failed:", e)

# Replace 'server_name' and 'database_name' with your SQL Server details
server_name = 'OUTCAST-BATES'
database_name = 'AdventureWorks2019'

test_db_connection(server_name, database_name)
