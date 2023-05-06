import pyodbc
from datetime import datetime

# Log file
def AppendToLog(txt):
    print(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + ': ' + txt)

# database connection
def connect_to_db(host_name, dbName, userName, passWord):
    
    try:
        conn = pyodbc.connect(host_name, 
                              database=dbName, 
                              user=userName, 
                              password=passWord)
        #cursor = conn.cursor()
    except pyodbc.OperationalError as e:
        raise e
    else:
        return conn
