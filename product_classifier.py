import pandas as pd
import numpy as np
import glob
import pyodbc
import time

from tqdm import tqdm
from collections import Counter

import warnings
warnings.filterwarnings("ignore")

import pickle
from db_connection import *

from time import time
from datetime import datetime
from progress.bar import Bar


# Strings to connect to a database
driver_server = 'host_server'   
db_name = 'etl_db'
user_name = 'etl_user_name' 
pasword = 'pa$$word'

# connect to a database
AppendToLog("connection to a db started!")
conn = connect_to_db(driver_server, db_name, user_name, pasword)
curr = conn.cursor()

AppendToLog("Connected!")
    
"""Select the maximum id from table called Taxonomy in the database mentioned earlier.
    This is for the sake of automating the etl incrementally. That is, 
    when there are new productlines added to
    the database daily, the script grabs them and make a prediction of their labels (classes). 
    And append the the description of the products and their predicted labels 
    to the Taxonomy table (etl table) in the database.
    i.e.: Extract data 
"""

maxId = pd.read_sql_query("Select MAX(TransactionId) FROM Taxonomy", conn)
maxVal = maxId.iloc[0,0]

"""
    select description of daily purchased products, their unique ids from TransactionInvoiceLines for prediction
"""
Invoicelines = pd.read_sql_query(f'''Select TransactionId, Description, 
                                     Type FROM TransactionInvoiceLines 
                                     WHERE TransactionId > {maxVal}''', 
                                     conn)  
Invoicelines = Invoicelines.sort_values(by = ['TransactionId']) # Invoicelines.loc[Invoicelines['TransactionId'] > maxVal]

""" We need to filter on transactions paid (not on attempts and declined ones) 
    from another table called Transactions 
"""
Transactions = pd.read_sql_query(f"""Select Id, 
                               TransactionStart, State 
                               FROM Transactions 
                               WHERE Id > {maxVal}""", 
                               conn2)  # 
Transactions = Transactions.sort_values(by = ['Id']) # Transactions.loc[Transactions['Id'] > maxVal]                              


""" Now combine the invoiceline data and transaction (with status of transactions) data"""
def data_filter(data1, data2):
    
    data2.rename(columns={"Id": "TransactionId"}, inplace=True)
    prod_Line = data1.merge(data2, on = ['TransactionId'])
    
    return prod_Line

logFile("data pulling initiated!")   
prod_Line = data_filter(Invoicelines, Transactions)

# removing numbers
prod_Line["Description"] = prod_Line["Description"].str.replace('\d+','')  
prod_Line.dropna(subset = "Description", inplace=True)

AppendToLog("data pulled and cleansed!")   

# Loading the saved model
AppendToLog("Reading a pickle model/file!")   
with open("prod_cat_ml V20-12-2022.pkl", "rb") as file:
    model = pickle.load(file)

AppendToLog("transforming invoiceline data to vectors commenced!")
X = model['vectorizer'].transform(prod_Line["Description"])  

# make prediction (daily) (i.e. Transform)
pred = model['classifier'].predict(X)

# Add the predicted labels column that takes predicted values
prod_Line.loc[:,'ProductLabel'] = pred
AppendToLog("product class prediction made and result appended to a dataframe and ready to be loaded to a db!")
print(prod_Line.head())
               
# Load (L) to the database
########################################################################
################## Check if table exists  ##############################
table_exists = False
if curr.tables(table='Taxonomy', tabletype='TABLE').fetchone():
    table_exists = True

################## if it does not exist, create table ################
if not table_exists:
    curr.execute('''
                     CREATE TABLE Taxonomy(
                         TransactionId VARCHAR(255),
                         Description TEXT,
                         Type VARCHAR(100),
                          TransactionStart DATE,
                         State int,
                         ProductLabel TEXT)
                     ''')  
##########################################################################


##############  Insert data into the table  #######################       
def insert_dataframe(data):
    
    bar = Bar('Processing')
    for row in data.itertuples():
        curr.execute('''
                      INSERT INTO Taxonomy(TransactionId,
                                            Description, 
                                            Type,                                                        
                                            TransactionStart,
                                            State,
                                            ProductLabel)
                      VALUES (?,?,?,?,?,?,?)
                      ''',
                      row.TransactionId,
                      row.Description,
                      row.Type,
                      row.TransactionStart,
                      row.State,
                      row.ProductLabel
                      )
        bar.next()
        
    conn.commit()
    conn.close()
    bar.finish()
     
AppendToLog("Inserting data to the created table in the etl db about to start!")
insert_dataframe(prod_Line)


print('Task completed')
