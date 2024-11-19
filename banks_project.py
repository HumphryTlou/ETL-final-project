# Code for ETL operations on Country-GDP data

# Importing the required libraries
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 



url = 'https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion", "MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S' 
    now = datetime.now() 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')  



def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows:
        if row.find('td') is not None:
            col = row.find_all('td')
            bank_name = col[1].find_all('a')[1]['title']
            market_cap = col[2].contents[0][:-1]
            data_dict = {
                "Name": bank_name,
                "MC_USD_Billion": float(market_cap)
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)

    return df


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    exchange_rates_df = pd.read_csv(csv_path)
    
    exchange_rates = exchange_rates_df.set_index('Currency').to_dict()['Rate']
    
    df['MC_EUR_Billion'] = round(df['MC_USD_Billion'] * exchange_rates['EUR'], 2)
    df['MC_GBP_Billion'] = round(df['MC_USD_Billion'] * exchange_rates['GBP'], 2)
    df['MC_INR_Billion'] = round(df['MC_USD_Billion'] * exchange_rates['INR'], 2)
        
    return df



def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''

    df.to_csv(csv_path)


def load_to_db(df, sql_connection, table_name):
    ''' 
    This function saves the final data frame to a database
    table with the provided name. Function returns nothing.
    '''
 
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_queries(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


log_progress('Preliminaries complete. Initiating ETL process')

df_extracted = extract(url, table_attribs)

log_progress("Data extraction complete. Initiating Transformation process.")
df_transformed = transform(df_extracted, "exchange_rate.csv")
log_progress("Data transformation complete. Initiating Loading process")

#print(df_transformed)

load_to_csv(df_transformed, csv_path)
log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('Banks.db')
log_progress('SQL Connection initiated')

load_to_db(df_transformed, sql_connection, 'Largest_banks')
log_progress('Data loaded to Database as a table, Executing queries')

#query_statement = f"SELECT * FROM Largest_banks"
#query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
query_statement = f"SELECT Name from Largest_banks LIMIT 5"

run_queries(query_statement, sql_connection)
log_progress('Process Complete')

sql_connection.close()
log_progress('Server Connection closed')