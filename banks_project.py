from bs4 import BeautifulSoup
import requests 
import pandas as pd
from datetime import datetime
import sqlite3


def log_progress(message):
    timeformat = '%Y-%h-%d-%H: %M: %S' 
    now = datetime.now()
    timestamp = now.strftime(timeformat)
    with open('code_log.txt','a') as f:
        f.write(timestamp + ' ' + message + '\n')

def extract(url,table_attribs):
    table_headers = []
    table_data = []

    page_content = requests.get(url).text
    bs_page_content = BeautifulSoup(page_content,'html.parser')
    target_table = bs_page_content.find('table',class_=table_attribs)

    for td_tag in target_table.find_all('td'):
        for child_tag in td_tag.find_all('button'):
            child_tag.decompose()
    target_table.prettify()
    table_headers = ['Rank', 'Bank', 'MC_US_Billion']
  
    for row in target_table.find_all('tr'):
        data_row = []
        for cell in row.find_all('td'):
            data_row.append(cell.get_text(strip=True))
        table_data.append(data_row)
    table_data = list(filter(None,table_data))
    df = pd.DataFrame(table_data,columns=table_headers)
    return df

def transform(df, csv_path):
    df_rate_info = pd.read_csv(csv_path)
    rates = df_rate_info[0:3]["Rate"]
    eur = rates[0]
    gbp = rates[1]
    inr = rates[2]
    df['MC_EUR_Billion'] = round(df['MC_US_Billion'].astype(float) * eur,2)
    df['MC_GBP_Billion'] = round(df['MC_US_Billion'].astype(float) * gbp,2)
    df['MC_INR_Billion'] = round(df['MC_US_Billion'].astype(float)* inr,2)
    return df

     
def load_to_csv(df,output_path):
    df.to_csv(output_path + 'Largest_banks_data.csv')

def load_to_db(df,sql_connection,table_name):
    df.to_sql(table_name,sql_connection,if_exists='replace', index=False)
    

log_progress('ELT Process started')

log_progress('Extracting Data')
df = extract('https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks','wikitable')
print(df)

log_progress('Transforming Data')
dataframe = transform(df,'./exchange_rate.csv')
print(dataframe)

log_progress('Saving file with new data')
load_to_csv(dataframe,'./')

log_progress("Connecting to the database")
connection = sqlite3.connect('Banks.db')

log_progress("Loading data to the database table")
load_to_db(dataframe,connection,'Largest_banks')

def run_queries(statement,conn):
    cursor = conn.cursor()
    cursor.execute(statement)
    result = cursor.fetchall()

    for row in result:
        print(row)
log_progress("Prepearing Queries")
print('SELECT * FROM Largest_banks')
run_queries('SELECT * FROM Largest_banks',connection)
print('\n')
print("SELECT AVG(MC_GBP_Billion) FROM Largest_banks")
run_queries("SELECT AVG(MC_GBP_Billion) FROM Largest_banks",connection)
print('\n')
print('SELECT Bank from Largest_banks LIMIT 5')
run_queries('SELECT Bank from Largest_banks LIMIT 5',connection)
connection.close()
log_progress("Closing Connection")