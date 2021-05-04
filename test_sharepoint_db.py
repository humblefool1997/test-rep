import json
import os
import tempfile
import yaml
from sqlalchemy import create_engine
from sqlalchemy.sql import select
import pandas as pd
import logging
import sqlite3
from office365.runtime.auth.user_credential import UserCredential
from office365.runtime.http.request_options import RequestOptions
from office365.sharepoint.client_context import ClientContext

with open(r'config.yaml') as file:
    config_list = yaml.load(file, Loader=yaml.FullLoader)

email = config_list['email']
password = config_list['password']
site_url = config_list['data_model']['site_url']
file_url = config_list['data_model']['file_url']
file_name = config_list['data_model']['file_name']
file_location = './data_sources/' + file_name
cols = config_list['data_model']['cols']
table_name = config_list['data_model']['table']
def fetch_file_sharepoint(email,password,site_url,file_name,file_url):
    ctx = ClientContext(site_url).with_credentials(UserCredential(email,password))
    download_path = os.path.join(tempfile.mkdtemp(), os.path.basename(file_url))
    with open('./data_sources/'+ file_name, "wb") as local_file:
        file = ctx.web.get_file_by_server_relative_url(file_url).download(local_file).execute_query()
    print("[Ok] file has been downloaded: {0}".format(download_path))
result = fetch_file_sharepoint(email, password, site_url, file_name, file_url)
print(result)

def ingest_data_to_db_csv(file_location,table_name,cols):
    data = pd.read_csv(file_location,sep=',',skiprows=0,na_values=['.', '??']) 
    data=data.filter(regex=cols)
    logging.info("Loading File Successfull")
    engine = create_engine('sqlite:///data_store.db', echo=True)
    sqlite_connection = engine.connect()
    sqlite_table = table_name
    logging.info('Writing data into' + table_name + 'table')
    data.to_sql(sqlite_table, sqlite_connection, if_exists='append')
    logging.info('Writing Data into DB Successfull')
    sqlite_connection.close()
    return "Data Ingestion Sucessfull"
ingest_data_to_db_csv(file_location,table_name,cols)
con = sqlite3.connect("data_store.db")
df = pd.read_sql_query("SELECT * from "+table_name, con)
print(df)