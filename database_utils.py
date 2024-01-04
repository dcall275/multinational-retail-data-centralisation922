import yaml
from sqlalchemy import create_engine #access and connect to sql databases
import psycopg2 #establishes connections to PostgreSQL databases, executes SQL queries, and retrieves data
import pandas as pd 

class DatabaseConnector: #conects to local database and external database
     def __init__(self, rds_creds_file, local_db_creds_file):
        self.rds_creds_file = rds_creds_file
        self.local_db_creds_file = local_db_creds_file
        self.rds_creds = self.read_db_creds(rds_creds_file)
        self.local_db_creds = self.read_db_creds(local_db_creds_file)
        self.rds_engine = self.init_db_engine(self.rds_creds)
        self.local_db_engine = self.init_db_engine(self.local_db_creds)

     def read_db_creds(self):
         with open('db_creds.yaml', 'r') as file:
             creds = yaml.safe_load(file)
         return creds

     def init_db_engine(self, creds):
         db_url = f"postgresql://{creds['USER']}:{creds['PASSWORD']}@{creds['HOST']}:{creds['PORT']}/{creds['DATABASE']}"
         engine = create_engine(db_url)
         return engine

     def list_rds_tables(self):
         tables = self.rds_engine.table_names()
         return tables
     
     def list_local_db_tables(self):
         tables = self.local_db_engine.table_names()
         return tables
     
     def upload_to_rds_db(self, data, table_name):
         data.to_sql(table_name, self.rds_engine, if_exists = 'replace', index=False)

     def upload_to_local_db(self, data, table_name):
         data.to_sql(table_name, self.local_db_engine, if_exists='replace', index=False)

     def list_db_tables(self):
         engine = self.init_db_engine()
         tables = engine.table_names()
         return tables
     
     def upload_to_db(self, data, table_name):
         engine = self.init_db_engine()
         data.to_sql(table_name, engine, if_exists='replace', index=False)

     def get_orders_table_name(self):
        tables = self.list_db_tables()
        # Assume the table name contains 'orders'
        orders_table_name = [table for table in tables if 'orders' in table.lower()]
        if orders_table_name:
            return orders_table_name[0]
        else:
            raise ValueError("Orders table not found in the database.")

     def upload_cleaned_orders_data(self, data_cleaned):
        table_name = 'orders_table'
        self.upload_to_db(data_cleaned, table_name)

     def upload_date_times_data(self, date_times_data):
        table_name = 'dim_date_times'
        self.upload_to_db(date_times_data, table_name)

    





