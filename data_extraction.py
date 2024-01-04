import pandas as pd
import boto3
from database_utils import DatabaseConnector
import tabula
import requests
import json

class DataExtractor: #set up a class named DataExtractor
    def read_rds_table(self, db_connector, table_name):
        engine = db_connector.init_db_engine()
        query = f"SELECT * FROM {table_name};"
        data = pd.read_sql(query, engine)
        return data
    
    def retrieve_pdf_data(self, pdf_link):
        pdf_data = tabula.read_pdf(pdf_link, pages='all', multiple_tables=True)
        combined_data = pd.concat(pdf_data, ignore_index=True) #combines multiple tables
        return combined_data
    
    def list_number_of_stores(self, number_of_stores_endpoint, headers):
        response = requests.get(number_of_stores_endpoint, headers=headers)
        return response.json()["number_of_stores"]
    
    def retrieve_stores_data(self, store_endpoint, headers):
        response = requests.get(store_endpoint, headers=headers)
        stores_data = response.json()
        return stores_data

    def extract_from_s3(self, s3_address): #AWS CLI must be configured for access
        s3_client = boto3.client('s3')
        s3_bucket, s3_key = s3_address.split('/')[2], '/'. join(s3_address.split('/')[3:])
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        products_data = pd.read_csv(response['Body'])
        return products_data

    def extract_orders_data(self, database_connector):
        orders_table_name = database_connector.get_orders_table_name()
        orders_data = self.read_rds_table(database_connector, orders_table_name)
        return orders_data

    def extract_from_s3_json(self, s3_address):
        # Initialize S3 client
        s3_client = boto3.client('s3')

        #Downloads JSON file from S3
        response = s3_client.get_object(Bucket='data-handling-public', Key='date_details.json')
        json_content = response['Body'].read().decode('utf-8')

        #Loads JSON content into Pandas DataFrame
        date_times_data = pd.json_normalize(json.loads(json_content))

        return date_times_data

    
