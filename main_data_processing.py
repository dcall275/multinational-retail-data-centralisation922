import pandas as pd
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

# Instantiate objects
database_connector = DatabaseConnector(db_creds_file,local_db_creds_file) #connect to both types of database
data_extractor = DataExtractor() #extract data from source
data_cleaner = DataCleaning() #clean the data

#details of both the external and local dbs credentials
db_creds_file = 'db_creds.yaml'
local_db_creds_file = 'local_db_creds.yaml'

#Extracts data
user_table_name = 'dim_users'
user_data = data_extractor.read_rds_table(database_connector, user_table_name)

#Cleans data
cleaned_user_data = data_cleaner.clean_user_data(user_data)

#Uploads to the database
database_connector.upload_to_db(cleaned_user_data, user_table_name)

pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws/card_details.pdf" #extract data from pdf file

card_data_df = data_extractor.retrieve_pdf_data(pdf_link)

cleaned_card_data = data_cleaner.clean_card_data(card_data_df) #clean card data

#Upload cleaned data to the database and to the dim_card_details table
card_table_name = 'dim_card_details'
database_connector.upload_to_db(cleaned_card_data, card_table_name)

#Retrieves the number of stores
number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
headers = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
number_of_stores = data_extractor.list_number_of_stores(number_of_stores_endpoint, headers)

#Retrieves store data for each store
store_endpoint_template = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{}"
all_stores_data = []
for store_number in range(1, number_of_stores + 1):
    store_endpoint = store_endpoint_template.format(store_number)
    store_data = data_extractor.retrieve_stores_data(store_endpoint, headers)
    all_stores_data.append(store_data)

#Combines store data into a single DataFrame
combined_store_data = pd.concat(all_stores_data, ignore_index=True)

#Cleans store data
cleaned_store_data = data_cleaner.clean_store_data(combined_store_data)

#Upload to the database and to the dim_store_details table
store_table_name = 'dim_store_details'
database_connector.upload_to_db(cleaned_store_data, store_table_name)

#Extracts product data from S3
s3_address = "s3://data-handling-public/products.csv"
products_data = data_extractor.extract_from_s3(s3_address)

#Converts product weights
cleaned_products_data = data_cleaner.convert_product_weights(products_data)

#Cleans product data
final_products_data = data_cleaner.clean_products_data(cleaned_products_data)

#Uploads to the database
products_table_name = 'dim_products'
database_connector.upload_to_db(final_products_data, products_table_name)

#Extracts product data from S3
s3_address = "s3://data-handling-public/products.csv"
products_data = data_extractor.extract_from_s3(s3_address)

#Converts product weights
cleaned_products_data = data_cleaner.convert_product_weights(products_data)

#Cleans product data
final_products_data = data_cleaner.clean_products_data(cleaned_products_data)

#Upload to the database to the dim_products table
products_table_name = 'dim_products'
database_connector.upload_to_db(final_products_data, products_table_name)

#Lists All Tables in the Database
orders_table_name = database_connector.get_orders_table_name()

#Extracts the Orders Data
orders_data = data_extractor.extract_orders_data(database_connector)

#Clean Orders Data
orders_data_cleaned = data_cleaner.clean_orders_data(orders_data)

#Upload Cleaned Orders Data
database_connector.upload_cleaned_orders_data(orders_data_cleaned)

#Extracts Data from S3
date_times_data = data_extractor.extract_from_s3_json('s3://data-handling-public/date_details.json')

#Cleans the Data
date_times_data_cleaned = data_cleaner.clean_date_times_data(date_times_data)

#Uploads cleaned Data to Database
database_connector.upload_date_times_data(date_times_data_cleaned)






