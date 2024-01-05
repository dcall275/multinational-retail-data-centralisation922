# Multinational Retail Data Centralisation Project

The project provided a unified source of data for a multinational company that sells various goods across the globe.
Their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team.
This project enables to the company to make its sales data accessible from one central location. In an effort to become more data-driven, your organisation would like to make its sales data accessible from one centralised location. 

## Files
The python files created to extract, clean, upload and process the data are: 

## data_extraction.py
Contains methods to extract data from various sources. 

## data_cleaning.py
Contains methods to clean data generated by the extraction methods. 

## database_utils.py
Contains the data connection methods which creates ways to connect to the postgresql database where the extracted data will be ingested to (sales_data) and also a way to connect to source databases (AWS)

## main_data_processing.py
imports the classes from the data_extraction, data_cleaning and database_utils files to generates the ETL process for each database table to be loaded to the postgresql database.  

## yaml files
There are 3 yaml files included in the .git ignore.  The first yaml file holds the credentials for the source aws database (from which we will be extracting data) and the second
yaml file holds the credentials for the local postgreSQL database called sales data to which the data will be loaded to.  

### Ongoing
This project is ongoing and will be updated.  
