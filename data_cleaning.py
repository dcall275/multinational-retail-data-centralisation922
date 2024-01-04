import pandas as pd
from data_extraction import DataExtractor
import numpy as np
import yaml

class DataCleaning:
     def clean_user_data(self, user_data):
       df = self.clean_invalid_date(df, 'date_of_birth') #cleaning invalid dates
       df = self.clean_invalid_date(df, 'join_date') #cleaning invalid dates
       df = self.clean_NaNs_Nulls_misses(df) #cleaning missing and null values
       df.drop(columns='1', inplace = True) #drop unwanted columns
       return df
     
     # data_cleaning.py  #have a look at github around the cleaning logig for clean_card data

     def clean_card_data(self, card_data):
       df = self.clean_invalid_card_number(df, 'card_number') #cleaning invalid card numbers
       df = self.clean_invalid_expiry_date(df, 'expiry_date') #cleaning invalid expiry dates
       df = self.clean_invalid_provider(df, 'card_provider') #cleaning invalid card provider name
       df - self.clean_invalid_payment_dater(df, 'date_payment_confirmed') #cleaning invalid payment date
       df.drop(columns= '1', inplace = True) #drop unwanted columns
       return df
     
     def clean_store_data(self, store_data):  
     # Add cleaning logic for store data = #need to work this out 
       cleaned_store_data = store_data
       return cleaned_store_data

     def clean_products(self, products_data):
       #add cleaning data i.e. remove rows with null data
       cleaned_products_data = products_data.dropna()
       return cleaned_products_data

     def convert_product_weights(self, products_data):
        # Ensure 'weight' column is treated as a string to handle non-numeric values
        products_data['weight'] = products_data['weight'].astype(str)
        
        # Convert weights to kg using the provided logic
        products_data['weight_in_kg'] = products_data['weight'].apply(self._convert_weight)

        # Drop the original 'weight' column if needed
        # products_data = products_data.drop('weight', axis=1)

        return products_data

     def _convert_weight(self, weight):
        #Handles different units and converts to kg
        if 'g' in weight:
            weight_in_kg = float(weight.replace('g', '')) / 1000
        elif 'ml' in weight:
            weight_in_kg = float(weight.replace('ml', '')) / 1000
        else:
            try:
                # If already a numeric value, assuming it's in kg
                weight_in_kg = float(weight)
            except ValueError:
                # If it's not numeric, set to NaN
                weight_in_kg = pd.NA

        return weight_in_kg
     
     # data_cleaning.py

     def clean_orders_data(self, orders_data):
        # Drop unnecessary columns
        orders_data_cleaned = orders_data.drop(['first_name', 'last_name', '1'], axis=1)
        return orders_data_cleaned

     def clean_date_times_data(self, date_times_data):
        # Perform any necessary cleaning steps here
        # (e.g., handle missing values, format dates, etc.)

        return date_times_data

    








