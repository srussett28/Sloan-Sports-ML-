import pandas as pd
import os


input_dir = "C:/Users/Sloan/Documents/Github/Sloan-Sports-ML-"

for file_name in os.listdir(input_dir):
    if file_name.endswith(".xlsx"):
        file_path = os.path.join(input_dir, file_name)
        print(f" Processing file: {file_name}")


        df = pd.read_excel(file_path)
        
        
        print(" First 5 rows:")
        print(df.head(), "\n")

        
        print(" Data Information:")
        print(df.info(), "\n")

       
        print(" Summary Statistics:")
        print(df.describe(include='all'), "\n")

       
        print(" Missing Values Summary:")
        print(df.isnull().sum(), "\n")

        
        print(" Unique Values in Categorical Columns:")
        for col in df.select_dtypes(include=['object', 'category']).columns:
            print(f"{col}: {df[col].nunique()} unique values")

        print("-" * 50, "\n")
