import json
import pandas as pd
import os
import re
from dateutil import parser as date_parser
import mysql.connector
from mysql.connector import Error

# ----------------------------
# Database Utilities
# ----------------------------
class DatabaseManager:
    @staticmethod
    def create_connection(host='localhost', database='sales_db', user='root', password=''):
        """
            Simple Database connection, checks if database exists and create it if doesn't
        """
        try:
            connection = mysql.connector.connect(
                host=host,
                database=database,
                user=user,
                password=password
            )
            
            if connection.is_connected():
                print(f"Successfully connected to MySQL database '{database}'")
                return connection
                
        except Error as e:
            print(f"Error connecting to MySQL: {str(e)}")
            
            # Try to create the database if it doesn't exist
            if "Unknown database" in str(e):
                try:
                    # Connect without specifying a database
                    conn = mysql.connector.connect(
                        host=host,
                        user=user,
                        password=password
                    )
                    
                    if conn.is_connected():
                        cursor = conn.cursor()
                        cursor.execute(f"CREATE DATABASE {database}")
                        print(f"Database '{database}' created successfully")
                        cursor.close()
                        conn.close()
                        
                        # Try connecting again with the new database
                        return DatabaseManager.create_connection(host, database, user, password)
                except Error as create_err:
                    print(f"Error creating database: {str(create_err)}")
                    
        return None

# ----------------------------
# Data Extraction
# ----------------------------
class DataExtractor:
    @staticmethod
    def from_json(json_file_path):
        """
        simply extract data from the json file
        """
        print(f"Extracting data from {json_file_path}...")
        
        # check filepath existence first
        if not os.path.exists(json_file_path):
            print(f"Error: File {json_file_path} not found.")
            return []
        
        try:
            with open(json_file_path, 'r') as file:
                data = json.load(file)
                print(f"Successfully extracted {len(data)} records.")
                return data
        
        except Exception as e:
            print(f"Error: {str(e)}")
            return []

# ----------------------------
# Data Transformation
# ----------------------------
class DataTransformer:
    @staticmethod
    def standardize_date(date_str):
        """
        The date format is different for different function"
        trying to standardize the dates
        """
        if pd.isna(date_str):
            return None

        date_str = str(date_str).strip()
        
        try:
            # First try parsing directly using dateutil (handles 99% of cases)
            return date_parser.parse(date_str, fuzzy=True, ignoretz=True).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass

        # Attempt to fix malformed dates like '20 23-07-22'
        date_str = re.sub(r'(\d{2})\s+(\d{2})([-/\s])', r'\1\2\3', date_str)
        date_str = re.sub(r'\s+', '-', date_str)

        try:
            return date_parser.parse(date_str, fuzzy=True, ignoretz=True).strftime("%Y-%m-%d")
        except Exception:
            print(f"Could not parse date: '{date_str}'")
            return None

    # ********************************************************
    # Main Function used for transformations in PART 1 Requirements
    # *********************************************************
    @staticmethod
    def transform(data):
        """
        Transform the data:
        1. Flatten nested product object
        2. Standardize date format
        3. Calculate total_value
        4. Handle missing or invalid data (Replace with GUEST and flag)
        """
        print("Transforming data...")
        
        # since data has flattended structure we will flatten the data using panda's built in function
        df = pd.json_normalize(data)
        print("Before transformation : ", df.columns)
        
        
        # Step 2: Flatten nested product object
        if 'product.id' in df.columns:
            # Data is already flattened by json_normalize
            df = df.rename(columns={
                'product.id': 'product_id',
                'product.name': 'product_name',
                'product.category': 'category',
                'product.price': 'price'
            })
        else:
            # Extract product data manually if json_normalize didn't flatten it
            product_cols = ['id', 'name', 'category', 'price']
            for col in product_cols:
                col_name = f'product_{col}' if col == 'id' or col == 'name' else col
                df[col_name] = df['product'].apply(lambda x: x.get(col, None) if isinstance(x, dict) else None)
            
            df = df.drop('product', axis=1)
        
        # due to too much inconsistency in date formats we will standardize date column
        df['standardized_date'] = df['date'].apply(DataTransformer.standardize_date)
        
        # Ensure price is numeric
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        
        # Replace negative quantities with absolute values
        df['original_quantity'] = df['quantity']  # Keep original for reference
        df['quantity'] = df['quantity'].apply(lambda x: abs(x) if isinstance(x, (int, float)) and x < 0 else x)
        
        # Flag records with data quality issues
        df['has_missing_customer'] = df['customer_id'].isna()
        df['had_negative_quantity'] = df['original_quantity'] < 0
        df['had_date_format_issue'] = df['date'] != df['standardized_date']
        
        # Calculate total_value
        df['total_value'] = df['quantity'] * df['price']
        
        # Rename any columns for clarity
        df = df.rename(columns={'standardized_date': 'date_std'})

        # Replacing IDs with "GUEST" where they are null
        # we will fill the ids with unknown in part3 instead of guest for now :) we have flagged it though
        # df['customer_id'] = df['customer_id'].fillna('GUEST')

        # Drop any unnecessary columns
        if 'original_quantity' in df.columns:
            df = df.drop('original_quantity', axis=1)
        
        print(f"Transformation complete. Resulting DataFrame has {len(df)} rows and {len(df.columns)} columns.")
        return df

# ----------------------------
# Data Loading
# ----------------------------
class DataLoader:
    @staticmethod
    def to_mysql(df, connection_params):
        print(f"Loading data into MySQL database...")
        
        # Create connection
        conn = DatabaseManager.create_connection(**connection_params)
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            # Create the sales table
            DataLoader._create_table(cursor)
            
            # First, delete any existing data to avoid key violations on re-runs
            cursor.execute("DELETE FROM sales")
            
            # Prepare data for insertion
            data_values = DataLoader._prepare_data_for_insertion(df)
            
            # Prepare batch insert SQL
            columns = ['transaction_id', 'customer_id', 'product_id', 'product_name', 'category', 
                      'price', 'quantity', 'date', 'date_std', 'region', 'total_value',
                      'has_missing_customer', 'had_negative_quantity', 'had_date_format_issue']
            
            placeholders = ', '.join(['%s'] * len(columns))
            insert_sql = f"INSERT INTO sales ({', '.join(columns)}) VALUES ({placeholders})"
            
            # Execute batch insert
            cursor.executemany(insert_sql, data_values)
            
            conn.commit()
            print(f"Successfully loaded {len(df)} records into the database.")
            
            cursor.close()
            conn.close()
            
            return True
        
        except Error as e:
            print(f"Error loading data into database: {str(e)}")
            if conn.is_connected():
                conn.close()
            return False

    @staticmethod
    def _create_table(cursor):
        """Create the sales table"""
        create_table_sql = '''
        CREATE TABLE IF NOT EXISTS sales (
            transaction_id VARCHAR(50) PRIMARY KEY,    -- Reduced from 255
            customer_id VARCHAR(50),                   -- Reduced from 255
            product_id VARCHAR(30),                    -- Reduced from 255
            product_name VARCHAR(150),                 -- Reduced from 255
            category VARCHAR(100),                     -- Kept as is
            price DECIMAL(10,2),
            quantity INT,
            date VARCHAR(100),
            date_std DATE,
            region VARCHAR(100),
            total_value DECIMAL(12,2),
            has_missing_customer BOOLEAN,
            had_negative_quantity BOOLEAN,
            had_date_format_issue BOOLEAN
        );
        '''
        cursor.execute(create_table_sql)

    @staticmethod
    def _prepare_data_for_insertion(df):
        """Prepare DataFrame data for MySQL insertion"""
        columns = ['transaction_id', 'customer_id', 'product_id', 'product_name', 'category', 
                  'price', 'quantity', 'date', 'date_std', 'region', 'total_value',
                  'has_missing_customer', 'had_negative_quantity', 'had_date_format_issue']
        
        data_values = []
        # Convert DataFrame to list of tuples for insertion
        for _, row in df.iterrows():
            # Convert boolean columns to proper MySQL boolean
            row_data = tuple(row[col] for col in columns)
            data_values.append(row_data)
        
        return data_values

# ----------------------------
# Main ETL Pipeline
# ----------------------------
class ETLPipeline:
    @staticmethod
    def run(input_file, connection_params):
        """
        Runs the Complete ETL  Pipeline 
        """
        print("Starting ETL pipeline...")
        
        # Extract
        data = DataExtractor.from_json(input_file)
        print(data[0] if data else "No data extracted")
        if not data:
            print("Extraction failed. ETL process aborted.")
            return False
        
        # Transform
        df = DataTransformer.transform(data)
        if df.empty:
            print("Transformation failed. ETL process aborted.")
            return False

        # Load
        success = DataLoader.to_mysql(df, connection_params)
        if not success:
            print("Loading failed. ETL process aborted.")
            return False
        
        print("ETL pipeline completed successfully!")
        return True

# ----------------------------
# Entry Point
# ----------------------------
def main():
    """
    Main ETL process.
    """
    # File paths
    # Use os.path.join for constructing file paths (more portable)
    input_file = os.path.join("..", "data", "sales_data.json")
    
    # MySQL connection parameters
    # Update these with your actual MySQL credentials
    mysql_params = {
        'host': 'localhost',
        'database': 'sales_db',
        'user': 'user',
        'password': 'pass'  # Add your MySQL password here
    }
    
    # Run ETL pipeline
    ETLPipeline.run(input_file, mysql_params)

if __name__ == '__main__':
    main()