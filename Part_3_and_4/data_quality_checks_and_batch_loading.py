import json
import pandas as pd
import os
import re
import mysql.connector
from mysql.connector import Error
import logging
from collections import defaultdict
from dateutil import parser as date_parser
from sqlalchemy import create_engine

# ----------------------------
# Logger Setup
# ----------------------------
def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('etl_pipeline.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logger()

# ----------------------------
# Database Utilities
# ----------------------------
class DatabaseManager:
    @staticmethod
    def create_connection(host='localhost', database='sales_db', user='root', password=''):
        try:
            connection = mysql.connector.connect(
                host=host,
                database=database,
                user=user,
                password=password
            )
            if connection.is_connected():
                logger.info(f"Connected to MySQL database '{database}'")
                return connection
        except Error as e:
            logger.error(f"Connection error: {str(e)}")
            if "Unknown database" in str(e):
                DatabaseManager._create_database(host, user, password, database)
                return DatabaseManager.create_connection(host, database, user, password)
        return None

    @staticmethod
    def create_sqlalchemy_engine(host='localhost', database='sales_db', user='root', password=''):
        """Create SQLAlchemy engine for pandas to_sql operations"""
        try:
            connection_string = f"mysql+mysqlconnector://{user}:{password}@{host}/{database}"
            engine = create_engine(connection_string)
            logger.info(f"Created SQLAlchemy engine for database '{database}'")
            return engine
        except Exception as e:
            logger.error(f"Error creating SQLAlchemy engine: {str(e)}")
            return None

    @staticmethod
    def _create_database(host, user, password, database):
        try:
            conn = mysql.connector.connect(host=host, user=user, password=password)
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
                logger.info(f"Database '{database}' created")
                cursor.close()
                conn.close()
        except Error as e:
            logger.error(f"Error creating database: {str(e)}")

    @staticmethod
    def create_schema_and_indexes(connection_params):
        """Create database schema with tables and indexes"""
        conn = DatabaseManager.create_connection(**connection_params)
        if not conn:
            return False

        try:
            cursor = conn.cursor()
            
            # Create tables
            schema_commands = [
                """CREATE TABLE IF NOT EXISTS customers (
                    customer_id VARCHAR(50) PRIMARY KEY
                )""",
                
                """CREATE TABLE IF NOT EXISTS products (
                    product_id VARCHAR(30) PRIMARY KEY,
                    product_name VARCHAR(150),
                    category VARCHAR(100),
                    price DECIMAL(10,2)
                )""",
                
                """CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id VARCHAR(50) PRIMARY KEY,
                    customer_id VARCHAR(50),
                    product_id VARCHAR(30),
                    quantity INT,
                    date VARCHAR(100),
                    date_std DATE,
                    region VARCHAR(100),
                    total_value DECIMAL(12,2),
                    has_missing_customer BOOLEAN,
                    had_negative_quantity BOOLEAN,
                    had_date_format_issue BOOLEAN,
                    has_suspicious_values BOOLEAN,
                    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
                    FOREIGN KEY (product_id) REFERENCES products(product_id)
                )"""
            ]
            
            # Create indexes for performance
            index_commands = [
                "CREATE INDEX idx_transactions_region ON transactions(region)",
                "CREATE INDEX idx_transactions_date_std ON transactions(date_std)",
                "CREATE INDEX idx_transactions_product_id ON transactions(product_id)"
            ]
            
            # Execute schema creation
            for command in schema_commands:
                cursor.execute(command)
                logger.info("Created table")
            
            # Execute index creation
            for command in index_commands:
                cursor.execute(command)
                logger.info("Created index")
            
            conn.commit()
            logger.info("Database schema and indexes created successfully")
            return True
            
        except Error as e:
            logger.error(f"Schema creation error: {str(e)}")
            return False
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

# ----------------------------
# Data Extraction
# ----------------------------
class DataExtractor:
    @staticmethod
    def from_json(json_file_path):
        logger.info(f"Extracting from {json_file_path}...")
        
        if not os.path.exists(json_file_path):
            logger.error(f"File not found: {json_file_path}")
            return []
        
        try:
            with open(json_file_path, 'r') as file:
                data = json.load(file)
                logger.info(f"Extracted {len(data)} records")
                return data
        except Exception as e:
            logger.error(f"Extraction error: {str(e)}")
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
        
    @staticmethod
    def transform(data):
        logger.info("Transforming data...")
        df = DataTransformer._initial_prep(data)
        df = DataQualityChecker.perform_checks(df)
        logger.info(f"Transformation complete. Final shape: {df.shape}")
        return df

    @staticmethod
    def _initial_prep(data):
        df = pd.json_normalize(data)
        
        # Flatten product structure
        if 'product.id' in df.columns:
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
            
            # Drop the original product column
            df = df.drop('product', axis=1)
        
        # Replacing IDs with "GUEST" where they are null
        df['customer_id'] = df['customer_id'].fillna('GUEST')

        # Standardize dates and numeric fields                                                      
        df['date_std'] = df['date'].apply(DataTransformer.standardize_date)
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['total_value'] = df['quantity'] * df['price']
        df['had_date_format_issue'] = df['date'] != df['date_std']
        return df

# ----------------------------
# Data Quality Checks
# ----------------------------
# ********* Main Requirements of Part 3 ********************
# **********************************************************
# **********************************************************
class DataQualityChecker:
    @staticmethod
    def perform_checks(df):
        logger.info("Performing data quality checks...")
        issues = defaultdict(int)
        
        # Initialize quality flag columns
        df['has_missing_customer'] = False
        df['had_negative_quantity'] = False
        df['has_suspicious_values'] = False
        
        # Replace Null customer_ids with unknown
        df, count = DataQualityChecker._check_missing_customer(df)
        issues['missing_customer'] = count
        
        # Replace a negative quantitiy with positive quantity absolute and flag it 
        df, count = DataQualityChecker._check_negative_quantities(df)
        issues['negative_quantities'] = count
        
        # remove duplicates based on transcations_id
        count = DataQualityChecker._check_duplicate_transactions(df)
        issues['duplicate_transactions'] = count
        
        # checks if any product is missing any info like product_id, name or price
        count = DataQualityChecker._check_missing_product_info(df)
        issues['missing_product_info'] = count
        
        # checks missing prices or null 
        count = DataQualityChecker._check_invalid_prices(df)
        issues['invalid_prices'] = count
        
        # checks for total date issues
        df, count = DataQualityChecker._check_date_issues(df)
        issues['date_issues'] = count
        
        # check for missing transaction_ids
        count = DataQualityChecker._check_missing_transaction_ids(df)
        issues['missing_transaction_ids'] = count
        
        # suspicious vales according to business logic like here 
        # quantity too much or prices or value is abnormal
        df, count = DataQualityChecker._check_suspicious_values(df)
        issues['suspicious_values'] = count
        
        DataQualityChecker._log_issues(issues)
        return df

    @staticmethod
    def _check_missing_customer(df):
        missing = df['customer_id'].isna()
        count = missing.sum()
        if count > 0:
            df['has_missing_customer'] = missing
            df['customer_id'] = df['customer_id'].fillna('Unknown')
            logger.warning(f"Fixed {count} missing customer IDs")
        return df, count

    @staticmethod
    def _check_negative_quantities(df):
        negative = df['quantity'] < 0
        count = negative.sum()
        if count > 0:
            df['had_negative_quantity'] = negative
            df['quantity'] = df['quantity'].abs()
            logger.warning(f"Fixed {count} negative quantities")
        return df, count

    @staticmethod
    def _check_duplicate_transactions(df):
        duplicates = df['transaction_id'].duplicated(keep=False)
        count = duplicates.sum()
        if count > 0:
            logger.error(f"Found {count} duplicate transaction IDs")
        return count

    @staticmethod
    def _check_missing_product_info(df):
        missing = df[['product_id', 'product_name', 'price']].isna().any(axis=1)
        count = missing.sum()
        if count > 0:
            logger.error(f"Found {count} records with missing product info")
        return count

    @staticmethod
    def _check_invalid_prices(df):
        invalid = (df['price'] <= 0) | df['price'].isna()
        count = invalid.sum()
        if count > 0:
            logger.error(f"Found {count} invalid prices")
        return count

    @staticmethod
    def _check_date_issues(df):
        issues = df['date_std'].isna() & df['date'].notna()
        count = issues.sum()
        if count > 0:
            df['had_date_format_issue'] = issues
            logger.warning(f"Found {count} date format issues")
        return df, count

    @staticmethod
    def _check_missing_transaction_ids(df):
        missing = df['transaction_id'].isna()
        count = missing.sum()
        if count > 0:
            logger.error(f"Found {count} missing transaction IDs")
        return count

    @staticmethod
    def _check_suspicious_values(df):
        """Check for suspicious business values"""
        suspicious = (
            (df['quantity'] > 1000) |  # High quantities
            (df['price'] < 0.01) |     # Very low prices
            (df['total_value'] > 100000)  # High value transactions
        )
        count = suspicious.sum()
        if count > 0:
            df['has_suspicious_values'] = suspicious
            logger.warning(f"Found {count} records with suspicious business values")
        return df, count

    @staticmethod
    def _log_issues(issues):
        if any(issues.values()):
            logger.info("Data Quality Issues Summary:")
            for issue, count in issues.items():
                if count > 0:
                    logger.info(f"- {issue.replace('_', ' ').title()}: {count}")
        else:
            logger.info("No data quality issues found")

# ----------------------------
# Batch Data Loading with pandas to_sql
# ----------------------------
# ----------------------------
# Data Quality Checks
# ----------------------------
# ********* Main Requirements of Part 4) a) Batch processing to load json data into the database ********************
# **********************************************************
# **********************************************************
class BatchDataLoader:
    @staticmethod
    def load_with_batch_processing(df, connection_params, batch_size=1000):
        """Load data using pandas to_sql with batch processing"""
        logger.info(f"Loading data with batch processing (batch_size={batch_size})...")
        
        # Create SQLAlchemy engine
        engine = DatabaseManager.create_sqlalchemy_engine(**connection_params)
        if not engine:
            return False

        try:
            # Prepare separate DataFrames for each table
            customers_df = BatchDataLoader._prepare_customers_df(df)
            products_df = BatchDataLoader._prepare_products_df(df)
            transactions_df = BatchDataLoader._prepare_transactions_df(df)
            
            # Load data in batches
            BatchDataLoader._load_table_in_batches(customers_df, 'customers', engine, batch_size)
            BatchDataLoader._load_table_in_batches(products_df, 'products', engine, batch_size)
            BatchDataLoader._load_table_in_batches(transactions_df, 'transactions', engine, batch_size)
            
            logger.info("Batch loading completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Batch loading error: {str(e)}")
            return False
        finally:
            engine.dispose()

    @staticmethod
    def _prepare_customers_df(df):
        """Prepare customers dataframe"""
        customers_df = df[['customer_id']].dropna().drop_duplicates().reset_index(drop=True)
        logger.info(f"Prepared {len(customers_df)} unique customers for loading")
        return customers_df

    @staticmethod
    def _prepare_products_df(df):
        """Prepare products dataframe"""
        products_df = df[['product_id', 'product_name', 'category', 'price']].drop_duplicates().reset_index(drop=True)
        # Handle NaN values
        products_df = products_df.dropna(subset=['product_id'])
        logger.info(f"Prepared {len(products_df)} unique products for loading")
        return products_df

    @staticmethod
    def _prepare_transactions_df(df):
        """Prepare transactions dataframe"""
        transactions_df = df[[
            'transaction_id', 'customer_id', 'product_id', 'quantity',
            'date', 'date_std', 'region', 'total_value',
            'has_missing_customer', 'had_negative_quantity',
            'had_date_format_issue', 'has_suspicious_values'
        ]].copy()
        
        # Convert date_std to proper datetime format for MySQL
        transactions_df['date_std'] = pd.to_datetime(transactions_df['date_std'], errors='coerce')
        
        logger.info(f"Prepared {len(transactions_df)} transactions for loading")
        return transactions_df

    @staticmethod
    def _load_table_in_batches(df, table_name, engine, batch_size):
        """Load dataframe to database table in batches"""
        if df.empty:
            logger.warning(f"No data to load for table {table_name}")
            return

        total_rows = len(df)
        batches = (total_rows + batch_size - 1) // batch_size
        
        logger.info(f"Loading {total_rows} rows to {table_name} in {batches} batches")
        
        for i in range(0, total_rows, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            
            try:
                batch_df.to_sql(
                    name=table_name,
                    con=engine,
                    if_exists='append',
                    index=False,
                    method='multi'  # Use multi-row INSERT for better performance
                )
                logger.info(f"Loaded batch {batch_num}/{batches} for {table_name}")
                
            except Exception as e:
                logger.error(f"Error loading batch {batch_num} for {table_name}: {str(e)}")
                raise

# ----------------------------
# Analytics Query Runner
# ----------------------------
class AnalyticsRunner:
    @staticmethod
    def run_analytics_queries(connection_params):
        """Run the analytical queries and display results"""
        engine = DatabaseManager.create_sqlalchemy_engine(**connection_params)
        if not engine:
            return False

        try:
            logger.info("Running analytical queries...")
            
            # Query 1: Regional Sales Summary
            logger.info("\n=== QUERY 1: Regional Sales Summary ===")
            query1 = """
            WITH filtered_transactions AS (
                SELECT region, total_values
                FROM transactions
                WHERE total_value > 0
            )
            SELECT region, SUM(total_value) AS total_sales
            FROM filtered_transactions
            GROUP BY region
            ORDER BY total_sales DESC
            """
            result1 = pd.read_sql(query1, engine)
            print(result1.to_string(index=False))
            
            # Query 2: Top 5 Products by Sales
            logger.info("\n=== QUERY 2: Top 5 Products by Sales ===")
            query2 = """
            SELECT 
                p.product_id,
                p.product_name,
                SUM(t.total_value) AS total_sales
            FROM transactions t
            JOIN products p ON t.product_id = p.product_id
            GROUP BY p.product_id, p.product_name
            ORDER BY total_sales DESC
            LIMIT 5
            """
            result2 = pd.read_sql(query2, engine)
            print(result2.to_string(index=False))
            
            # Query 3: Monthly Sales Trends
            logger.info("\n=== QUERY 3: Monthly Sales Trends ===")
            query3 = """
            SELECT 
                DATE_FORMAT(date_std, '%Y-%m') AS month,
                SUM(total_value) AS monthly_sales
            FROM transactions
            WHERE date_std IS NOT NULL
            GROUP BY month
            ORDER BY month
            """
            result3 = pd.read_sql(query3, engine)
            print(result3.to_string(index=False))
            
            logger.info("Analytics queries completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Analytics query error: {str(e)}")
            return False
        finally:
            engine.dispose()

# ----------------------------
# Main ETL Pipeline
# ----------------------------
class ETLPipeline:
    @staticmethod
    def run(input_file, db_params, batch_size=1000, run_analytics=True):
        logger.info("Starting Enhanced ETL Pipeline with Batch Processing")
        
        # Create database schema first
        logger.info("Setting up database schema and indexes...")
        if not DatabaseManager.create_schema_and_indexes(db_params):
            logger.error("Schema setup failed - pipeline aborted")
            return False
        
        # Extract
        data = DataExtractor.from_json(input_file)
        if not data:
            logger.error("Extraction failed - pipeline aborted")
            return False
        
        # Transform
        df = DataTransformer.transform(data)
        if df.empty:
            logger.error("Transformation failed - pipeline aborted")
            return False
        
        # Load with batch processing
        success = BatchDataLoader.load_with_batch_processing(df, db_params, batch_size)
        if not success:
            logger.error("Batch loading failed - pipeline aborted")
            return False
        
        # Run analytics queries
        if run_analytics:
            logger.info("Running analytical queries...")
            AnalyticsRunner.run_analytics_queries(db_params)
        
        logger.info("Enhanced ETL pipeline completed successfully")
        return True

# ----------------------------
# Entry Point
# ----------------------------
if __name__ == '__main__':
    # Use os.path.join for constructing file paths (more portable)
    INPUT_FILE = os.path.join("..", "data", "sales_data.json")
    DB_PARAMS = {
        'host': 'localhost',
        'database': 'sales_db',
        'user': 'user',
        'password': 'pass'  # Add your MySQL password here
    }
    BATCH_SIZE = 1000  # Adjust based on your system's memory and performance needs
    
    ETLPipeline.run(INPUT_FILE, DB_PARAMS, BATCH_SIZE, run_analytics=True)