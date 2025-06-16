import json
import random
from datetime import datetime, timedelta
import sys

def load_sample_data(file_path='sales_data.json'):
    """
    Load sample data from a JSON file.
    Exit if the file doesn't exist.
    """
    try:
        with open(file_path, 'r') as f:
            sample_data = json.load(f)
            print(f"Successfully loaded {len(sample_data)} records from {file_path}")
            return sample_data
    except FileNotFoundError:
        print(f"Error: Input file '{file_path}' not found.")
        print(f"Please create '{file_path}' with your sample data first.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: Could not parse '{file_path}' as valid JSON.")
        sys.exit(1)

def generate_sales_dataset(num_records=500, input_file='sales_data.json'):
    """
    Generate a synthetic sales dataset with deliberate data quality issues,
    continuing from the input file format and IDs.
    """
    # Load sample data from file (required)
    sample_data = load_sample_data(input_file)
    
    # Extract product data from original dataset
    original_products = []
    for item in sample_data:
        if "product" in item and isinstance(item["product"], dict):
            product = item["product"].copy()
            if product not in original_products:
                original_products.append(product)
    
    # Add a few more product variations while preserving the original format
    additional_products = [
        {"id": "P04", "name": "Keyboard", "category": "Accessories", "price": 49.99},
        {"id": "P05", "name": "Headphones", "category": "Audio", "price": 129.99},
        {"id": "P06", "name": "Smartphone", "category": "Electronics", "price": 799.99},
        {"id": "P07", "name": "Tablet", "category": "Electronics", "price": 499.99},
        {"id": "P08", "name": "USB Drive", "category": "Storage", "price": 15.99},
        {"id": "P09", "name": "External HDD", "category": "Storage", "price": 89.99},
        {"id": "P10", "name": "Camera", "category": "Electronics", "price": 349.99},
        {"id": "P11", "name": "Printer", "category": "Office", "price": 199.99},
        {"id": "P12", "name": "Speaker", "category": "Audio", "price": 79.99},
        {"id": "P13", "name": "Router", "category": "Networking", "price": 59.99}
    ]
    
    all_products = original_products + additional_products
    
    # Extract regions from original dataset
    regions = set()
    for item in sample_data:
        if "region" in item and item["region"]:
            regions.add(item["region"])
    regions = list(regions) if regions else ["North", "South", "East", "West"]
    
    # Find the highest transaction ID to continue from
    last_transaction_id = "T000"
    for item in sample_data:
        if "transaction_id" in item and isinstance(item["transaction_id"], str):
            if item["transaction_id"].startswith("T") and item["transaction_id"] > last_transaction_id:
                last_transaction_id = item["transaction_id"]
    
    # Extract the numeric part and increment
    try:
        next_transaction_num = int(last_transaction_id[1:]) + 1
    except ValueError:
        next_transaction_num = 6  # Default to T006 if parsing fails
    
    # Extract customer IDs from original dataset
    customer_ids = set()
    for item in sample_data:
        if "customer_id" in item and item["customer_id"]:
            customer_ids.add(item["customer_id"])
    
    customer_list = [f"C{random.randint(1, 100):03d}" for _ in range(30)]
    customer_ids = list(customer_ids) if customer_ids else customer_list
    
    # Start date for data generation
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)
    
    # Initialize result list with original data
    result = list(sample_data)  # Start with the original data
    
    # Generate additional random data
    for i in range(num_records - len(sample_data)):
        # Determine if this should be a record with data issues
        has_issues = random.random() < 0.2  # 20% chance of issues
        
        # Create transaction ID continuing from the last one
        transaction_id = f"T{str(next_transaction_num).zfill(3)}"
        next_transaction_num += 1
        
        # Customer ID (includes missing values)
        if has_issues and random.random() < 0.4:  # 40% of issue records have null customer_id
            customer_id = None
        else:
            customer_list = [f"C{random.randint(1, 100):03d}" for _ in range(30)]
            customer_id = random.choice(customer_list)
        
        # Product (from original or added products)
        product = random.choice(all_products).copy()
        
        # Quantity (includes negative values for some records)
        if has_issues and random.random() < 0.3:  # 30% of issue records have negative quantity
            quantity = random.randint(-10, -1)
        else:
            quantity = random.randint(1, 20)
        
        # Date (includes inconsistent formats)
        random_days = random.randint(0, (end_date - start_date).days)
        record_date = start_date + timedelta(days=random_days)
        
        if has_issues and random.random() < 0.5:  # 50% of issue records have inconsistent date format
            date_formats = [
                "%Y-%m-%d",          # 2023-01-15
                "%d/%m/%Y",          # 15/01/2023
                "%m/%d/%Y",          # 01/15/2023
                "%Y %m %d"           # 2023 01 15
            ]
            date_str = record_date.strftime(random.choice(date_formats))
        else:
            date_str = record_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Region
        region = random.choice(regions)
        
        # Create record
        record = {
            "transaction_id": transaction_id,
            "customer_id": customer_id,
            "product": product,
            "quantity": quantity,
            "date": date_str,
            "region": region
        }
        
        result.append(record)
    
    return result

def main():
    import argparse
    
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description='Generate synthetic sales data with specific quality issues for cleaning practice')
    parser.add_argument('--input', '-i', type=str, default='sales_data.json', 
                        help='Input JSON file with sample data (default: sales_data.json)')
    parser.add_argument('--output', '-o', type=str, default='sales_data_expanded.json',
                        help='Output file name (default: sales_data_expanded.json)')
    parser.add_argument('--count', '-c', type=int, default=500,
                        help='Total number of records to generate, including original samples (default: 500)')
    
    args = parser.parse_args()
    
    # Generate dataset with specified parameters
    dataset = generate_sales_dataset(num_records=args.count, input_file=args.input)
    
    # Display example of new records
    orig_count = 0
    with open(args.input, 'r') as f:
        orig_count = len(json.load(f))
    
    print(f"\nOriginal record count: {orig_count}")
    print(f"New records generated: {len(dataset) - orig_count}")
    print(f"Total records: {len(dataset)}")
    
    print(f"\nSample of new records:")
    for i, record in enumerate(dataset[orig_count:orig_count+3]):
        print(f"New Record {i+1}:", json.dumps(record, indent=2))
        
    # Save to file
    output_path = args.output
    with open(output_path, "w") as f:
        json.dump(dataset, f, indent=2)
    
    print(f"\nGenerated {len(dataset)} total records and saved to '{output_path}'")
    
    # Count data quality issues for verification
    null_customer_ids = sum(1 for record in dataset if record.get("customer_id") is None)
    negative_quantities = sum(1 for record in dataset if isinstance(record.get("quantity"), int) and record["quantity"] < 0)
    non_standard_dates = sum(1 for record in dataset if "Z" not in str(record.get("date", "")))
    
    print("\nData quality issues summary:")
    print(f"- Records with missing customer_id: {null_customer_ids}")
    print(f"- Records with negative quantity: {negative_quantities}")
    print(f"- Records with non-standard date format: {non_standard_dates}")
    print(f"- Records with nested product data: {len(dataset)}")  # All records have nested product data

if __name__ == "__main__":
    main()