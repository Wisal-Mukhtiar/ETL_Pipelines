# ETL Pipeline and Dashboard Project

This repository contains the implementation of a complete data pipeline, from basic ETL to advanced processing and dashboard visualization. The project is divided into five parts, each organized in a separate folder for clarity. Additional folders are provided for dataset creation and data storage.

## Repository Structure

├── Part1/
│ └── basic_etl.py
├── Part2/
│ └── db_design_and_queries.sql
├── Part3_Part4/
│ ├── advanced_etl_pipeline.py
│ └── optimized_queries.sql
├── Part5/
│ ├── dashboard.pbix
│ └── dashboard_explanation.pdf
├── Dataset_Creator/
│ └── scale_dataset.py
├── Data/
│ └── [dataset files]
└── README.md


## Part 1: Basic ETL

This part includes a single Python script (`basic_etl.py`) implementing a basic ETL pipeline as described in the Data Pipeline Development section. It covers the initial extraction, transformation, and loading of the dataset into a target structure.

## Part 2: Database Design and Queries

This part contains a SQL file (`db_design_and_queries.sql`) that:

- Defines the database schema.
- Loads data from the output generated in Part 1.
- Includes queries to manipulate and validate the data.

## Part 3 & Part 4: Advanced ETL and Query Optimization

Both parts are combined in one folder for simplicity.

- `advanced_etl_pipeline.py` implements an advanced ETL pipeline that:
  - Processes batch JSON data.
  - Performs data quality checks before loading.
- `optimized_queries.sql` enhances database performance by:
  - Creating additional indexes.
  - Providing optimized queries for faster access and reporting.

## Part 5: Dashboard Design

This part includes:

- `dashboard.pbix`: A Power BI dashboard file.
- `dashboard_explanation.pdf`: A document explaining the design choices, layout, and functionality of the dashboard along with screenshots.

## Additional Folders

- `Dataset_Creator/`: Contains a Python script (`scale_dataset.py`) used to scale or generate the dataset used throughout the project.
- `Data/`: Contains the dataset files used by the ETL pipeline.

## Notes

- Code and SQL scripts are commented for better readability and understanding.
- Feel free to reach out if any part needs further explanation or clarification.
