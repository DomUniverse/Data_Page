import os
import pandas as pd
import sweetviz as sv
import duckdb
import tempfile
from pathlib import Path

def load_csv_file(file_path):
    """
    Load a CSV file into a pandas DataFrame.
    
    Args:
        file_path (str): Path to the CSV file
        
    Returns:
        pandas.DataFrame: Loaded data
    """
    try:
        # Try to automatically detect encoding and separator
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        # If automatic detection fails, try common encodings and separators
        encodings = ['utf-8', 'latin1', 'iso-8859-1']
        separators = [',', ';', '\t', '|']
        
        for encoding in encodings:
            for sep in separators:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, sep=sep)
                    return df
                except:
                    continue
        
        # If all attempts fail, raise the original exception
        raise e

def generate_profiling_report(df, filename):
    """
    Generate a Sweetviz profiling report for the DataFrame.
    
    Args:
        df (pandas.DataFrame): The DataFrame to profile
        filename (str): Name of the file for the report
        
    Returns:
        str: Path to the generated report file
    """
    try:
        # Create a report object
        report = sv.analyze(df)
        
        # Create a temporary directory for the report
        report_dir = tempfile.mkdtemp()
        report_name = f"{os.path.splitext(filename)[0]}_report.html"
        report_path = os.path.join(report_dir, report_name)
        
        # Save the report
        report.show_html(report_path, open_browser=False)
        
        return report_path
    except Exception as e:
        print(f"Error generating profiling report: {e}")
        return None

def execute_duckdb_query(df, query, table_name):
    """
    Execute a SQL query on a DataFrame using DuckDB.
    
    Args:
        df (pandas.DataFrame): The DataFrame to query
        query (str): SQL query to execute
        table_name (str): Name of the table to use in the query
        
    Returns:
        pandas.DataFrame: Query results
    """
    # Create a connection to an in-memory DuckDB database
    conn = duckdb.connect(database=':memory:')
    
    # Register the DataFrame as a table
    conn.register(table_name, df)
    
    # Execute the query and return the results as a DataFrame
    result_df = conn.execute(query).fetchdf()
    
    return result_df

def get_table_schema(df):
    """
    Generate a SQL-like schema representation of the DataFrame.
    
    Args:
        df (pandas.DataFrame): The DataFrame to get schema for
        
    Returns:
        str: SQL CREATE TABLE statement representing the schema
    """
    # Map pandas data types to SQL types
    type_mapping = {
        'int64': 'INTEGER',
        'float64': 'DOUBLE',
        'bool': 'BOOLEAN',
        'object': 'VARCHAR',
        'datetime64[ns]': 'TIMESTAMP',
    }
    
    # Generate column definitions
    columns = []
    for column, dtype in df.dtypes.items():
        sql_type = type_mapping.get(str(dtype), 'VARCHAR')
        columns.append(f"    \"{column}\" {sql_type}")
    
    # Create the schema string
    schema = "CREATE TABLE table_name (\n"
    schema += ",\n".join(columns)
    schema += "\n)"
    
    return schema

def get_sample_queries(columns, table_name):
    """
    Generate sample SQL queries based on the DataFrame columns.
    
    Args:
        columns (list): List of column names
        table_name (str): Name of the table to use in the queries
        
    Returns:
        list: List of tuples containing (query title, query text)
    """
    queries = []
    
    # Select all columns
    queries.append(("Select all data", f"SELECT * FROM '{table_name}'"))
    
    # Count rows
    queries.append(("Count rows", f"SELECT COUNT(*) AS row_count FROM '{table_name}'"))
    
    # Select numeric columns if available
    numeric_columns = []
    for column in columns:
        if column.lower().find('id') >= 0 or any(word in column.lower() for word in ['price', 'amount', 'qty', 'count', 'number', 'total', 'sum']):
            numeric_columns.append(column)
    
    if numeric_columns:
        # Basic statistics for a numeric column
        col = numeric_columns[0]
        queries.append(("Basic statistics", 
                        f"""SELECT 
    MIN("{col}") AS min_value,
    MAX("{col}") AS max_value,
    AVG("{col}") AS avg_value,
    COUNT(*) AS count
FROM '{table_name}'"""))
        
        # Group by and count (if there are at least 2 columns)
        if len(columns) >= 2:
            group_col = columns[0] if columns[0] != col else columns[1]
            queries.append(("Group and count", 
                           f"""SELECT 
    "{group_col}",
    COUNT(*) AS count
FROM '{table_name}'
GROUP BY "{group_col}"
ORDER BY count DESC"""))
    
    # Filter example
    if len(columns) > 0:
        queries.append(("Filter data", 
                       f"""SELECT *
FROM '{table_name}'
WHERE "{columns[0]}" IS NOT NULL
LIMIT 100"""))
    
    return queries