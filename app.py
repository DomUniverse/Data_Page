import os
import streamlit as st
import pandas as pd
import sweetviz as sv
import tempfile

from utils import (
    load_csv_file,
    generate_profiling_report,
    execute_duckdb_query,
    get_table_schema,
    get_sample_queries
)

st.set_page_config(
    page_title="Data Catalogue",
    page_icon="📊",
    layout="wide"
)

def main():
    st.title("CSV Catalogue and Analysis Tool")
    st.sidebar.header("CSV Data Analysis")
    
    # File upload section
    st.sidebar.subheader("Upload CSV File") 
    uploaded_file = st.sidebar.file_uploader("Choose a CSV file", type=["csv"])
    
    if uploaded_file is not None:
        # Save the uploaded file to a temporary location
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_path = tmp_file.name
        
        # Load the CSV file
        df = load_csv_file(tmp_path)
        
        # Display basic information about the dataset
        st.header(f"Dataset: {uploaded_file.name}")
        
        # Overview tab
        tab1, tab2, tab3 = st.tabs(["Overview", "Data Profiling", "SQL Query"])
        
        with tab1:
            st.subheader("Data Preview")
            st.dataframe(df.head(10))
            
            st.subheader("Dataset Information")
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"**Rows:** {df.shape[0]}")
                st.write(f"**Columns:** {df.shape[1]}")
            with col2:
                st.write(f"**Memory Usage:** {df.memory_usage(deep=True).sum() / (1024*1024):.2f} MB")
                st.write(f"**Missing Values:** {df.isna().sum().sum()}")
            
            st.subheader("Column Information")
            col_info = pd.DataFrame({
                'Column': df.columns,
                'Type': df.dtypes.astype(str),
                'Non-Null Count': df.count().values,
                'Missing Values': df.isna().sum().values,
                'Missing %': (df.isna().sum().values / len(df) * 100).round(2)
            })
            st.dataframe(col_info)
        
        # Data Profiling tab
        with tab2:
            st.subheader("Data Profiling with Sweetviz")
            
            if st.button("Generate Profiling Report"):
                with st.spinner("Generating report..."):
                    report_path = generate_profiling_report(df, uploaded_file.name)
                    
                    # Display the report in an iframe
                    if report_path:
                        report_dir = os.path.dirname(report_path)
                        report_filename = os.path.basename(report_path)
                        st.components.v1.html(open(report_path, 'r', encoding='utf-8').read(), height=600)
                        st.success(f"Report generated successfully!")
        
        # SQL Query tab
        with tab3:
            st.subheader("Query Data with SQL")
            
            # Display the table schema
            schema = get_table_schema(df)
            with st.expander("Table Schema"):
                st.code(schema)
            
            # Sample queries
            sample_queries = get_sample_queries(df.columns.tolist(), uploaded_file.name.split('.')[0])
            with st.expander("Sample Queries"):
                for i, (title, query) in enumerate(sample_queries):
                    st.write(f"**{title}**")
                    st.code(query)
            
            # Custom query input
            custom_query = st.text_area("Enter your SQL query:", 
                                       f"SELECT * FROM '{uploaded_file.name.split('.')[0]}' LIMIT 10")
            
            if st.button("Execute Query"):
                try:
                    with st.spinner("Executing query..."):
                        result_df = execute_duckdb_query(df, custom_query, uploaded_file.name.split('.')[0])
                        st.subheader("Query Result")
                        st.dataframe(result_df)
                        st.success(f"Query executed successfully! Result has {result_df.shape[0]} rows and {result_df.shape[1]} columns.")
                except Exception as e:
                    st.error(f"Error executing query: {str(e)}")
        
        # Clean up the temporary file
        os.unlink(tmp_path)
    
    else:
        # Show a welcome message and instructions
        st.info("👈 Upload a CSV file using the sidebar to start analyzing your data!")
        st.write("""
        ## Universe Data Catalogue!
        
        This tool allows you to:
        
        1. **Upload** and preview CSV files
        2. **Profile** your data with automated reports
        3. **Query** your data using SQL with DuckDB
        
        Get started by uploading a CSV file in the sidebar.
        """)

if __name__ == "__main__":
    main()
