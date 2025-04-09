from spark_session import SparkManager
from data_validator import DataValidator
from report_generator import ReportGenerator
from config import Config
import os

def main():
    """
    Executes the complete employee data processing pipeline:
        1. Sets up output directories
        2. Establishes MySQL connection and loads data
        3. Validates and cleans data
        4. Saves cleaned data in Parquet format
        5. Generates analysis and reports
        6. Terminates the Spark session
    """
    # create output directory if not exist
    tables_dir = os.path.join(Config.OUTPUT_DIR, "tables")
    os.makedirs(tables_dir, exist_ok=True)
    reports_dir = os.path.join(Config.OUTPUT_DIR, "reports")
    os.makedirs(reports_dir, exist_ok=True)
    
    # new spark session, connect to Mysql and load dataframes
    spark_manager = SparkManager(
        mysql_host="employees_db",
        mysql_port="3306",
        mysql_db="employees",
        mysql_user="root",
        mysql_password="password"
    )
    dataframes = spark_manager.load_tables(Config.TABLES)
    

    validator = DataValidator()
    
    print("\n=== Data Validation ===")
    validator.validate(
        dataframes, 
        Config.PK_COLUMNS,
        Config.DATE_TABLES,
        Config.TABLE_SCHEMAS
    )
    
    print("\n=== Data Cleaning ===")
    cleaned_dataframes = validator.clean(
        dataframes, 
        Config.PK_COLUMNS, 
        Config.TABLE_SCHEMAS
    )
    
    print("\n=== Saving Cleaned Data ===")
    for table, df in cleaned_dataframes.items():
        output_path = f"{tables_dir}/{table}.parquet"
        print(f"Saving cleaned {table} to {output_path}")
        df.write.mode("overwrite").parquet(output_path)

    print("\n=== Generating Reports ===")
    report_generator = ReportGenerator(spark_manager.spark, reports_dir)
    
    report_generator.generate_annual_salary_by_department(
        cleaned_dataframes['dept_emp'], 
        cleaned_dataframes['departments'], 
        cleaned_dataframes['salaries']
    )
    
    report_generator.identify_job_hoppers(
        cleaned_dataframes['dept_emp'], 
        cleaned_dataframes['employees']
    )

    print("\n=== Generating Additional Reports ===")
    report_generator.generate_employee_master_report(
        cleaned_dataframes['employees'],
        cleaned_dataframes['dept_emp'], 
        cleaned_dataframes['titles'],
        cleaned_dataframes['salaries'],
        cleaned_dataframes['departments']
    )

    report_generator.generate_changes_report(
        cleaned_dataframes['employees'],
        cleaned_dataframes['dept_emp'],
        cleaned_dataframes['titles'],
        cleaned_dataframes['salaries'],
        cleaned_dataframes['dept_manager'],
        cleaned_dataframes['departments']
    )
    
    spark_manager.stop_session()
    print("\nData processing completed successfully")

if __name__ == "__main__":
    main()