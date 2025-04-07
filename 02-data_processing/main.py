from spark_session import SparkManager
from data_validator import DataValidator
from config import Config
import os

def main():
    # create output directory if not exist
    os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
    
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
        output_path = f"{Config.OUTPUT_DIR}/{table}.parquet"
        print(f"Saving cleaned {table} to {output_path}")
        df.write.mode("overwrite").parquet(output_path)
    
    spark_manager.stop_session()
    print("\nData processing completed successfully")

if __name__ == "__main__":
    main()