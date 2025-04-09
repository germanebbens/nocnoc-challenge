from pyspark.sql.functions import col, when, count, isnull

class DataValidator:
    """
    Validates and cleans dataframes from employee database tables
    
    Provides methods to check for null values, duplicate primary keys, data 
    type consistency, and date range validity. Also handles data cleaning 
    operations, including type casting and duplicate removal.
    
    Note: This class does not require initialization since it doesnt 
    maintain any internal state and operates only with the provided parameters
    """

    def validate(self, dataframes, pk_columns, date_tables, schemas):
        validation_results = {}
        
        # for each table validate nulls, duplicates, data types and dates consistency
        for table, df in dataframes.items():
            table_results = {}
            
            table_results["nulls"] = self._check_nulls(df)
            
            if table in pk_columns:
                table_results["duplicates"] = self._check_duplicates(df, pk_columns[table])
            
            if table in schemas:
                table_results["types"] = self._check_data_types(df, schemas[table])
            
            if table in date_tables:
                table_results["date_consistency"] = self._check_date_consistency(df)
            
            validation_results[table] = table_results
            
            # print results
            self._print_validation_results(table, table_results)
        
        return validation_results
    
    def clean(self, dataframes, pk_columns, schemas):
        # for each table clean nulls, duplicates and fix data types if is possible
        for table, df in dataframes.items():
            if table in schemas:
                df = self._fix_data_types(df, schemas[table])
            
            df = self._handle_nulls(df)
            
            if table in pk_columns:
                count_before = df.count()
                df = df.dropDuplicates(pk_columns[table])
                count_after = df.count()
                if count_before != count_after:
                    print(f"Removed {count_before - count_after} duplicates from {table}")
            
            dataframes[table] = df
        
        return dataframes
    
    def _check_nulls(self, df):
        null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
        return null_counts.collect()[0]
    
    def _check_duplicates(self, df, pk_columns):
        duplicate_count = df.groupBy(pk_columns).count().filter("count > 1").count()
        return duplicate_count
    
    def _check_data_types(self, df, schema):
        type_issues = []
        for column_name, expected_type in schema.items():
            if column_name in df.columns:
                current_type = df.schema[df.schema.names.index(column_name)].dataType
                if type(current_type) != type(expected_type):
                    type_issues.append({
                        "column": column_name,
                        "current_type": str(current_type),
                        "expected_type": str(expected_type)
                    })
        return type_issues
    
    def _check_date_consistency(self, df):
        if "from_date" in df.columns and "to_date" in df.columns:
            invalid_count = df.filter(col("from_date") > col("to_date")).count()
            return invalid_count
        return
    
    def _fix_data_types(self, df, schema):
        for column_name, expected_type in schema.items():
            if column_name in df.columns:
                current_type = df.schema[df.schema.names.index(column_name)].dataType
                if type(current_type) != type(expected_type):
                    print(f"Different data type! Casting {column_name} from {current_type} to {expected_type}")
                    df = df.withColumn(column_name, df[column_name].cast(expected_type))
        return df
    
    def _handle_nulls(self, df):
        # Now, I'm only checking for nulls, 
        # not actually removing or replacing them,
        # doesn't have nulls according to previous validation
        return df
    
    def _print_validation_results(self, table, results):
        print(f"\nValidation results for {table}:")
        
        print(f"Null counts:")
        for col_name, count in results["nulls"].asDict().items():
            print(f"\t{col_name}: {count}")
        
        if "duplicates" in results:
            print(f"Duplicate rows: {results['duplicates']}")
        
        if "types" in results and results["types"]:
            print(f"Type issues:")
            for issue in results["types"]:
                print(f"\t{issue['column']}: {issue['current_type']} (expected {issue['expected_type']})")
        
        if "date_consistency" in results:
            print(f"Date inconsistencies: {results['date_consistency']}")