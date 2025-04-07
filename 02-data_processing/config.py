from pyspark.sql.types import IntegerType, StringType, DateType

class Config:
    PK_COLUMNS = {
        "employees": ["emp_no"],
        "departments": ["dept_no"],
        "dept_emp": ["emp_no", "dept_no", "from_date"],
        "dept_manager": ["emp_no", "dept_no", "from_date"],
        "titles": ["emp_no", "title", "from_date"],
        "salaries": ["emp_no", "from_date"]
    }
    
    DATE_TABLES = ["dept_emp", "dept_manager", "titles", "salaries"]

    TABLE_SCHEMAS = {
        "employees": {
            "emp_no": IntegerType(),
            "birth_date": DateType(),
            "first_name": StringType(),
            "last_name": StringType(),
            "gender": StringType(),
            "hire_date": DateType()
        },
        "departments": {
            "dept_no": StringType(),
            "dept_name": StringType()
        },
        "dept_emp": {
            "emp_no": IntegerType(),
            "dept_no": StringType(),
            "from_date": DateType(),
            "to_date": DateType()
        },
        "dept_manager": {
            "emp_no": IntegerType(),
            "dept_no": StringType(),
            "from_date": DateType(),
            "to_date": DateType()
        },
        "titles": {
            "emp_no": IntegerType(),
            "title": StringType(),
            "from_date": DateType(),
            "to_date": DateType()
        },
        "salaries": {
            "emp_no": IntegerType(),
            "salary": IntegerType(),
            "from_date": DateType(),
            "to_date": DateType()
        }
    }
    
    TABLES = list(TABLE_SCHEMAS.keys())
    
    OUTPUT_DIR = "/app/output"
