from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, year, count, countDistinct
)

class ReportGenerator:
    def __init__(self, spark_session: SparkSession, output_dir):
        """
        Initialize ReportGenerator with Spark session and output directory
        
        :param spark_session: Active SparkSession
        :param output_dir: Directory to save reports
        """
        self.spark = spark_session
        self.output_dir = output_dir
    
    def generate_annual_salary_by_department(self, dept_emp_df, departments_df, salaries_df):
        annual_costs = (
            salaries_df.alias("s")
            .join(
                dept_emp_df.alias("de"),
                (col("s.emp_no") == col("de.emp_no")) &
                (col("s.from_date") < col("de.to_date")) &
                (col("s.to_date") > col("de.from_date"))
            )
            .join(
                departments_df.alias("d"),
                col("de.dept_no") == col("d.dept_no")
            )
            .withColumn("year", year(col("s.from_date")))
            .groupBy("year", "d.dept_name")
            .agg(
                sum("s.salary").alias("total_salary"),
                countDistinct("s.emp_no").alias("employee_count")
            )
            .orderBy("year", "d.dept_name")
        )
        

        output_path = f"{self.output_dir}/annual_salary_by_department"
        (
            annual_costs.coalesce(1)
            .write.mode("overwrite")
            .option("header", "true")
            .csv(output_path)

        )
        
        return annual_costs
        
    def identify_job_hoppers(self, dept_emp_df, employees_df):
        dept_changes = (
            dept_emp_df.groupBy("emp_no")
            .agg(count("dept_no").alias("dept_change_count"))
            .filter(col("dept_change_count") > 3)
        )
        
        job_hoppers = (
            dept_changes
            .join(employees_df, "emp_no")
            .select(
                "emp_no", 
                col("first_name"), 
                col("last_name"), 
                "dept_change_count"
            )
            .orderBy(col("dept_change_count").desc())
        )

        output_path = f"{self.output_dir}/job_hoppers"
        (
            job_hoppers.coalesce(1)
            .write.mode("overwrite")
            .option("header", "true")
            .csv(output_path)
        )

        return job_hoppers