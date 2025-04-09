from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, sum, year, count, countDistinct, 
    first, concat, lit, when, datediff, round, lag, udf
)
from pyspark.sql.types import StringType

class ReportGenerator:
    def __init__(self, spark_session: SparkSession, output_dir):
        """
        Initialize ReportGenerator with Spark session and output directory
        
        :param spark_session: Active SparkSession
        :param output_dir: Directory to save reports
        """
        self.spark = spark_session
        self.output_dir = output_dir

    def save_report_csv(self, output_path, df):
        if df.count() > 0:
            (
                df.coalesce(1)
                .write.mode("overwrite")
                .option("header", "true")
                .csv(output_path)
            )
        else:
            print(f"Skipping CSV export for {output_path} - DataFrame is empty")
    
    def generate_annual_salary_by_department(self, dept_emp_df, departments_df, salaries_df):
        end_date = "2002-12-31"
        
        dept_emp_fixed = dept_emp_df.withColumn(
            "to_date",
            when(col("to_date") == "9999-01-01", lit(end_date)).otherwise(col("to_date"))
        )
        
        salaries_fixed = salaries_df.withColumn(
            "to_date",
            when(col("to_date") == "9999-01-01", lit(end_date)).otherwise(col("to_date"))
        )
        
        overlap_df = (
            salaries_fixed.alias("s")
            .join(
                dept_emp_fixed.alias("de"),
                (col("s.emp_no") == col("de.emp_no")) &
                (col("s.from_date") < col("de.to_date")) &
                (col("s.to_date") > col("de.from_date"))
            )
            .select(
                col("s.emp_no"),
                col("s.salary"),
                col("de.dept_no"),
                when(col("s.from_date") > col("de.from_date"), col("s.from_date"))
                    .otherwise(col("de.from_date")).alias("overlap_start"),
                when(col("s.to_date") < col("de.to_date"), col("s.to_date"))
                    .otherwise(col("de.to_date")).alias("overlap_end")
            )
        )
        
        overlap_with_proration = (
            overlap_df
            .withColumn("overlap_days", datediff(col("overlap_end"), col("overlap_start")))
            .withColumn("daily_rate", col("salary") / 365)
            .withColumn("prorated_salary", col("daily_rate") * col("overlap_days"))
            .withColumn("year", year(col("overlap_start")))
        )
        
        annual_costs = (
            overlap_with_proration
            .join(departments_df.alias("d"), "dept_no")
            .groupBy("year", "d.dept_name")
            .agg(
                round(sum("prorated_salary"), 0).cast("long").alias("total_salary"),
                countDistinct("emp_no").alias("employee_count")
            )
            .orderBy("year", "d.dept_name")
        )
        
        output_path = f"{self.output_dir}/annual_salary_by_department"
        self.save_report_csv(output_path, annual_costs)
        
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
        self.save_report_csv(output_path, job_hoppers)

        return job_hoppers
    
    def generate_employee_master_report(self, 
                                        employees_df, 
                                        dept_emp_df, 
                                        titles_df, 
                                        salaries_df, 
                                        departments_df):
        
        current_window = Window.partitionBy("emp_no").orderBy(col("from_date").desc())

        current_dept = (
            dept_emp_df.filter(col("to_date") == "9999-01-01")
            .withColumn("rank", first("dept_no").over(current_window))
            .filter(col("rank") == col("dept_no"))
            .select("emp_no", "dept_no")
        )

        current_title = (
            titles_df.filter(col("to_date") == "9999-01-01")
            .withColumn("rank", first("title").over(current_window))
            .filter(col("rank") == col("title"))
            .select("emp_no", "title")
        )

        current_salary = (
            salaries_df.filter(col("to_date") == "9999-01-01")
            .withColumn("rank", first("salary").over(current_window))
            .filter(col("rank") == col("salary"))
            .select("emp_no", "salary")
        )

        employee_master = (
            employees_df
            .join(current_dept, "emp_no")
            .join(departments_df, "dept_no")
            .join(current_title, "emp_no")
            .join(current_salary, "emp_no")
            .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
            .withColumn("tenure_years",
                round(datediff(lit("2002-12-31"), col("hire_date")) / 365.25, 0).cast("int"))
            .select(
                "emp_no", 
                "full_name", 
                col("dept_name").alias("current_department"),
                col("title").alias("current_title"),
                col("salary").alias("current_salary"),
                "hire_date",
                "tenure_years"
            )
        )

        output_path = f"{self.output_dir}/employee_master"
        employee_master.write.mode("overwrite").parquet(output_path)
        self.save_report_csv(output_path+"_csv", employee_master)
        
        return employee_master

    def generate_changes_report(self,
                          employees_df,
                          dept_emp_df,
                          titles_df,
                          salaries_df,
                          dept_manager_df,
                          departments_df,
                          start_date='2000-01-01',
                          end_date='2000-06-30'):
    
        window_spec = Window.partitionBy("emp_no").orderBy("from_date")
        dept_window = Window.partitionBy("dept_no").orderBy("from_date")
        
        dept_changes = (
            dept_emp_df
            .withColumn("prev_dept_no", lag("dept_no", 1).over(window_spec))
            .withColumn("is_change", col("dept_no") != col("prev_dept_no"))
            .filter(col("from_date").between(start_date, end_date) & col("is_change"))
            .select(
                col("emp_no"),
                col("from_date").alias("change_date"),
                col("prev_dept_no").alias("old_value"),
                col("dept_no").alias("new_value"),
                lit("Department Change").alias("change_type")
            )
        )
        
        title_changes = (
            titles_df
            .withColumn("prev_title", lag("title", 1).over(window_spec))
            .withColumn("is_change", col("title") != col("prev_title"))
            .filter(col("from_date").between(start_date, end_date) & col("is_change"))
            .select(
                col("emp_no"),
                col("from_date").alias("change_date"),
                col("prev_title").alias("old_value"),
                col("title").alias("new_value"),
                lit("Title Change").alias("change_type")
            )
        )
        
        salary_changes = (
            salaries_df
            .withColumn("prev_salary", lag("salary", 1).over(window_spec))
            .withColumn("is_change", col("salary") != col("prev_salary"))
            .filter(col("from_date").between(start_date, end_date) & col("is_change"))
            .select(
                col("emp_no"),
                col("from_date").alias("change_date"),
                col("prev_salary").cast("string").alias("old_value"),
                col("salary").cast("string").alias("new_value"),
                lit("Salary Change").alias("change_type")
            )
        )
        
        manager_changes = (
            dept_manager_df
            .withColumn("prev_emp_no", lag("emp_no", 1).over(dept_window))
            .withColumn("is_change", col("emp_no") != col("prev_emp_no"))
            .filter(col("from_date").between(start_date, end_date) & col("is_change"))
            .select(
                col("dept_no"),
                col("from_date").alias("change_date"),
                col("prev_emp_no").alias("old_value"),
                col("emp_no").alias("new_value"),
                lit("Manager Change").alias("change_type")
            )
        )
        
        employee_changes = (
            dept_changes.union(title_changes).union(salary_changes)
            .join(
                employees_df.select(
                    "emp_no", 
                    concat(col("first_name"), lit(" "), col("last_name")).alias("entity_name")
                ),
                "emp_no"
            )
            .select(
                col("emp_no").alias("entity_id"),
                col("entity_name"),
                "change_date",
                "change_type",
                "old_value",
                "new_value"
            )
        )
        
        dept_manager_changes = (
            manager_changes
            .join(
                departments_df.select(
                    "dept_no", 
                    col("dept_name").alias("entity_name")
                ),
                "dept_no"
            )
            .select(
                col("dept_no").alias("entity_id"),
                col("entity_name"),
                "change_date",
                "change_type",
                "old_value",
                "new_value"
            )
        )
        
        # combine changes
        all_changes = (
            employee_changes.union(dept_manager_changes)
            .orderBy("change_date")
        )
        
        # replace manager employee IDs with names
        all_changes_with_names = all_changes
        
        if all_changes.filter(col("change_type") == "Manager Change").count() > 0:
            emp_names_map = employees_df.select(
                col("emp_no").cast("string"), 
                concat(col("first_name"), lit(" "), col("last_name")).alias("name")
            ).rdd.collectAsMap()
            
            # UDF to get employee name from ID
            get_name_udf = udf(lambda emp_id: emp_names_map.get(emp_id), StringType())
            
            all_changes_with_names = all_changes.withColumn(
                "old_value", 
                when(col("change_type") == "Manager Change", get_name_udf(col("old_value"))).otherwise(col("old_value"))
            ).withColumn(
                "new_value", 
                when(col("change_type") == "Manager Change", get_name_udf(col("new_value"))).otherwise(col("new_value"))
            )
        
        output_path = f"{self.output_dir}/employee_changes"
        self.save_report_csv(output_path, all_changes_with_names)
        
        return all_changes_with_names