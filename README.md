# NocNoc Challenge

Data engineering solution for employee database analysis and processing using SQL, Python, Spark and Airflow.

Repo contains:
- [Challenge](/Challenge%20Data%20Engineer%20-%20NocNoc%20__.pdf): steps to complete challenge.
- Dockerfile & docker-compose.yml: docker config files to run database, load and process data.
- /01-sql folder: Contains SQL queries for step 1
- /02-data_processing: Python and PySpark scripts - step 2.1 to 2.3
  - config.py: Configuration settings for database schema and output paths
  - data_validator.py: Data cleaning and validation utilities
  - main.py: Entry point that orchestrates the entire ETL process
  - report_generator.py: Functions to create analytical reports from processed data
  - spark_session.py: Spark connection management and table loading
  - /output: Directory where processed data and reports are saved

## Database Setup

![Employees class diagram](/images/employees.png)

The project uses the [MySQL Employees Sample Database](https://github.com/datacharmer/test_db), a combination of a large base of data (approximately 160MB) spread over six separate tables and consisting of 4 million records in total.

[MySQL Sample Database official documentation](https://dev.mysql.com/doc/employee/en/employees-introduction.html)

### Docker Configuration

Two files handle the database setup:

- **Dockerfile**: Creates a MySQL image with the employees database
  - Downloads the __test_db__ repository
  - Sets up initialization scripts
  - Loads all employee data and validates it

- **docker-compose.yml**: Configures the MySQL container
  - Sets up environment variables

### Getting Started

1. Build and start database container:
```bash
docker-compose up -d db
```

2. Verify if the data is loaded:
```bash
docker exec -it employees_db mysql -uroot -ppassword -e "USE employees; SELECT COUNT(*) FROM employees;"
```

Expected output: 300,024 employees

### Useful Commands

- **MySQL CLI**:
  ```bash
  docker exec -it employees_db mysql -uroot -ppassword
  ```

- **Run a specific SQL query**:
  ```bash
  docker exec -it employees_db mysql -uroot -ppassword employees -e "SELECT * FROM departments;"
  ```

- **Remove container and volume**:
  ```bash
  docker-compose down -v
  ```

## Data Analysis (with SQL) [Step 1]

I did all the analysis considering that the date marker ‘9999-01-01’ represents the current date and that we are at the end of 2002 (‘2002-12-31’). To have complete information for that period.

### 1. Employee Career Progression

I did a group by employee and for each of them I counted the different amount of titles they had and calculated the average time between each title, in years.


### 2. Department Turnover Rate

To calculate the turnover rate, first I join departments table with employees per department. Then I did a group by department, counted the total number of employees over time, and also counted how many employees no longer worked in that department. 

Finally doing the division between the two I get the turnover rate (the percentage of employees who left the department).


### 3. Salary Trends

I use date range overlaps to ensure the salary record corresponds to the period when the employee held that specific title and was in that specific department.

The data is grouped by year, department name, and title to show the evolution of average salaries across the organization's structure, over time.

### 4. Longest-serving employees

I filter for current employment status and positions by looking for records where the end date is `9999-01-01` (currently working). The query calculates years of service by finding the difference between the hire date and the reference date ('2002-12-31').

### 5. Manager Impact

For this exercise, I created two CTEs. The first one makes several joins to get the employees by department, and their manager (it keeps only the periods of time where each employee worked with each manager).
Then based on the dates of the employee and manager, I calculate the employee's years under that manager. 
In the other CTE I applied on the previous table a group by manager and department and use windows functions to rank the managers (to avoid a union). DENSE RANK could be used in case there is a tie.

### 6. Gender Distribution

I focus on current employees (where to_date = `9999-01-01`) to analyze the situation as of the reference date ('2002-12-31') rather than historical patterns. The query uses a CTE with window functions to calculate both, individual gender counts and total department employee counts.

## Data Processing (with Python and Spark) [Steps 2.1 - 2.3]

The `/02-data_processing` folder contains a simple ETL pipeline for processing employee data using PySpark. The project follows a straightforward approach to extract data from a MySQL database, perform basic validations, and prepare the dataset for further analysis.


The main script connects to the database, loads the tables, and applies basic data validation. It checks for data type consistency and identifies potential issues like null values or duplicate records. 


This solution uses Spark to extract, transform and analyze data. There are the following files:

```
|   api.py -> used in Airflow step 
|   config.py -> to define data structure and table names
|   data_validator.py -> class to make validations and transform data
|   main.py -> main orchestration process
|   report_generator.py -> class to analyze data and make reports
|   spark_session.py -> class to manage spark session and ddbb connection
|
+---connector -> to establish a connection to the database 
|       mysql-connector-java-8.0.28.jar
```

This `connector` folder should not be necessary, the container should be able to install the dependency by itself, but it was giving me a lot of headaches.

### Run container [Step 2.1]

Build and start data processing and reporting container:
```bash
docker-compose up -d spark
```

This will process the main.py, with the ETL process, and then leave an endpoint open to receive airflow requests for report creation.

### Report Generation [Step 2.2 & 2.3]

The report generation module creates four analytical reports from the processed employee data.

Results are saved in `02-data_processing/output/reports/...` directory.

#### Annual Salary by Department Report
- Salary Proration: prorates salaries when employees change departments mid-year, ensuring costs are attributed accurately to each department based on days worked.
- Period Overlap Logic: The implementation identifies the precise overlap between salary periods and department assignments using the later of start dates and earlier of end dates.

#### Job Hopper Analysis
The analysis identifies employees who have changed departments more than two times, considering them "job hoppers."

_Note: no results were found_

#### Employee Master Report
A comprehensive snapshot report including:
- Employee number and full name
- Current department and position title
- Current salary
- Hire date and calculated tenure in years

This report use the flag date '9999-01-01' to identify current positions and properly handles employees with multiple historical records. The report is saved in Parquet and CSV format, for easy viewing.

#### Changes Report
This detailed timeline tracks organizational changes between specified dates (defaulting to first half of 2000):
- Department changes
- Title changes
- Salary changes
- Manager changes


## Airflow Automation - Pipeline Architecture [Step 2.4]

The system uses a microservices architecture to separate orchestration (Airflow) from data processing (Spark). Communication between them is achieved through a REST API developed with Flask.

Airflow files and directories (and the DAG) are alocated on `03-airflow/...` directory.

To run airflow container:
```bash
docker-compose up -d airflow
```

### Components:

1. **Spark Container**: Responsible for data processing and analysis using PySpark. Exposes a very easy REST API to generate reports on demand.

2. **Airflow Container**: Responsible for task orchestration and scheduling, execution monitoring, and error handling.

3. **Flask API** (inside spark container): Interface that allows Airflow to request report generation from the Spark container.

### REST API for Processing

A REST endpoint was implemented in the Spark container that allows requesting the generation of change reports for a specific period:

`http://localhost:5000/process?start_date=2000-02-01&end_date=2000-03-01`

This endpoint receives start and end dates as parameters and executes processing in the background, immediately returning a response to allow Airflow to continue its flow.

### Airflow DAG

![Airflow running](/images/airflow_running.png)

The implemented DAG (`employee_changes_monthly_report`) automates the complete process of generating and storing monthly reports:

1. **calculate_date_range**: Calculates the date range for the previous month or uses user-defined parameters.

2. **clean_previous_reports**: Prepares the environment by removing previous reports.

3. **call_api**: Invokes the API endpoint to initiate data processing.

4. **wait_for_csv**: Waits until the CSV report is generated.

5. **upload_to_s3**: Simulates uploading the generated CSV file to an S3 bucket.

6. **notify_success**: Notifies successful completion of the process.

The DAG is scheduled to run automatically on the first day of each month at 3:00 AM, generating the previous month's change report.

### Error Handling

The alerts logic is inside `03-airflow/dags/utils/alerts.py` and is just an example, it does not actually have a SMTP service configured.

### S3 Connection

The connection and uploading the file to amazon S3 is not really being done, this is only a simulation.

In a production environment, uploading to S3 should be done using official Airflow connectors:

1. **AWS Provider Installation**:

    `pip install apache-airflow-providers-amazon`


2. **Connection Configuration**: Configure an AWS connection in the Airflow interface with appropriate credentials.

3. **S3 Operator Usage**: Replace the simulation task with a real S3 operator:

    ```python
    from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id='upload_to_s3',
        filename=f'{CSV_DIR}/*.csv',
        dest_key=f'reports/employee_changes_{{ execution_date.strftime("%Y-%m") }}.csv',
        dest_bucket='my-s3-bucket',
        aws_conn_id='aws_default',
        replace=True
    )
    ```

## High level architecture diagram

![Arch Diagram](/images/arch_diagram.png)


## Proposed Improvements

To enhance the code quality, maintainability, and robustness of this repository, I would make the following improvements:

### Code Quality
- **Implement linters**: Set up Black for formatting, Pylint for code analysis, and Flake8 for PEP 8 compliance
- **Add type hints**: Incorporate Python type annotations to improve code clarity and enable static analysis
- **Add docstrings**: Ensure comprehensive documentation within the code following a standard format (e.g., Google style)
- **Implement unit tests**: Develop a comprehensive test suite with pytest, including mock tests for external dependencies

### Logging
- **Structured logging**: Implement a proper logging framework with different levels (DEBUG, INFO, ERROR)
- **Centralized logs**: Set up a centralized logging system to aggregate logs from all containers

### Configuration Management
- **Environment variables**: Replace hardcoded values with environment variables using dotenv
- **Configuration files**: Implement a hierarchical configuration system for different environments (dev, test, prod)

### Error Handling
- **Custom exceptions**: Define application-specific exception hierarchy
- **Graceful degradation**: Implement fallback mechanisms for non-critical failures

### Infrastructure Improvements
- **CI/CD pipeline**: Set up automated testing, linting, and deployment pipeline
- **Monitoring**: Implement application metrics and monitoring with tools like Grafana.
- **Containerization**: Optimize Docker images for smaller size and better security

### Data Processing Enhancements
- **Incremental processing**: Implement change data capture for more efficient data processing
- **Quality checks**: Add more data validations and quality metrics
- **Lineage**: Track data transformations for better auditability