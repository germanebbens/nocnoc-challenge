# NocNoc Challenge

Data engineering solution for employee database analysis and processing using SQL, Python, Spark and Airflow.

Repo contains:
- [Challenge](/Challenge%20Data%20Engineer%20-%20NocNoc%20__.pdf): steps to complete challenge.
- Dockerfile & docker-compose.yml: docker config files to run database and load data.

## Database Setup

The project uses the [MySQL Employees Sample Database](https://github.com/datacharmer/test_db), a combination of a large base of data (approximately 160MB) spread over six separate tables and consisting of 4 million records in total.

[MySQL Sample Database official documentation](https://dev.mysql.com/doc/employee/en/employees-introduction.html)

### Docker Configuration

Two files handle the database setup:

- **Dockerfile**: Creates a MySQL image with the employees database
  - Downloads the __test_db__ repository
  - Sets up initialization scripts
  - Loads all employee data and validates it

- **docker-compose.yml**: Configures the MySQL container
  - Exposes port 3306
  - Sets up environment variables

### Getting Started

1. Build and start container:
```bash
docker-compose up -d
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

## Data Analysis (with SQL)

### 1. Employee Career Progression

The query identifies how many different titles each employee has held and the average time in each position. 

I use `9999-01-01` as the current position indicator and calculate the duration to the current date. For previous positions, calculate the difference between start and end date. Then convert days to years for readability.


### 2. Department Turnover Rate

This query calculates the turnover rate for each department by identifying the proportion of employees who have left over time.

I consider an employee has left when their `to_date` is not equal to `9999-01-01` (which represents current employment). The turnover rate is calculated as the percentage of employees who have left compared to the total number of employees who have worked in the department.


### 3. Salary Trends

This query determines the progression of average salary over time, broken down by title and department.

I use date range overlaps to ensure the salary record corresponds to the period when the employee held that specific title and was in that specific department.

The data is grouped by year, department name, and title to show the evolution of salaries across the organization's structure over time.

### 4. Longest-serving employees

Identify the 10 employees with the longest tenure, including their current department and title.

I filter for current employment status and positions by looking for records where the end date is `9999-01-01` (currently working). The query calculates years of service making the difference between the hire date and current date.

### 5. Manager Impact

This query calculates the average time employees stay in a department under each manager and identifies managers with the lowest and highest retention rates.

I use window functions (RANK) to efficiently identify both the highest and lowest retention managers in a single query. To handle the special date `9999-01-01` (which is "current" in the database), I replace it with the current date for accurate calculations of employee tenure under each manager.

The query carefully handles overlapping time periods to ensure I'm only counting time when both the employee and manager were active in the same department simultaneously.

### 6. Gender Distribution

This query calculates the gender proportion for each department and identifies which departments have the highest gender disparity.

I focus on current employees (where to_date = `9999-01-01`) to analyze the present situation rather than historical patterns. The query uses a CTE with window functions to efficiently calculate both individual gender counts and total department employee counts in one pass.