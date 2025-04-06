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