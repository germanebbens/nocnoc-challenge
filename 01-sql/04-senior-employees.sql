SELECT e.emp_no,
       CONCAT(e.first_name, ' ', e.last_name) AS full_name,
       e.hire_date,
       DATEDIFF('2002-12-31', e.hire_date) / 365 AS years_of_service,
       d.dept_name AS current_department,
       t.title AS current_title
FROM employees e
JOIN dept_emp de ON e.emp_no = de.emp_no
JOIN departments d ON de.dept_no = d.dept_no
JOIN titles t ON e.emp_no = t.emp_no
WHERE de.to_date = '9999-01-01'
  AND t.to_date = '9999-01-01'
ORDER BY e.hire_date ASC
LIMIT 10;