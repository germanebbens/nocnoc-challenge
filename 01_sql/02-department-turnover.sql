SELECT d.dept_no,
       d.dept_name,
       COUNT(DISTINCT de.emp_no) AS total_employees,
       SUM(CASE
               WHEN de.to_date != '9999-01-01' THEN 1
               ELSE 0
           END) AS left_employees,
       ROUND((SUM(CASE
                      WHEN de.to_date != '9999-01-01' THEN 1
                      ELSE 0
                  END) / COUNT(DISTINCT de.emp_no)) * 100, 2) AS turnover_rate
FROM departments d
JOIN dept_emp de ON d.dept_no = de.dept_no
GROUP BY d.dept_no,
         d.dept_name
ORDER BY turnover_rate DESC;