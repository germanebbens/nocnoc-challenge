SELECT YEAR(s.from_date) AS year,
       d.dept_name,
       t.title,
       ROUND(AVG(s.salary), 2) AS avg_salary
FROM salaries s
JOIN employees e ON s.emp_no = e.emp_no
JOIN dept_emp de ON e.emp_no = de.emp_no
JOIN departments d ON de.dept_no = d.dept_no
JOIN titles t ON e.emp_no = t.emp_no
WHERE (s.from_date BETWEEN de.from_date AND de.to_date)
  AND (s.from_date BETWEEN t.from_date AND t.to_date)
GROUP BY year,
         d.dept_name,
         t.title
ORDER BY year,
         d.dept_name,
         t.title;