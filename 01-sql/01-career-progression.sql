SELECT e.emp_no,
       CONCAT(e.first_name, ' ', e.last_name) AS full_name,
       COUNT(DISTINCT t.title) AS different_titles,
       ROUND(AVG(CASE
                     WHEN t.to_date = '9999-01-01' THEN DATEDIFF('2002-12-31', t.from_date)
                     ELSE DATEDIFF(t.to_date, t.from_date)
                 END) / 365, 2) AS avg_years_per_title
FROM employees e
JOIN titles t ON e.emp_no = t.emp_no
GROUP BY e.emp_no,
         full_name
ORDER BY different_titles DESC,
         avg_years_per_title DESC;