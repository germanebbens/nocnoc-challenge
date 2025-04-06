WITH department_gender_counts AS (
    SELECT 
        d.dept_no,
        d.dept_name,
        e.gender,
        COUNT(*) AS gender_count,
        SUM(COUNT(*)) OVER (PARTITION BY d.dept_no) AS total_dept_employees
    FROM employees e
    JOIN dept_emp de ON e.emp_no = de.emp_no
    JOIN departments d ON de.dept_no = d.dept_no
    WHERE de.to_date = '9999-01-01'
    GROUP BY d.dept_no, 
             d.dept_name, 
             e.gender
)
SELECT 
    dept_no,
    dept_name,
    SUM(CASE WHEN gender = 'M' THEN gender_count ELSE 0 END) AS male_count,
    SUM(CASE WHEN gender = 'F' THEN gender_count ELSE 0 END) AS female_count,
    ROUND(SUM(CASE WHEN gender = 'M' THEN gender_count ELSE 0 END) / total_dept_employees * 100, 2) AS male_percentage,
    ROUND(SUM(CASE WHEN gender = 'F' THEN gender_count ELSE 0 END) / total_dept_employees * 100, 2) AS female_percentage,
    ROUND(ABS(SUM(CASE WHEN gender = 'M' THEN gender_count ELSE 0 END) - 
              SUM(CASE WHEN gender = 'F' THEN gender_count ELSE 0 END)) / total_dept_employees * 100, 2) AS gender_disparity
FROM department_gender_counts
GROUP BY dept_no, 
         dept_name, 
         total_dept_employees
ORDER BY gender_disparity DESC;