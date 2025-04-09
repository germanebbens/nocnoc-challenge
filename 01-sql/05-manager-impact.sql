WITH emp_tenure AS (
    SELECT
        dm.emp_no AS manager_id,
        CONCAT(e.first_name, ' ', e.last_name) AS manager_name,
        d.dept_name,
        de.emp_no,
        CASE
            WHEN de.to_date = '9999-01-01' THEN DATEDIFF('2002-12-31', GREATEST(de.from_date, dm.from_date))
            WHEN de.to_date < dm.to_date THEN DATEDIFF(de.to_date, GREATEST(de.from_date, dm.from_date))
            ELSE DATEDIFF(CASE WHEN dm.to_date = '9999-01-01' THEN '2002-12-31' ELSE dm.to_date END, 
                          GREATEST(de.from_date, dm.from_date))
        END / 365.0 AS years_under_manager
    FROM dept_manager dm
    JOIN employees e ON dm.emp_no = e.emp_no
    JOIN departments d ON dm.dept_no = d.dept_no
    JOIN dept_emp de ON dm.dept_no = de.dept_no
    WHERE de.from_date <= CASE WHEN dm.to_date = '9999-01-01' THEN '2002-12-31' ELSE dm.to_date END
      AND (de.to_date >= dm.from_date OR de.to_date = '9999-01-01')
),
retention_stats AS (
    SELECT
        manager_id,
        manager_name,
        dept_name,
        AVG(years_under_manager) AS avg_years,
        RANK() OVER (ORDER BY AVG(years_under_manager)) AS asc_rank,
        RANK() OVER (ORDER BY AVG(years_under_manager) DESC) AS desc_rank
    FROM emp_tenure
    GROUP BY manager_id, manager_name, dept_name
)
SELECT
    manager_id,
    manager_name,
    dept_name,
    ROUND(avg_years, 2) AS avg_years_under_manager,
    IF(asc_rank = 1, 'Lowest Retention', 'Highest Retention') AS retention_type
FROM retention_stats
WHERE asc_rank = 1 OR desc_rank = 1
ORDER BY avg_years;