employees table
emp_id | emp_name | dept_id | salary | join_date
-----------------------------------------------
1      | Alice    | 10      | 60000  | 2021-01-10
2      | Bob      | 20      | 70000  | 2020-03-15
3      | Charlie  | 10      | 50000  | 2022-07-01
4      | David    | 20      | 90000  | 2019-05-20
5      | Eva      | 30      | 80000  | 2021-09-12

departments table
dept_id | dept_name
-------------------
10      | HR
20      | IT
30      | Finance


1️⃣ GROUP BY
🟢 Basic

Q1: Get total salary per department

SELECT dept_id, SUM(salary) AS total_salary
FROM employees
GROUP BY dept_id;

🟡 Medium

Q2: Get average salary per department

SELECT dept_id, AVG(salary) AS avg_salary
FROM employees
GROUP BY dept_id;

🔴 Advanced

Q3: Get department having more than 2 employees

SELECT dept_id
FROM employees
GROUP BY dept_id
HAVING COUNT(*) > 2;

2️⃣ HAVING
🟢 Basic

Q4: Departments with total salary > 1,20,000

SELECT dept_id, SUM(salary)
FROM employees
GROUP BY dept_id
HAVING SUM(salary) > 120000;

🟡 Medium

Q5: Departments where avg salary > overall avg salary

SELECT dept_id
FROM employees
GROUP BY dept_id
HAVING AVG(salary) >
       (SELECT AVG(salary) FROM employees);

🔴 Advanced

Q6: Departments with highest total salary

SELECT dept_id
FROM employees
GROUP BY dept_id
HAVING SUM(salary) = (
    SELECT MAX(total_sal)
    FROM (
        SELECT SUM(salary) AS total_sal
        FROM employees
        GROUP BY dept_id
    ) t
);

3️⃣ WINDOW FUNCTIONS (Super Important)
🟢 Basic

Q7: Rank employees by salary (overall)

SELECT emp_name, salary,
       RANK() OVER (ORDER BY salary DESC) AS rank
FROM employees;

🟡 Medium

Q8: Rank employees by salary within department

SELECT emp_name, dept_id, salary,
       RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS dept_rank
FROM employees;

🔴 Advanced

Q9: Get highest paid employee per department

SELECT *
FROM (
    SELECT emp_name, dept_id, salary,
           ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) rn
    FROM employees
) t
WHERE rn = 1;

4️⃣ CASE WHEN
🟢 Basic

Q10: Categorize salary
SELECT emp_name, salary,
       CASE
           WHEN salary >= 80000 THEN 'High'
           WHEN salary >= 60000 THEN 'Medium'
           ELSE 'Low'
       END AS salary_category
FROM employees;

🟡 Medium

Q11: Add bonus eligibility

SELECT emp_name, salary,
       CASE
           WHEN salary > 70000 THEN salary * 0.10
           ELSE 0
       END AS bonus
FROM employees;

🔴 Advanced

Q12: Department-wise conditional aggregation

SELECT dept_id,
       SUM(CASE WHEN salary > 70000 THEN 1 ELSE 0 END) AS high_paid_count
FROM employees
GROUP BY dept_id;

5️⃣ SUBQUERIES
🟢 Basic

Q13: Employees earning more than average salary

SELECT emp_name, salary
FROM employees
WHERE salary > (SELECT AVG(salary) FROM employees);

🟡 Medium

Q14: Employees earning more than department avg

SELECT emp_name, salary
FROM employees e
WHERE salary >
      (SELECT AVG(salary)
       FROM employees
       WHERE dept_id = e.dept_id);

🔴 Advanced

Q15: Employees in department with highest avg salary

SELECT emp_name
FROM employees
WHERE dept_id = (
    SELECT dept_id
    FROM employees
    GROUP BY dept_id
    ORDER BY AVG(salary) DESC
    LIMIT 1
);

6️⃣ CTEs (WITH clause)
🟢 Basic

Q16: Use CTE to get department avg salary
WITH dept_avg AS (
    SELECT dept_id, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY dept_id
)
SELECT * FROM dept_avg;

🟡 Medium

Q17: Employees earning above department avg (using CTE)

WITH dept_avg AS (
    SELECT dept_id, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY dept_id
)
SELECT e.emp_name, e.salary
FROM employees e
JOIN dept_avg d
ON e.dept_id = d.dept_id
WHERE e.salary > d.avg_salary;

🔴 Advanced

Q18: Second highest salary per department
WITH ranked AS (
    SELECT emp_name, dept_id, salary,
           DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) rnk
    FROM employees
)
SELECT emp_name, dept_id, salary
FROM ranked
WHERE rnk = 2;

