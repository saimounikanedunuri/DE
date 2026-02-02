orders table
order_id | customer_id | order_date | amount | status
-----------------------------------------------------
1        | 101         | 2024-01-01 | 5000   | SUCCESS
2        | 102         | 2024-01-02 | 12000  | FAILED
3        | 101         | 2024-01-05 | 8000   | SUCCESS
4        | 103         | 2024-01-07 | 15000  | SUCCESS
5        | 102         | 2024-01-08 | 7000   | SUCCESS


customers table
customer_id | customer_name | city
----------------------------------
101         | Ravi          | Hyderabad
102         | Anjali        | Bangalore
103         | Kiran         | Pune


 GROUP BY + HAVING
 Q1: Total order amount per customer
SELECT customer_id,
       SUM(amount) AS total_amount
FROM orders
GROUP BY customer_id;

 Q2: Customers whose total SUCCESS amount > 10,000
SELECT customer_id,
       SUM(amount) AS total_success_amount
FROM orders
WHERE status = 'SUCCESS'
GROUP BY customer_id
HAVING SUM(amount) > 10000;

 Q3: Customer with maximum total order value
SELECT customer_id
FROM orders
GROUP BY customer_id
ORDER BY SUM(amount) DESC
LIMIT 1;

 CASE WHEN
Q4: Categorize orders by amount
SELECT order_id,
       amount,
       CASE
           WHEN amount < 7000 THEN 'LOW'
           WHEN amount BETWEEN 7000 AND 12000 THEN 'MEDIUM'
           ELSE 'HIGH'
       END AS order_category
FROM orders;

 Q5: Create order_flag (SUCCESS=1, FAILED=0)
SELECT order_id,
       status,
       CASE
           WHEN status = 'SUCCESS' THEN 1
           ELSE 0
       END AS order_flag
FROM orders;

 Q6: Count HIGH value orders per city
SELECT c.city,
       COUNT(
           CASE WHEN o.amount > 12000 THEN 1 END
       ) AS high_value_orders
FROM orders o
JOIN customers c
ON o.customer_id = c.customer_id
GROUP BY c.city;

 SUBQUERIES
Q7: Orders with amount > average order amount
SELECT *
FROM orders
WHERE amount >
      (SELECT AVG(amount) FROM orders);

 Q8: Customers who placed more orders than average orders per customer
SELECT customer_id
FROM orders
GROUP BY customer_id
HAVING COUNT(*) >
       (SELECT AVG(order_count)
        FROM (
            SELECT COUNT(*) AS order_count
            FROM orders
            GROUP BY customer_id
        ) t);

 Q9: Customers who never placed a FAILED order
SELECT customer_id
FROM customers
WHERE customer_id NOT IN (
    SELECT customer_id
    FROM orders
    WHERE status = 'FAILED'
);
-- NOT EXISTS is also acceptable (and safer with NULLs)

 WINDOW FUNCTIONS
Q10: Row number by order amount (highest first)
SELECT *,
       ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn
FROM orders;

 Q11: Rank orders per customer by amount
SELECT *,
       RANK() OVER (
           PARTITION BY customer_id
           ORDER BY amount DESC
       ) AS order_rank
FROM orders;

 Q12: Latest order per customer
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY customer_id
               ORDER BY order_date DESC
           ) AS rn
    FROM orders
) t
WHERE rn = 1;
