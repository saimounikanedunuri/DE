Tables (assume)
transactions
txn_id | account_id | txn_date   | amount | txn_type
-----------------------------------------------
1      | A101       | 2024-01-01 | 10000  | CREDIT
2      | A101       | 2024-01-05 | 3000   | DEBIT
3      | A102       | 2024-01-02 | 20000  | CREDIT
4      | A102       | 2024-01-10 | 5000   | DEBIT
5      | A102       | 2024-01-15 | 7000   | DEBIT
6      | A103       | 2024-01-03 | 15000  | CREDIT

accounts
account_id | customer_name | city
---------------------------------
A101       | Ravi          | Hyderabad
A102       | Anjali        | Bangalore
A103       | Kiran         | Pune

Q1: Total credited amount per account
SELECT account_id,
       SUM(amount) AS total_credit
FROM transactions
WHERE txn_type = 'CREDIT'
GROUP BY account_id;

2. Accounts where total debit amount > 5,000
SELECT account_id,
       SUM(amount) AS total_debit
FROM transactions
WHERE txn_type = 'DEBIT'
GROUP BY account_id
HAVING SUM(amount) > 5000;

3: Account with highest net balance

(Net = Credit − Debit)

SELECT account_id
FROM transactions
GROUP BY account_id
ORDER BY
    SUM(CASE WHEN txn_type = 'CREDIT' THEN amount ELSE 0 END) -
    SUM(CASE WHEN txn_type = 'DEBIT' THEN amount ELSE 0 END) DESC
LIMIT 1;

--CASE WHEN
Q4: Add txn_direction column
SELECT txn_id,
       txn_type,
       CASE
           WHEN txn_type = 'CREDIT' THEN 'IN'
           ELSE 'OUT'
       END AS txn_direction
FROM transactions;

5: Signed amount
SELECT txn_id,
       CASE
           WHEN txn_type = 'CREDIT' THEN amount
           ELSE -amount
       END AS signed_amount
FROM transactions;
-- above is Very common finance logic

6: City-wise net balance
SELECT a.city,
       SUM(
           CASE
               WHEN t.txn_type = 'CREDIT' THEN t.amount
               ELSE -t.amount
           END
       ) AS net_balance
FROM transactions t
JOIN accounts a
ON t.account_id = a.account_id
GROUP BY a.city;

--SUBQUERIES
7: Transactions greater than average transaction amount
SELECT *
FROM transactions
WHERE amount >
      (SELECT AVG(amount) FROM transactions);

8: Accounts with more transactions than average
SELECT account_id
FROM transactions
GROUP BY account_id
HAVING COUNT(*) >
      (
        SELECT AVG(txn_count)
        FROM (
            SELECT COUNT(*) AS txn_count
            FROM transactions
            GROUP BY account_id
        ) t
      );

9: Customers who only did CREDIT transactions
SELECT a.customer_name
FROM accounts a
WHERE a.account_id NOT IN (
    SELECT account_id
    FROM transactions
    WHERE txn_type = 'DEBIT'
);
--Interview note: NOT EXISTS is safer with NULLs.

--WINDOW FUNCTIONS
10: Row number by transaction amount
SELECT *,
       ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn
FROM transactions;

11: Rank transactions per account by date
SELECT *,
       RANK() OVER (
           PARTITION BY account_id
           ORDER BY txn_date
       ) AS txn_rank
FROM transactions;

12: Latest transaction per account
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY account_id
               ORDER BY txn_date DESC
           ) AS rn
    FROM transactions
) t
WHERE rn = 1;
--Extremely common interview question

13: All transactions with customer name and city
SELECT t.*, a.customer_name, a.city
FROM transactions t
JOIN accounts a
ON t.account_id = a.account_id;

