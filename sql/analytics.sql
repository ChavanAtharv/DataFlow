-- 1. Total customers
SELECT COUNT(*) AS total_customers
FROM customers;

-- 2. Average age
SELECT ROUND(AVG(age), 2) AS average_age
FROM customers;

-- 3. Customers by city
SELECT city, COUNT(*) AS customer_count
FROM customers
GROUP BY city
ORDER BY customer_count DESC;

-- 4. Customers by age group
SELECT age_group, COUNT(*) AS customer_count
FROM customers
GROUP BY age_group
ORDER BY customer_count DESC;

-- 5. Subquery: customers older than average age
SELECT customer_id, name, age
FROM customers
WHERE age > (
    SELECT AVG(age)
    FROM customers
)
ORDER BY age DESC;


-- 6. CTE: customers above average age
WITH average_age AS (
    SELECT AVG(age) AS avg_age
    FROM customers
)
SELECT c.customer_id, c.name, c.age
FROM customers c
CROSS JOIN average_age a
WHERE c.age > a.avg_age
ORDER BY c.age DESC;


-- 7. Window function: rank customers by age
SELECT
    customer_id,
    name,
    age,
    RANK() OVER (ORDER BY age DESC) AS age_rank
FROM customers
ORDER BY age_rank;
