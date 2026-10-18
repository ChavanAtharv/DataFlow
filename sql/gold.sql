-- Gold Layer: Customer Analytics

DROP TABLE IF EXISTS gold_customer_city_summary;

CREATE TABLE gold_customer_city_summary AS
SELECT
    city,
    COUNT(*) AS customer_count
FROM customers
GROUP BY city
ORDER BY customer_count DESC;


DROP TABLE IF EXISTS gold_customer_age_summary;

CREATE TABLE gold_customer_age_summary AS
SELECT
    age_group,
    COUNT(*) AS customer_count
FROM customers
GROUP BY age_group
ORDER BY customer_count DESC;
