WITH raw_sales AS (
    SELECT 
        sale_id, 
        book_id,
        customer_id, 
        TO_DATE(sale_date, 'YYYY-MM-DD') AS sale_date,  -- Conversion du format si nécessaire
        quantity, 
        total_amount
    FROM {{ source('RAW', 'SALES') }}
)
SELECT * 
FROM raw_sales
WHERE sale_date IS NOT NULL  -- Assurer qu'il n'y a pas de dates NULL
