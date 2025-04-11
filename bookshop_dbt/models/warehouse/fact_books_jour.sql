-- models/warehouse/fact_books_jour.sql
{{ config(materialized='table',schema='warehouse')}}


SELECT 
    book_id,
    TO_CHAR(TO_DATE(TO_CHAR(sale_date)), 'FMDay') AS jour,
    SUM(quantity) AS quantity
FROM {{ ref('stg_ventes') }}
GROUP BY book_id, jour

