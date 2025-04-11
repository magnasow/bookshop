-- models/warehouse/fact_books_annees.sql
{{ config(materialized='table',schema='warehouse')}}


SELECT 
    book_id,
    EXTRACT(YEAR FROM TO_DATE(TO_CHAR(sale_date))) AS annees,
    SUM(quantity) AS quantity
FROM {{ ref('stg_ventes') }}
GROUP BY book_id, annees
