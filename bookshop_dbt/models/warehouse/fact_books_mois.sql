-- models/warehouse/fact_books_mois.sql
{{ config(materialized='table',schema='warehouse')}}

SELECT 
    book_id,
    TO_CHAR(TO_DATE(TO_CHAR(sale_date)), 'FMMonth') AS mois,
    SUM(quantity) AS quantity
FROM {{ ref('stg_ventes') }}
GROUP BY book_id, mois
