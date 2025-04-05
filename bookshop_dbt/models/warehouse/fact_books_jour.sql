-- models/warehouse/fact_books_jour.sql
{{ config(materialized='table',schema='warehouse')}}


SELECT 
    books_id,
    TO_CHAR(TO_DATE(TO_CHAR(date_edit)), 'FMDay') AS jour,
    SUM(qte) AS total_qte_vendue
FROM {{ ref('stg_ventes') }}
GROUP BY books_id, jour

