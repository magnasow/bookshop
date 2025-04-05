-- models/warehouse/fact_books_annees.sql
{{ config(materialized='table',schema='warehouse')}}


SELECT 
    books_id,
    EXTRACT(YEAR FROM TO_DATE(TO_CHAR(date_edit))) AS annees,
    SUM(qte) AS total_qte_vendue
FROM {{ ref('stg_ventes') }}
GROUP BY books_id, annees
