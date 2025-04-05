-- models/warehouse/fact_books_mois.sql
{{ config(materialized='table',schema='warehouse')}}

SELECT 
    books_id,
    TO_CHAR(TO_DATE(TO_CHAR(date_edit)), 'FMMonth') AS mois,
    SUM(qte) AS total_qte_vendue
FROM {{ ref('stg_ventes') }}
GROUP BY books_id, mois
