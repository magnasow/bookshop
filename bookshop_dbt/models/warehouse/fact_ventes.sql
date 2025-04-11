-- models/warehouse/fact_ventes.sql
{{ config(materialized='table',schema='warehouse')}}


WITH base AS (
    SELECT 
        sale_id,
        customer_id,
        book_id,
        quantity,
        total_amount,
        TO_DATE(TO_CHAR( sale_date)) AS  sale_date,
        EXTRACT(YEAR FROM TO_DATE(TO_CHAR( sale_date))) AS annees,
        TO_CHAR(TO_DATE(TO_CHAR( sale_date)), 'FMMonth') AS mois,
        TO_CHAR(TO_DATE(TO_CHAR( sale_date)), 'FMDay') AS jour
    FROM {{ ref('stg_ventes') }}
)

SELECT * FROM base
