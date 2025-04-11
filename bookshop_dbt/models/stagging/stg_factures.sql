-- models/stagging/stg_factures.sql

{{ config(materialized='table') }}

WITH raw_factures AS (
    SELECT 
        id, 
        code AS code, 
        TO_DATE(date_edit, 'YYYY-MM-DD') AS date_edit,  -- Utilisation du format 'YYYY-MM-DD'
        customer_id, 
        qte_totale, 
        total_amount, 
        total_paid, 
        created_at
    FROM {{ source('RAW', 'FACTURES') }}
)
SELECT * 
FROM raw_factures
WHERE date_edit IS NOT NULL
