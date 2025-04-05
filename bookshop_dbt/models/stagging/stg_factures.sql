-- models/stagging/stg_factures.sql

{{ config(materialized='table') }}

WITH raw_factures AS (
    SELECT 
        id, 
        code AS code, 
        TO_DATE(date_edit, 'YYYYMMDD') AS date_edit,  -- Conversion de date_edit en DATE
        customers_id, 
        qte_totale, 
        total_amount, 
        total_paid, 
        created_at
    FROM {{ source('RAW', 'FACTURES') }}
)
SELECT * 
FROM raw_factures
WHERE date_edit IS NOT NULL