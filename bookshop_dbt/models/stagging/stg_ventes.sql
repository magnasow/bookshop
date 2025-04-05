-- models/stagging/stg_ventes.sql

{{ config(materialized='table') }}

WITH raw_ventes AS (
    SELECT 
        id, 
        code, 
        TO_DATE(date_edit, 'YYYYMMDD') AS date_edit,  -- Transformation de date_edit en DATE
        factures_id, 
        books_id , 
        pu , 
        qte , 
        created_at
    FROM {{ source('RAW', 'VENTES') }}
)
SELECT * 
FROM raw_ventes
WHERE date_edit IS NOT NULL