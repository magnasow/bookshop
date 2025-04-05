-- models/stagging/stg_category.sql
{{ config(materialized='table') }}
SELECT 
    *
FROM {{ source('RAW', 'CATEGORY') }}  -- Copier toutes les données de la table 'CATEGORY' de la source 'RAW'
