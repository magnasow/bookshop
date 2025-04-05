-- models/stagging/stg_customers.sql
{{ config(materialized='table') }}
SELECT 
    *
FROM {{ source('RAW', 'CUSTOMERS') }}  -- Copier toutes les données de la table 'CUSTOMERS' de la source 'RAW'
