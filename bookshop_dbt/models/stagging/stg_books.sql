-- models/stagging/stg_books.sql
{{ config(materialized='table') }}
SELECT 
    *
FROM {{ source('RAW', 'BOOKS') }}  -- Copier toutes les données de la table 'BOOKS' de la source 'RAW'
