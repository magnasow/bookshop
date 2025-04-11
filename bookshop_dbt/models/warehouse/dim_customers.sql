-- models/warehouse/dim_customers.sql
{{ config(materialized='table', schema='warehouse') }}

WITH base AS (
    SELECT 
        customer_id,
        email,
        gender,
        birthdate,
        country,
        CONCAT(name, ' ') AS nom  -- Concatène le champ name avec un espace
    FROM {{ ref('stg_customers') }}  -- Récupère les données de la table stagging
)

SELECT * FROM base
