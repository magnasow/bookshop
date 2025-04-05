-- models/warehouse/dim_customers.sql
{{ config(materialized='table',schema='warehouse')}}


WITH base AS (
    SELECT 
        id,
        code,
        first_name,
        last_name,
        CONCAT(first_name, ' ', last_name) AS nom
    FROM {{ ref('stg_customers') }}
)

SELECT * FROM base
