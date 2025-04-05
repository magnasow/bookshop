-- models/warehouse/dim_category.sql
{{ config(materialized='table',schema='warehouse')}}

SELECT * 
FROM {{ ref('stg_category') }}
