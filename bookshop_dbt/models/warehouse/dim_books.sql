-- models/warehouse/dim_books.sql
{{ config(materialized='table',schema='warehouse')}}


SELECT * 
FROM {{ ref('stg_books') }}
