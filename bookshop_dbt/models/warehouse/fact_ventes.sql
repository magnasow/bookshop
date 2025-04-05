-- models/warehouse/fact_ventes.sql
{{ config(materialized='table',schema='warehouse')}}


WITH base AS (
    SELECT 
        id,
        code,
        factures_id,
        books_id,
        pu,
        qte,
        TO_DATE(TO_CHAR(date_edit)) AS date_ventes,
        EXTRACT(YEAR FROM TO_DATE(TO_CHAR(date_edit))) AS annees,
        TO_CHAR(TO_DATE(TO_CHAR(date_edit)), 'FMMonth') AS mois,
        TO_CHAR(TO_DATE(TO_CHAR(date_edit)), 'FMDay') AS jour
    FROM {{ ref('stg_ventes') }}
)

SELECT * FROM base
