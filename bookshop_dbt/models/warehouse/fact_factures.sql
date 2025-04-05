-- models/warehouse/fact_factures.sql
{{ config(materialized='table',schema='warehouse')}}


WITH base AS (
    SELECT 
        id,
        code,
        customers_id,
        qte_totale,
        total_amount,
        total_paid,
        TO_DATE(TO_CHAR(date_edit)) AS date_facture,
        EXTRACT(YEAR FROM TO_DATE(TO_CHAR(date_edit))) AS annees,
        TO_CHAR(TO_DATE(TO_CHAR(date_edit)), 'FMMonth') AS mois,
        TO_CHAR(TO_DATE(TO_CHAR(date_edit)), 'FMDay') AS jour
    FROM {{ ref('stg_factures') }}
) 

SELECT * FROM base
