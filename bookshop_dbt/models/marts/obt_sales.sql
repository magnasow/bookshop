{{ config(materialized='table', schema='marts') }}

WITH ventes AS (
    SELECT
        id AS id_vente,
        factures_id,
        books_id,
        pu,
        qte,
        annees,
        mois,
        jour
    FROM {{ ref('fact_ventes') }}
),

factures AS (
    SELECT
        id AS id_facture,
        code AS code_facture,
        customers_id,
        qte_totale,
        total_amount,
        total_paid
    FROM {{ ref('fact_factures') }}
),

books AS (
    SELECT
        id AS id_book,
        code AS code_book,
        intitule AS intitule_book,
        isbn_10,
        isbn_13,
        category_id
    FROM {{ ref('dim_books') }}
),

categories AS (
    SELECT
        id AS id_category,
        intitule AS intitule_category
    FROM {{ ref('dim_category') }}
),

clients AS (
    SELECT
        id AS id_customer,
        code AS code_customer,
        nom
    FROM {{ ref('dim_customers') }}
)

SELECT
    v.id_vente,
    v.annees,
    v.mois,
    v.jour,
    v.pu,
    v.qte,

    f.code_facture,
    f.qte_totale,
    f.total_amount,
    f.total_paid,

    b.code_book,
    b.intitule_book,
    b.isbn_10,
    b.isbn_13,

    c.intitule_category,

    cu.code_customer,
    cu.nom

FROM ventes v
LEFT JOIN factures f ON v.factures_id = f.id_facture
LEFT JOIN clients cu ON f.customers_id = cu.id_customer
LEFT JOIN books b ON v.books_id = b.id_book
LEFT JOIN categories c ON b.category_id = c.id_category
