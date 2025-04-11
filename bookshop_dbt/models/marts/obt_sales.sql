WITH sales AS (
    SELECT
        sale_id,
        customer_id,
        book_id,
        quantity AS qte,
        total_amount,
        total_amount / NULLIF(quantity, 0) AS pu,
        EXTRACT(YEAR FROM sale_date) AS annees,
        TRIM(TO_CHAR(sale_date, 'Month')) AS mois,
        TRIM(TO_CHAR(sale_date, 'Day')) AS jour
    FROM {{ ref('fact_ventes') }}
),

factures AS (
    SELECT
        code AS id_facture,
        customer_id,
        qte_totale,
        total_amount,
        total_paid
    FROM {{ ref('fact_factures') }}
),

categories AS (
    SELECT
        category_id,
        category_name
    FROM {{ ref('dim_category') }}
),

books AS (
    SELECT
        b.book_id,
        b.title,
        b.isbn_10,
        b.isbn_13,
        b.category,  -- nom de la catégorie (texte)
        c.category_id,
        c.category_name AS intitule  -- libellé propre depuis la table dim_category
    FROM {{ ref('dim_books') }} b
    LEFT JOIN categories c
        ON b.category = c.category_name
),

clients AS (
    SELECT
        customer_id,
        nom
    FROM {{ ref('dim_customers') }}
)

SELECT
    s.sale_id,
    s.book_id,
    s.annees,
    s.mois,
    s.jour,
    s.pu AS prix_unitaire,
    s.qte AS qte,

    f.id_facture,
    f.qte_totale,
    f.total_amount AS total_amount,
    f.total_paid AS montant_paye,

    b.title AS title,
    b.isbn_10,
    b.isbn_13,
    b.intitule AS intitule_categorie,

    cl.nom AS nom_client

FROM sales s
LEFT JOIN factures f 
    ON s.customer_id = f.customer_id 
LEFT JOIN clients cl 
    ON s.customer_id = cl.customer_id  
LEFT JOIN books b 
    ON s.book_id = b.book_id
