# streamlit_app.py

import streamlit as st
import snowflake.connector
import pandas as pd

# --- Titre ---
st.set_page_config(page_title="BookShop Dashboard", layout="wide")
st.title("📚 Dashboard BookShop - Ventes & Clients")

# --- Initialisation de df vide ---
df = pd.DataFrame()

# --- Connexion à Snowflake ---
try:
    conn = snowflake.connector.connect(
        user='marietasow',
        password='Sow]123456789]',
        account='bivpnnj-vd86335',
        warehouse='COMPUTE_WH',
        database='BOOKSHOP',
        schema='STAGGING_MARTS'
    )
    st.success("Connexion à Snowflake réussie.")
except Exception as e:
    st.error(f"Erreur de connexion à Snowflake : {str(e)}")
    conn = None

# --- Chargement des données ---
query = "SELECT * FROM obt_sales"

try:
    if conn:
        df = pd.read_sql(query, conn)
        if df.empty:
            st.warning("⚠️ Aucune donnée dans Snowflake. Chargement local...")
            df = pd.read_csv("data/Books_Data_Clean.csv")
            st.success("✅ Données locales chargées.")
except Exception:
    st.warning("⚠️ Erreur Snowflake. Chargement local...")
    try:
        df = pd.read_csv("data/Books_Data_Clean.csv")
        st.success("✅ Données locales chargées.")
    except Exception as err:
        st.error(f"❌ Impossible de charger les données locales : {str(err)}")

# --- Si données chargées ---
if not df.empty:
    df.columns = df.columns.str.strip().str.lower()  # standardisation

    # --- Barre latérale : Filtres ---
    st.sidebar.header("🎛️ Filtres dynamiques")

    annees = sorted(df["publishing year"].dropna().unique())
    auteurs = sorted(df["author"].dropna().unique())
    genres = sorted(df["genre"].dropna().unique())
    langues = sorted(df["language_code"].dropna().unique())

    annee_selection = st.sidebar.multiselect("Année", annees)
    auteur_selection = st.sidebar.multiselect("Auteur", auteurs)
    genre_selection = st.sidebar.multiselect("Genre", genres)
    langue_selection = st.sidebar.multiselect("Langue", langues)

    # --- Recherche ---
    st.sidebar.markdown("### 🔍 Recherche livre / auteur")
    search_query = st.sidebar.text_input("Mot-clé")

    # --- Application des filtres ---
    df_filtré = df.copy()
    if annee_selection:
        df_filtré = df_filtré[df_filtré["publishing year"].isin(annee_selection)]
    if auteur_selection:
        df_filtré = df_filtré[df_filtré["author"].isin(auteur_selection)]
    if genre_selection:
        df_filtré = df_filtré[df_filtré["genre"].isin(genre_selection)]
    if langue_selection:
        df_filtré = df_filtré[df_filtré["language_code"].isin(langue_selection)]
    if search_query:
        df_filtré = df_filtré[
            df_filtré["book name"].str.contains(search_query, case=False, na=False)
            | df_filtré["author"].str.contains(search_query, case=False, na=False)
        ]

    # --- KPIs ---
    st.subheader("📊 Indicateurs clés")
    col1, col2, col3 = st.columns(3)
    col1.metric("💰 CA total", f"${df_filtré['gross sales'].sum():,.2f}")
    col2.metric("📚 Livres vendus", int(df_filtré["units sold"].sum()))
    col3.metric("⭐ Note moyenne", round(df_filtré["book_average_rating"].mean(), 2))

    # --- Aperçu données ---
    with st.expander("🧐 Aperçu des données filtrées"):
        st.dataframe(df_filtré.head(20))

    # --- Top 10 livres les plus vendus ---
    if "book name" in df.columns and "units sold" in df.columns:
        st.subheader("📖 Top 10 livres les plus vendus")
        top_books = (
            df_filtré.groupby("book name")["units sold"]
            .sum()
            .sort_values(ascending=False)
            .head(10)
        )
        st.bar_chart(top_books)

    # --- Ventes par genre ---
    if "genre" in df.columns:
        st.subheader("🎯 Ventes par genre")
        genre_sales = (
            df_filtré.groupby("genre")["units sold"]
            .sum()
            .sort_values(ascending=False)
        )
        st.bar_chart(genre_sales)

    # --- Ventes par année ---
    if "publishing year" in df.columns:
        st.subheader("📅 Ventes par année")
        year_sales = (
            df_filtré.groupby("publishing year")["units sold"]
            .sum()
            .sort_index()
        )
        st.line_chart(year_sales)

    # --- Export CSV ---
    st.subheader("📥 Exporter les données")
    csv = df_filtré.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="📁 Télécharger les données filtrées (CSV)",
        data=csv,
        file_name="donnees_filtrees_bookshop.csv",
        mime="text/csv",
    )

else:
    st.warning("⚠️ Aucune donnée à afficher.")

# --- Fermeture de connexion ---
if conn:
    conn.close()
