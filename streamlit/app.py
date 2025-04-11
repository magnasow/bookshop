import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import snowflake.connector
import pyarrow as pa
from sqlalchemy import create_engine


# --- Connexion à Snowflake ---
try:
    conn = snowflake.connector.connect(
        user='marietasow',
        password='Sow]123456789]',
        account='bivpnnj-vd86335',
        warehouse='COMPUTE_WH',
        database='BOOKSHOP',
        schema='BOOKSHOP_MARTS',
        role='ACCOUNTADMIN'
    )

    # Titre principal avec une icône avant
    st.markdown("""
      <h1 style="text-align: center;">
        📚 <span style="color: #1E90FF;">DASHBOARD BOOKSHOP VENTE et CLIENTS</span> 📊
      </h1>
    """, unsafe_allow_html=True)
    st.success("Connexion à Snowflake réussie.")
except Exception as e:
    st.error(f"Erreur de connexion à Snowflake : {str(e)}")
    conn = None

# Charger les données depuis Snowflake
if conn:
    try:
        # Exécuter une requête SQL pour obtenir les données
        query = "SELECT * FROM obt_sales"  # Remplacer 'obt_sales' par le nom de la table que vous souhaitez charger
        df = pd.read_sql(query, conn)
        
        # Nettoyage des noms de colonnes (pour éviter les erreurs)
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        df = df.dropna()

        # Convertir les colonnes en types appropriés avant de les afficher
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str)

        # Affichage des données récupérées
        st.write("### Aperçu des données récupérées depuis Snowflake :")
        st.write(df)
    except Exception as e:
        st.error(f"Erreur lors de l'exécution de la requête SQL : {str(e)}")
else:
    st.error("Impossible de se connecter à Snowflake.")


# Affichage des premières lignes pour vérifier les données dans 'intitule_book' et 'qte'
st.write("### Aperçu des premières lignes :")
st.write(df[['title', 'qte']].head())

# Exemple de calcul du montant total pour chaque vente
if 'total_amount' in df.columns and 'qte' in df.columns:
    df['total_montant'] = df['qte'] * df['total_amount']
    st.write("### Données avec le calcul du montant total :")
    st.write(df[['sale_id', 'total_montant']])  # Affiche les ID de vente et les montants totaux
else:
    st.error("⚠️ Les colonnes 'total_amount' ou 'qte' sont manquantes.")

# Affichage d'un graphique de la distribution des ventes par année
if 'annees' in df.columns:
    ventes_par_annee = df.groupby('annees')['sale_id'].count()

    # Affichage sous forme de graphique à barres
    st.write("### Distribution des ventes par année :")
    st.bar_chart(ventes_par_annee)

    # Optionnel : Amélioration avec un graphique Matplotlib plus détaillé
    fig, ax = plt.subplots()
    ventes_par_annee.plot(kind='bar', ax=ax, color='skyblue')
    ax.set_title('Distribution des ventes par année')
    ax.set_xlabel('Année')
    ax.set_ylabel('Nombre de ventes')
    st.pyplot(fig)  # Afficher le graphique matplotlib
else:
    st.error("⚠️ La colonne 'annees' est nécessaire pour l'affichage de la distribution des ventes.")

# Liste des livres les plus vendus
if 'title' in df.columns and 'qte' in df.columns:
    st.write("### 📖 Liste des livres les plus vendus :")

    # Supprimer les lignes où 'intitule_book' ou 'qte' sont manquants
    df_clean = df.dropna(subset=['title', 'qte'])
    
    

   

    # Regrouper par titre du livre (intitule_book) et somme des quantités vendues (qte)
    livres_plus_vendus = df_clean.groupby('title')['qte'].sum().sort_values(ascending=False).reset_index()

   

    # Si le regroupement contient des résultats
    if not livres_plus_vendus.empty:
        livres_plus_vendus.columns = ['Titre du Livre', 'Quantité Vendue']
        
        # Affichage du tableau
        st.dataframe(livres_plus_vendus)

        # Affichage du graphique
        fig2, ax2 = plt.subplots(figsize=(10, 6))
        top_n = 10  # Nombre de livres à afficher dans le graphique
        ax2.barh(livres_plus_vendus['Titre du Livre'][:top_n], livres_plus_vendus['Quantité Vendue'][:top_n], color='lightcoral')
        ax2.invert_yaxis()  # Pour avoir le livre le plus vendu en haut
        ax2.set_title('Top 10 des livres les plus vendus')
        ax2.set_xlabel('Quantité Vendue')
        ax2.set_ylabel('Titre du Livre')

        st.pyplot(fig2)
    else:
        st.warning("Aucun livre trouvé après le regroupement.")
else:
    st.warning("Les colonnes 'intitule_book' et/ou 'qte' sont absentes pour générer la liste des livres les plus vendus.")

# Message de confirmation
st.markdown("### Les données sont prêtes pour le téléchargement !")

# Fonction pour télécharger le fichier CSV
def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

# Ajout du bouton de téléchargement du fichier CSV
csv = convert_df_to_csv(df)
st.download_button(
    label="Télécharger le fichier CSV",
    data=csv,
    file_name='ventes_data.csv',
    mime='text/csv'
)

# Convertir le DataFrame en un tableau Arrow pour Streamlit
try:
    # Convertir toutes les colonnes en string explicite
    df = df.apply(lambda x: x.astype(str) if x.dtype == 'object' else x)

    # Convertir le DataFrame en tableau Arrow
    table = pa.Table.from_pandas(df)
    st.write("Tableau Arrow créé avec succès!")
except Exception as e:
    st.error(f"Erreur lors de la conversion du DataFrame en tableau Arrow : {e}")
