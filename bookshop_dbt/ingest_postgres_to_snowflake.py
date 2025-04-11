import psycopg2
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os

def load_data_to_snowflake():
    load_dotenv()

    # Connexion PostgreSQL
    try:
        conn_pg = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            dbname=os.getenv("PG_DB"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD")
        )
        print("Connexion PostgreSQL réussie")
    except Exception as e:
        print(f"Erreur PostgreSQL: {e}")
        return

    # Connexion Snowflake
    try:
        conn_sf = snowflake.connector.connect(
            user=os.getenv("SF_USER"),
            password=os.getenv("SF_PASSWORD"),
            account=os.getenv("SF_ACCOUNT"),
            warehouse=os.getenv("SF_WAREHOUSE"),
            database=os.getenv("SF_DATABASE"),
            schema=os.getenv("SF_SCHEMA")
        )
        print("Connexion Snowflake réussie")
    except Exception as e:
        print(f"Erreur Snowflake: {e}")
        conn_pg.close()
        return

    # Extraction des données PostgreSQL
    try:
        df = pd.read_sql("SELECT * FROM category", conn_pg)
        print(f"{len(df)} lignes récupérées depuis PostgreSQL")
    except Exception as e:
        print(f"Erreur lecture PostgreSQL: {e}")
        conn_pg.close()
        conn_sf.close()
        return

    # Chargement dans Snowflake
    try:
        cursor = conn_sf.cursor()
        success, nchunks, nrows, _ = cursor.write_pandas(df, 'category')
        cursor.close()

        if success:
            print(f"{nrows} lignes chargées avec succès dans Snowflake")
        else:
            print("Échec du chargement dans Snowflake")
    except Exception as e:
        print(f"Erreur chargement Snowflake: {e}")

    # Fermeture des connexions
    conn_pg.close()
    conn_sf.close()
