import psycopg2
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os

# Charger les variables d'environnement
load_dotenv()

# Connexion PostgreSQL
pg_host = os.getenv("PG_HOST")
pg_db = os.getenv("PG_DB")
pg_user = os.getenv("PG_USER")
pg_password = os.getenv("PG_PASSWORD")

try:
    conn_pg = psycopg2.connect(
        host=pg_host,
        dbname=pg_db,
        user=pg_user,
        password=pg_password
    )
    print("Connexion PostgreSQL réussie")
except Exception as e:
    print(f"Erreur de connexion PostgreSQL: {e}")
    exit()

# Connexion Snowflake avec les variables d'environnement
sf_user = os.getenv("SF_USER")
sf_password = os.getenv("SF_PASSWORD")
sf_account = os.getenv("SF_ACCOUNT")
sf_warehouse = os.getenv("SF_WAREHOUSE")
sf_database = os.getenv("SF_DATABASE")
sf_schema = os.getenv("SF_SCHEMA")

try:
    conn_sf = snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema
    )
    print("Connexion Snowflake réussie")
except Exception as e:
    print(f"Erreur de connexion Snowflake: {e}")
    exit()

# Récupérer les données depuis PostgreSQL
query = "SELECT * FROM category"
try:
    df = pd.read_sql(query, conn_pg)
    print(f"Données récupérées depuis PostgreSQL, nombre de lignes: {len(df)}")
except Exception as e:
    print(f"Erreur lors de la récupération des données PostgreSQL: {e}")
    conn_pg.close()
    conn_sf.close()
    exit()

# Charger les données dans Snowflake
def load_data_to_snowflake(df, table_name):
    try:
        cursor = conn_sf.cursor()
        # Charger les données dans Snowflake
        success, nchunks, nrows, nbytes = cursor.write_pandas(df, table_name)
        cursor.close()

        if success:
            print(f"Les données ont été chargées avec succès dans {table_name}")
        else:
            print(f"Échec du chargement des données dans {table_name}")
    except Exception as e:
        print(f"Erreur lors du chargement des données dans Snowflake: {e}")

# Charger les données dans la table "category"
load_data_to_snowflake(df, 'category')

# Fermer les connexions
conn_pg.close()
conn_sf.close()
