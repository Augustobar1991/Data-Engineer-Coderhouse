import logging
import psycopg2
import requests
import pandas as pd
import json
from psycopg2 import extras
import configparser

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_data(api_key, paises):
    """
    Obtiene datos de clima actual de la API WeatherAPI para una lista de países.

    Parameters:
        - api_key (str): La clave de la API WeatherAPI.
        - paises (list): Una lista de nombres de países para los cuales se desean obtener los datos del clima.

    Returns:
        pandas.DataFrame: Un DataFrame que contiene los datos de clima actual para cada país en la lista.
    """
    all_data = []

    try:
        for i, pais in enumerate(paises):
            url = f'http://api.weatherapi.com/v1/current.json?key={api_key}&q={pais}&aqi=no'
            response = requests.get(url)
            response.raise_for_status()  # Lanza una excepción en caso de error HTTP

            data_json = response.json()
            all_data.append({
                'location': data_json['location'],
                'current': data_json['current'],
                'id': i + 1,
            })
    except requests.exceptions.RequestException as e:
        # Capturar cualquier error de solicitud, como errores HTTP.
        logging.error(f"La petición a {url} ha fallado: {e}")
        return None
    except json.JSONDecodeError:
        # Registrar error de formato JSON
        logging.error(f"Respuesta en formato incorrecto de {url}")
        return None
    except Exception as e:
        # Registrar cualquier otro error
        logging.exception(f"Error al obtener datos de {url}: {e}")
        return None

    df = pd.json_normalize(all_data, sep='_')
    return df
    
def connect_to_db(config_file):
    """
    Crea una conexión a la base de datos especificada en el archivo de configuración.

    Parameters:
    config_file (str): La ruta del archivo de configuración.

    Returns:
    Un objeto de conexión a la base de datos.
    """
    try:
        # Lee el archivo .ini
        config = configparser.ConfigParser()
        config.read(config_file)

        # Extrae los valores de conexión de la sección [redshift]
        redshift_credentials = {
            'dbname': config.get('redshift', 'dbname'),
            'user': config.get('redshift', 'user'),
            'password': config.get('redshift', 'pwd'),
            'host': config.get('redshift', 'host'),
            'port': config.get('redshift', 'port')
        }

        # Conecta con Redshift
        conn = psycopg2.connect(**redshift_credentials)
        print("Conexión a la base de datos establecida exitosamente")
        return conn
        
    except Exception as e:
        print(f"Error al conectarse a la base de datos: {e}")
        return None

def get_api_key(config_file_api):
    """
    Obtiene la clave de la API especificada en la sección del archivo de configuración.

    Parameters:
    config_file_api (str): La ruta del archivo de configuración.

    Returns:
    str: La clave de la API.
    """
    try:
        # Lee el archivo INI
        config = configparser.ConfigParser()
        config.read(config_file_api)

        api_key = config['api_key']['api_key']
        return api_key
    except Exception as e:
        print(f"Error al obtener la clave de API: {e}")
    
import pandas as pd
import psycopg2
from psycopg2 import extras

def insert_weather_data(df, table_name, config_file, dup_column):
    """
    Inserta un DataFrame en una tabla existente de Amazon Redshift.

    Args:
    df (pandas.DataFrame): El DataFrame a cargar en la base de datos.
    table_name (str): El nombre de la tabla en la base de datos.
    config_file (str): El archivo de configuración que contiene las credenciales.
    dup_column (str): El nombre de la columna para verificar duplicados.

    Returns:
    None
    """
    try:
        # Conecta con Redshift
        conn = connect_to_db(config_file) # asumiendo que config_data contiene tus credenciales
        # Crear un cursor
        cur = conn.cursor()

        # Truncar la tabla
        cur.execute(f"TRUNCATE TABLE {table_name};")

        # Verificar duplicados
        dup_query = f"SELECT {dup_column} FROM {table_name};"
        cur.execute(dup_query)
        existing_values = set(row[0] for row in cur.fetchall())
        duplicates = df[df[dup_column].isin(existing_values)]
        if not duplicates.empty:
            print("Advertencia: Se encontraron duplicados en la columna especificada. Los siguientes datos no se insertarán:")
            print(duplicates)

        # Insertar valores en la tabla
        insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES %s;"
        valores = [tuple(row) for row in df.to_numpy()]
        extras.execute_values(cur, insert_query, valores, page_size=1000) # Usar execute_values para una inserción más eficiente
        conn.commit()

        # Cerrar cursor (la conexión no se cierra para permitir su reutilización)
        cur.close()     
        print("Datos meteorológicos insertados exitosamente.")
        
    except psycopg2.Error as e:
        print("Ocurrió un error al insertar los datos meteorológicos:", e)
