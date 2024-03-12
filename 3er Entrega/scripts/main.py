from datetime import datetime, timedelta, timezone

import pandas as pd

from utils import *

paises = [
    'Afghanistan', 'Albania', 'Algeria', 'Anguilla', 'Antigua and Barbuda', 'Argentina', 'Aruba', 'Austria',
    'Barbados', 'Belgium', 'Botswana', 'Brazil', 'Bulgaria', 'Canada', 'Colombia',
    'Croatia', 'Cyprus', 'Czech Republic', 'Denmark', 'Estonia', 'Federated States of Micronesia', 'Finland',
    'France', 'Germany', 'Greece', 'Guyana', 'Hungary', 'Iceland', 'India', 'Indonesia', 'Ireland', 'Israel',
    'Italy', 'Jamaica', 'Kenya', 'Kuwait', 'Latvia', 'Lithuania', 'Luxembourg', 'Madagascar', 'Malawi', 'Malta',
    'Marshall Islands', 'Mexico', 'Moldova', 'Mongolia', 'Montenegro', 'Myanmar', 'Netherlands', 'New Zealand',
    'North Macedonia', 'Norway', 'Oman', 'Palau', 'Papua New Guinea', 'Paraguay', 'Philippines', 'Poland',
    'Portugal', 'Romania', 'Russia', 'Saint Lucia', 'Saint Vincent and the Grenadines', 'Samoa', 'Senegal', 'Serbia',
    'Slovakia', 'Slovenia', 'Solomon Islands', 'Spain', 'Suriname', 'Sweden', 'Switzerland',
    'Tanzania', 'Thailand', 'Tonga', 'Trinidad and Tobago', 'United Kingdom of Great Britain and Northern Ireland',
    'United States', 'Vanuatu', 'Zimbabwe'
]

def load_weather_data(config_file):
    """
    Proceso ETL para los datos de estaciones
    """
    api_key = get_api_key(config_file)
    df_weather = get_data(api_key, paises)

    # agregar excepcion por http error 429
    try:
        df_weather = df_weather.drop(['location_tz_id',
              'location_localtime_epoch',
              'current_last_updated_epoch',
              'current_wind_mph',
              'current_condition_icon',
              'current_temp_f',
              'current_pressure_in',
              'current_precip_in',
              'current_gust_mph',
              'current_feelslike_f',
              'current_vis_miles'], axis=1)
        df_weather['load_datetime'] = datetime.now()
        insert_weather_data(df_weather,"weather_data", config_file,"location_name")
    except Exception as e:
        logging.error(f"Error: {e}")
        raise e

if __name__ == "__main__":
    load_weather_data("config/config.ini")

    start = datetime.now(timezone.utc) - timedelta(hours=1)
    end = start.strftime("%Y-%m-%dT%H:59:59Z")
    start = start.strftime("%Y-%m-%dT%H:00:00Z")
