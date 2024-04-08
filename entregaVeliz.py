import json
import pandas as pd
import requests
import psycopg2

# Credenciales de Redshift desde el archivo config JSON
with open("C:/redshift_config.json", 'r') as config_r:
    config = json.load(config_r)

# Clave de API desde el archivo config JSON
with open("C:/api_config.json", 'r') as config_a:
    ap_config = json.load(config_a)

api_key = ap_config["api_key"]

# Lista de ciudades
cities = ["Villa Gesell", "Puerto Iguazú", "Mar del Plata", "Villa La Angostura", "Villa Carlos Paz", "San Rafael", "Salta", "Merlo", "El Calafate", "Bariloche"]

# Crear una lista de registros (filas) para el DataFrame
records = []

for city in cities:
    response = requests.get(f"https://api.weatherbit.io/v2.0/current?city={city}&key={api_key}&lang=es")
    data = response.json()
    city_data = data["data"][0]

    record = {
        "ob_time": city_data["ob_time"],
        "city_name": city,
        "lat": city_data["lat"],
        "lon": city_data["lon"],
        "temp": city_data["temp"],
        "app_temp": city_data["app_temp"],
        "rh": city_data["rh"],
        "description": city_data["weather"]["description"],
        "precip": city_data["precip"],
        "clouds": city_data["clouds"],
        "vis": city_data["vis"],
        "wind_spd": city_data["wind_spd"],
        "wind_cdir_full": city_data["wind_cdir_full"],
        "pres": city_data["pres"]
    }
    records.append(record)

# Crear un DataFrame a partir de la lista de registros
df = pd.DataFrame(records)

# Mostrar el DataFrame
print(df)

# Conexión a Amazon Redshift
try:
    conn = psycopg2.connect(
        host=config["host"],
        dbname=config["dbname"],
        user=config["user"],
        password=config["password"],
        port=config["port"]
    )
    print("Conectado a Amazon Redshift exitosamente!")

except Exception as e:
    print("No se pudo conectar a Amazon Redshift.")
    print(e)

# Crear tabla inicial en Amazon Redshift
create_table = """
CREATE TABLE IF NOT EXISTS Datos_del_clima (
    Hora_Observacion VARCHAR(50),
    Ciudad VARCHAR(50),
    Longitud NUMERIC,
    Latitud NUMERIC,
    Temperatura NUMERIC,
    Sensacion_Termica NUMERIC,
    Humedad NUMERIC,
    Descripcion VARCHAR(50),
    Precipitacion NUMERIC,
    Nubosidad NUMERIC,
    Velocidad_Viento NUMERIC,
    Direccion_Viento VARCHAR(50),
    Presion NUMERIC,
    Visibilidad NUMERIC
);
"""

with conn.cursor() as cursor:
    cursor.execute(create_table)

# Confirmar los cambios en la base de datos
conn.commit()

# Cerrar la conexión
conn.close()
