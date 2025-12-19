
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook  # core Airflow (no provider needed)
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests

# Coordinates (London)
LATITUDE = '51.5074'
LONGITUDE = '-0.1278'

POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'  # optional; if not present we fall back to public base URL

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

def _build_api_url():
    """
    Build the Open-Meteo API URL.
    If an Airflow connection named `open_meteo_api` exists, use it for the base host;
    otherwise default to https://api.open-meteo.com
    """
    base = "https://api.open-meteo.com"
    try:
        conn = BaseHook.get_connection(API_CONN_ID)
        # Prefer schema + host if present
        schema = conn.schema or "https"
        host = (conn.host or "").rstrip("/")
        if host:
            base = f"{schema}://{host}"
    except Exception:
        # No connection configured; stick with default
        pass

    endpoint = f"/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true"
    return f"{base}{endpoint}"

with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=["example", "weather", "etl"],
) as dag:

    @task()
    def extract_weather_data():
        """Extract weather data from Open-Meteo API using direct HTTP (no HttpHook)."""
        url = _build_api_url()
        resp = requests.get(url, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to fetch weather data: {resp.status_code} - {resp.text[:200]}")
        data = resp.json()
        if "current_weather" not in data:
            raise ValueError(f"Unexpected response structure: {list(data.keys())}")
        return data

    @task()
    def transform_weather_data(weather_data: dict):
        """Transform the extracted weather data into a flat dict."""
        cw = weather_data['current_weather']
        transformed = {
            'latitude': float(LATITUDE),
            'longitude': float(LONGITUDE),
            'temperature': float(cw['temperature']),
            'windspeed': float(cw['windspeed']),
            'winddirection': float(cw['winddirection']),
            'weathercode': int(cw['weathercode']),
        }
        return transformed

    @task()
    def load_weather_data(transformed_data: dict):
        """Load transformed data into PostgreSQL using PostgresHook."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS weather_data (
                    latitude FLOAT,
                    longitude FLOAT,
                    temperature FLOAT,
                    windspeed FLOAT,
                    winddirection FLOAT,
                    weathercode INT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """)
                cur.execute("""
                INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
                VALUES (%s, %s, %s, %s, %s, %s);
                """, (
                    transformed_data['latitude'],
                    transformed_data['longitude'],
                    transformed_data['temperature'],
                    transformed_data['windspeed'],
                    transformed_data['winddirection'],
                    transformed_data['weathercode'],
                ))
            conn.commit()

    # DAG flow
    wd = extract_weather_data()
    td = transform_weather_data(wd)
    load_weather_data(td)
