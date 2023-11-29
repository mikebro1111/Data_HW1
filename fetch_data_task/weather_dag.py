from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests

def fetch_weather_data(ds, **kwargs):
    cities = ['Lviv', 'Kyiv', 'Kharkiv', 'Odesa', 'Zhmerynka']
    weather_data = {}

    for city in cities:
        # API call to fetch weather data for a specific date and city
        # Replace 'YOUR_API_KEY' and 'API_ENDPOINT' with actual values
        api_url = f"http://API_ENDPOINT?date={ds}&q={city}&key=API_KEY"
        response = requests.get(api_url)
        if response.status_code == 200:
            data = response.json()
            # Extract required data fields
            weather_data[city] = {
                'temperature': data['temperature'],
                'humidity': data['humidity'],
                'cloudiness': data['cloudiness'],
                'wind_speed': data['wind_speed']
            }
            # Process or save your data here, e.g., saving to a database
        else:
            print(f"Failed to fetch data for {city} on {ds}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_data_dag',
    default_args=default_args,
    description='DAG for fetching weather data',
    schedule_interval=timedelta(days=1),
    catchup=True
)

fetch_data_task = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    provide_context=True,
    dag=dag,
)

fetch_data_task