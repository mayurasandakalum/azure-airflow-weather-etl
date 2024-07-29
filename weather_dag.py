import json
import pandas as pd

from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient


def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9 / 5) + 32
    return temp_in_fahrenheit


def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")

    city = data["name"]
    weather_description = data["weather"][0]["description"]
    temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_farenheit = kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data["dt"] + data["timezone"])
    sunrise_time = datetime.utcfromtimestamp(data["sys"]["sunrise"] + data["timezone"])
    sunset_time = datetime.utcfromtimestamp(data["sys"]["sunset"] + data["timezone"])

    transformed_data = {
        "City": city,
        "Description": weather_description,
        "Temperature (F)": temp_farenheit,
        "Feels Like (F)": feels_like_farenheit,
        "Minimum Temp (F)": min_temp_farenheit,
        "Maximum Temp (F)": max_temp_farenheit,
        "Pressure": pressure,
        "Humidity": humidity,
        "Wind Speed": wind_speed,
        "Sunrise (Local Time)": sunrise_time,
        "Sunset (Local Time)": sunset_time,
    }

    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)

    # now = datetime.now()
    # dt_string = now.strftime("%d%m%Y%H%M%S")
    # dt_string = "current_weather_data_portland_" + dt_string
    # df_data.to_csv(f"{dt_string}.csv", index=False)

    # Azure Blob Storage credentials
    connect_str = "<your_connection_string>"
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_name = "weatheretlcontainer"
    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=f"current_weather_data_portland_{datetime.now().strftime('%d%m%Y%H%M%S')}.csv",
    )

    # Convert DataFrame to CSV and upload to Azure Blob Storage
    csv_data = df_data.to_csv(index=False)
    blob_client.upload_blob(csv_data, blob_type="BlockBlob")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 8),
    "email": ["myemail@domain.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "weather_dag", default_args=default_args, schedule_interval="@daily", catchup=False
) as dag:

    is_weather_api_ready = HttpSensor(
        task_id="is_weather_api_ready",
        http_conn_id="weathermap_api",
        endpoint="/data/2.5/weather?q=Portland&APPID=<your_api_key>",
    )

    extract_weather_data = SimpleHttpOperator(
        task_id="extract_weather_data",
        http_conn_id="weathermap_api",
        endpoint="/data/2.5/weather?q=Portland&APPID=<your_api_key>",
        method="GET",
        response_filter=lambda r: json.loads(r.text),
        log_response=True,
    )

    transform_load_weather_data = PythonOperator(
        task_id="transform_load_weather_data", python_callable=transform_load_data
    )

    is_weather_api_ready >> extract_weather_data >> transform_load_weather_data
