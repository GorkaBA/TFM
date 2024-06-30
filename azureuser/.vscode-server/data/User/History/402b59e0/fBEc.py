from codecarbon import EmissionsTracker
import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timedelta
from config import nombres, geo_names, influxdb_token, influxdb_bucket, influxdb_org, influxdb_url

# Iniciar el rastreador de emisiones
tracker = EmissionsTracker()
tracker.start()





# Detener el rastreador de emisiones
emissions = tracker.stop()

print(f"Emisiones de carbono estimadas: {emissions} kg CO2")