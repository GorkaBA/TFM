import logging
import requests
from datetime import datetime, timedelta, timezone
import sys
import os

# Añadir la ruta del directorio 'carga_de_datos' al path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'carga_de_datos')))

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from config import nombres, geo_names, influxdb_token, influxdb_bucket, influxdb_org, influxdb_url
from balance_carga import consulta_balance_electrico, balance_comunidades
import azure.functions as func


def main(mytimer: TimerRequest) -> None:
    utc_timestamp = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
