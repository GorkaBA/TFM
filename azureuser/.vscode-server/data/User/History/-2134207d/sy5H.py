import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions
import pandas as pd
import numpy as np
from scipy.signal import butter, filtfilt
import time
from datetime import datetime, timedelta
import ssl
import calendar
import re
import urllib3
from influxdb_client.client.write_api import SYNCHRONOUS
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import warnings
warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)

# Parámetros de conexión
url = "http://localhost:8086"  # Usa https si estás utilizando SSL/TLS
token = "MH7gG-ayy-Oum10exnUj9eXLnMRXVSna2aSmRcDHTXt2E4iFZLE1Ozms9McDA5KHFYhpZw823A5Onbma2UNbbg=="
org = "tfm"
bucket_raw = "raw_co2_data"
bucket_processed = "etl_data"

client = InfluxDBClient(
    url=url,
    token=token,
    org=org,
    timeout=60000
)

write_api = client.write_api(write_options=SYNCHRONOUS)

def query_influxdb_sensores(client, start, stop):
    start_query_time = time.time()
    query = f"""
    from(bucket: "raw_co2_data")
      |> range(start: {start.isoformat()}Z, stop: {stop.isoformat()}Z)
      |> filter(fn: (r) => r["_measurement"] == "iborraberganza/co2")
    """
    query_api = client.query_api()
    tables = query_api.query(query)
    data = []
    for table in tables:
        for record in table.records:
            data.append(record.values)
    end_query_time = time.time()
    duration_query = end_query_time - start_query_time
    return pd.DataFrame(data), duration_query

def query_influxdb_api(client, start, stop):
    start_query_time = time.time()
    query = f"""
    from(bucket: "raw_api_data")
      |> range(start: {start.isoformat()}Z, stop: {stop.isoformat()}Z)
      |> filter(fn: (r) => r["Categoria"] == "generacion")
      |> filter(fn: (r) => r["Tipo"] == "No renovable")
    """
    query_api = client.query_api()
    tables = query_api.query(query)
    data = []
    for table in tables:
        for record in table.records:
            data.append(record.values)
    end_query_time = time.time()
    duration_query = end_query_time - start_query_time
    return pd.DataFrame(data), duration_query

def query_influx_api_totalCO2(client, start, stop):
    start_query_time = time.time()
    query = f"""
    from(bucket: "raw_api_data")
      |> range(start: {start.isoformat()}Z, stop: {stop.isoformat()}Z)
      |> filter(fn: (r) => r["Categoria"] == "generacion")
      |> filter(fn: (r) => r["Tipo"] == "Total tCO2 eq.")
    """
    query_api = client.query_api()
    tables = query_api.query(query)
    data = []
    for table in tables:
        for record in table.records:
            data.append(record.values)
    end_query_time = time.time()
    duration_query = end_query_time - start_query_time
    return pd.DataFrame(data), duration_query

def query_influx_api_generacion_total_renovable_norenovale(client, start, stop):
    start_query_time = time.time()
    query = f"""
    from(bucket: "raw_api_data")
      |> range(start: {start.isoformat()}Z, stop: {stop.isoformat()}Z)
      |> filter(fn: (r) => r["Categoria"] == "generacion")
      |> filter(fn: (r) => r["Tipo"] == "No renovable" or r["Tipo"] == "Renovable" or r["Tipo"] == "Generación total")
    """
    query_api = client.query_api()
    tables = query_api.query(query)
    data = []
    for table in tables:
        for record in table.records:
            data.append(record.values)
    end_query_time = time.time()
    duration_query = end_query_time - start_query_time
    return pd.DataFrame(data), duration_query

def query_influx_api_demanda_total(client, start, stop):
    start_query_time = time.time()
    query = f"""
    from(bucket: "raw_api_data")
      |> range(start: {start.isoformat()}Z, stop: {stop.isoformat()}Z)
      |> filter(fn: (r) => r["Categoria"] == "demanda")
      |> filter(fn: (r) => r["Tipo"] == "Demanda")
    """
    query_api = client.query_api()
    tables = query_api.query(query)
    data = []
    for table in tables:
        for record in table.records:
            data.append(record.values)
    end_query_time = time.time()
    duration_query = end_query_time - start_query_time
    return pd.DataFrame(data), duration_query

def agrupar_sensores(df_final_sensores):
    df_final_sensores['_time'] = pd.to_datetime(df_final_sensores['_time'])
    df_final_sensores['Año'] = df_final_sensores['_time'].dt.year
    df_final_sensores['Mes'] = df_final_sensores['_time'].dt.month
    resultado_sensores_mes = df_final_sensores.groupby(['Comunidad_Autonoma', 'Año', 'Mes'])['_value'].sum().reset_index()
    resultado_sensores_ano = df_final_sensores.groupby(['Comunidad_Autonoma', 'Año'])['_value'].sum().reset_index()
    resultado_sensores_total_mes = df_final_sensores.groupby(['Año', 'Mes'])['_value'].sum().reset_index()
    resultado_sensores_total_ano = df_final_sensores.groupby(['Año'])['_value'].sum().reset_index()
    return resultado_sensores_mes, resultado_sensores_ano, resultado_sensores_total_mes, resultado_sensores_total_ano

def agrupar_api(df_final_api):
    df_final_api['_time'] = pd.to_datetime(df_final_api['_time'])
    df_final_api['Año'] = df_final_api['_time'].dt.year
    df_final_api['Mes'] = df_final_api['_time'].dt.month
    df_final_api['_value'] = df_final_api['_value'] / 1000
    resultado_api_mes = df_final_api.groupby(['Comunidad', 'Año', 'Mes'])['_value'].sum().reset_index()
    resultado_api_ano = df_final_api.groupby(['Comunidad', 'Año'])['_value'].sum().reset_index()
    resultado_api_total_ano = df_final_api.groupby(['Año'])['_value'].sum().reset_index()
    return resultado_api_mes, resultado_api_ano, resultado_api_total_ano

def agrupar_api_CO2(df_final_api_CO2):
    df_final_api_CO2['_time'] = pd.to_datetime(df_final_api_CO2['_time'])
    df_final_api_CO2['Año'] = df_final_api_CO2['_time'].dt.year
    df_final_api_CO2['Mes'] = df_final_api_CO2['_time'].dt.month
    df_final_api_CO2['_value'] = df_final_api_CO2['_value'] / 1000
    resultado_api_CO2_mes = df_final_api_CO2.groupby(['Año', 'Mes'])['_value'].sum().reset_index()
    resultado_api_CO2_ano = df_final_api_CO2.groupby(['Año'])['_value'].sum().reset_index()
    return resultado_api_CO2_mes, resultado_api_CO2_ano

def calcular_porcentaje_CO2_respecto_contaminacion_total_anual(resultado_sensores_total_ano, resultado_api_CO2_ano):
    resultado_sensores_total_ano_calculos = resultado_sensores_total_ano.rename(columns={'_value': 'Total_Value'})
    resultado_api_CO2_ano_calculos = resultado_api_CO2_ano.rename(columns={'_value': 'CO2_Value'})
    df_combined = pd.merge(resultado_sensores_total_ano_calculos, resultado_api_CO2_ano_calculos, on='Año')
    df_combined['CO2_Percentage'] = (df_combined['CO2_Value'] * 100) / df_combined['Total_Value']
    resultado_calculo_final = df_combined
    return resultado_calculo_final

def calcular_porcentaje_CO2_respecto_contaminacion_total_mensual(CO2_España_Total_sensores_Mes, resultado_api_CO2_mes):
    resultado_sensores_total_mes_calculos = CO2_España_Total_sensores_Mes.rename(columns={'_value': 'Total_Value'})
    resultado_api_CO2_mes_calculos = resultado_api_CO2_mes.rename(columns={'_value': 'CO2_Value'})
    df_combined = pd.merge(resultado_sensores_total_mes_calculos, resultado_api_CO2_mes_calculos, on=['Año', 'Mes'])
    df_combined['CO2_Percentage'] = (df_combined['CO2_Value'] * 100) / df_combined['Total_Value']
    resultado_calculo_final_mes = df_combined
    return resultado_calculo_final_mes

def calcular_porcentaje_contaminacion_de_cada_comunidad_respecto_a_españa_anual(CO2_España_Total_sensores_Año, CO2_Comunidad_Año):
    df_merged = pd.merge(CO2_España_Total_sensores_Año, CO2_Comunidad_Año, on='Año', suffixes=('_esp', '_com'))
    df_merged['percentage'] = (df_merged['_value_com'] * 100) / df_merged['_value_esp']
    # print(df_merged[['Comunidad_Autonoma', 'Año', 'percentage']])
    # total_percentage = df_merged['percentage'].sum()
    # print(f"La suma de los valores de percentage es: {total_percentage:.2f}")
    return df_merged

def calcular_porcentaje_contaminacion_de_cada_comunidad_respecto_a_españa_mensual(CO2_España_Total_sensores_Mes, CO2_Comunidad_Mes):
    df_merged = pd.merge(CO2_España_Total_sensores_Mes, CO2_Comunidad_Mes, on=['Año', 'Mes'], suffixes=('_esp', '_com'))
    df_merged['percentage'] = (df_merged['_value_com'] * 100) / df_merged['_value_esp']
    # print(df_merged[['Comunidad_Autonoma', 'Año', 'percentage']])
    # total_percentage = df_merged['percentage'].sum()
    # print(f"La suma de los valores de percentage es: {total_percentage:.2f}")
    return df_merged

def estructura_generacion_anual_generacion_total(df_generacion_total):
    df_generacion_total_mes = df_generacion_total.groupby(['Año', 'Mes', 'Comunidad'])['_value'].sum().reset_index()
    df_generacion_total_año = df_generacion_total.groupby(['Año', 'Comunidad'])['_value'].sum().reset_index()
    comunidad_espana_mes = df_generacion_total_mes[df_generacion_total_mes['Comunidad'] == 'España']
    df_generacion_total_mes['percentage'] = df_generacion_total_mes.apply(lambda row: (row['_value'] * 100 / comunidad_espana_mes['_value'].values[0]) if row['Comunidad'] != 'España' else None, axis=1)
    comunidad_espana_año = df_generacion_total_año[df_generacion_total_año['Comunidad'] == 'España']
    df_generacion_total_año['percentage'] = df_generacion_total_año.apply(lambda row: (row['_value'] * 100 / comunidad_espana_año['_value'].values[0]) if row['Comunidad'] != 'España' else None, axis=1)
    return df_generacion_total_mes, df_generacion_total_año

def estructura_generacion_anual_generacion_renovable(df_renovable):
    df_generacion_renovable_mes = df_renovable.groupby(['Año', 'Mes', 'Comunidad'])['_value'].sum().reset_index()
    df_generacion_renovable_año = df_renovable.groupby(['Año', 'Comunidad'])['_value'].sum().reset_index()
    comunidad_espana_mes = df_generacion_renovable_mes[df_generacion_renovable_mes['Comunidad'] == 'España']
    df_generacion_renovable_mes['percentage'] = df_generacion_renovable_mes.apply(lambda row: (row['_value'] * 100 / comunidad_espana_mes['_value'].values[0]) if row['Comunidad'] != 'España' else None, axis=1)
    comunidad_espana_año = df_generacion_renovable_año[df_generacion_renovable_año['Comunidad'] == 'España']
    df_generacion_renovable_año['percentage'] = df_generacion_renovable_año.apply(lambda row: (row['_value'] * 100 / comunidad_espana_año['_value'].values[0]) if row['Comunidad'] != 'España' else None, axis=1)
    return df_generacion_renovable_mes, df_generacion_renovable_año

def estructura_generacion_anual_generacion_norenovable(df_no_renovable):
    df_generacion_norenovable_mes = df_no_renovable.groupby(['Año', 'Mes', 'Comunidad'])['_value'].sum().reset_index()
    df_generacion_norenovable_año = df_no_renovable.groupby(['Año', 'Comunidad'])['_value'].sum().reset_index()
    comunidad_espana_mes = df_generacion_norenovable_mes[df_generacion_norenovable_mes['Comunidad'] == 'España']
    df_generacion_norenovable_mes['percentage'] = df_generacion_norenovable_mes.apply(lambda row: (row['_value'] * 100 / comunidad_espana_mes['_value'].values[0]) if row['Comunidad'] != 'España' else None, axis=1)
    comunidad_espana_año = df_generacion_norenovable_año[df_generacion_norenovable_año['Comunidad'] == 'España']
    df_generacion_norenovable_año['percentage'] = df_generacion_norenovable_año.apply(lambda row: (row['_value'] * 100 / comunidad_espana_año['_value'].values[0]) if row['Comunidad'] != 'España' else None, axis=1)
    return df_generacion_norenovable_mes, df_generacion_norenovable_año

def solucionar_problemas_renovables_norenovables_demanda(df_renovable, tipo):
    # Paso 1: Filtramos los datos que no son de España y los datos de España
    df_sin_espana = df_renovable[df_renovable['Comunidad'] != 'España']
    df_espana = df_renovable[df_renovable['Comunidad'] == 'España']
    # Paso 2: Calcular el porcentaje de generación de cada comunidad respecto al total de cada mes
    df_sin_espana = df_sin_espana.merge(
        df_espana[['Año', 'Mes', '_value']],
        on=['Año', 'Mes'],
        suffixes=('', '_total')
    )
    df_sin_espana['porcentaje'] = df_sin_espana['_value'] / df_sin_espana['_value_total']
    # Paso 3: Calcular la media de estos porcentajes para cada comunidad, para cada año (sin diciembre)
    mean_percentage = df_sin_espana[df_sin_espana['Mes'] != 12].groupby(['Año', 'Comunidad'])['porcentaje'].mean().reset_index()
    # Paso 4: Obtener el valor total de generación renovable para España en diciembre de cada año
    total_december_value = df_espana[df_espana['Mes'] == 12][['Año', '_value']]
    # Paso 5: Calcular los valores estimados de diciembre para cada comunidad, para cada año
    mean_percentage = mean_percentage.merge(total_december_value, on='Año')
    mean_percentage['estimated_value'] = mean_percentage['porcentaje'] * mean_percentage['_value']
    # print(f"mean_percentage {tipo}")
    # print(mean_percentage)
    # Crear DataFrame para los valores estimados de diciembre
    december_estimated_df = mean_percentage[['Año', 'Comunidad', 'estimated_value']]
    december_estimated_df['Mes'] = 12
    december_estimated_df['_time'] = december_estimated_df['Año'].astype(str) + '-12-31 23:00:00+00:00'
    december_estimated_df['Tipo'] = tipo
    # Renombrar la columna 'estimated_value' a '_value' para que coincida con el DataFrame original
    december_estimated_df.rename(columns={'estimated_value': '_value'}, inplace=True)
    # Paso 6: Filtrar las comunidades que ya tienen datos de diciembre
    # Obtenemos las comunidades que ya tienen datos para diciembre
    comunidades_con_datos_diciembre = df_renovable[(df_renovable['Mes'] == 12) & (df_renovable['Comunidad'] != 'España')]['Comunidad'].unique()
    # Filtramos las comunidades que ya tienen datos de diciembre
    december_estimated_df = december_estimated_df[~december_estimated_df['Comunidad'].isin(comunidades_con_datos_diciembre)]
    # Paso 7: Añadir los valores estimados al DataFrame original
    df_renovable_completo = pd.concat([df_renovable, december_estimated_df], ignore_index=True)
    # Verificamos el DataFrame actualizado
    return df_renovable_completo

def estructura_generacion_anual(df_estructura_generacion):
    df_estructura_generacion['Comunidad'] = df_estructura_generacion['Comunidad'].fillna('España')
    df_estructura_generacion = df_estructura_generacion.drop(columns=['result', 'table', '_start', '_stop', '_measurement', '_field', 'Categoria', 'Tema'])
    df_estructura_generacion['_time'] = pd.to_datetime(df_estructura_generacion['_time'])
    df_estructura_generacion['Año'] = df_estructura_generacion['_time'].dt.year
    df_estructura_generacion['Mes'] = df_estructura_generacion['_time'].dt.month
    df_estructura_generacion = df_estructura_generacion.drop(df_estructura_generacion[(df_estructura_generacion['Comunidad'] == 'Comunidad_18') | (df_estructura_generacion['Comunidad'] == 'Comunidad_19')].index)
    df_generacion_total = df_estructura_generacion[df_estructura_generacion['Tipo'] == 'Generación total']
    df_renovable = df_estructura_generacion[df_estructura_generacion['Tipo'] == 'Renovable']
    df_no_renovable = df_estructura_generacion[df_estructura_generacion['Tipo'] == 'No renovable']
    df_generacion_total_solucionado = solucionar_problemas_renovables_norenovables_demanda(df_generacion_total, "Generación total")
    df_renovable_solucionado = solucionar_problemas_renovables_norenovables_demanda(df_renovable, "Renovable")
    df_no_renovable_solucionado = solucionar_problemas_renovables_norenovables_demanda(df_no_renovable, "No renovable")
    estructura_generacion_mensual_total, estructura_generacion_anual_total = estructura_generacion_anual_generacion_total(df_generacion_total_solucionado)
    estructura_generacion_mensual_renovable, estructura_generacion_anual_renovable = estructura_generacion_anual_generacion_renovable(df_renovable_solucionado)
    estructura_generacion_mensual_norenovable, estructura_generacion_anual_norenovable = estructura_generacion_anual_generacion_norenovable(df_no_renovable_solucionado)
    return estructura_generacion_mensual_total, estructura_generacion_mensual_renovable, estructura_generacion_mensual_norenovable, estructura_generacion_anual_total, estructura_generacion_anual_renovable, estructura_generacion_anual_norenovable

def estructura_demanda(df_estructura_demanda):
    df_estructura_demanda['Comunidad'] = df_estructura_demanda['Comunidad'].fillna('España')
    df_estructura_demanda = df_estructura_demanda.drop(columns=['result', 'table', '_start', '_stop', '_measurement', '_field', 'Categoria', 'Tema'])
    df_estructura_demanda['_time'] = pd.to_datetime(df_estructura_demanda['_time'])
    df_estructura_demanda['Año'] = df_estructura_demanda['_time'].dt.year
    df_estructura_demanda['Mes'] = df_estructura_demanda['_time'].dt.month
    df_estructura_demanda = df_estructura_demanda.drop(df_estructura_demanda[(df_estructura_demanda['Comunidad'] == 'Comunidad_18') | (df_estructura_demanda['Comunidad'] == 'Comunidad_19')].index)
    df_estructura_demanda = df_estructura_demanda.groupby(['Año', 'Mes', 'Comunidad'], as_index=False)['_value'].sum()
    df_estructura_demanda.loc[df_estructura_demanda['Comunidad'] == 'España', '_value'] /= 2
    df_estructura_demanda_solucionado = solucionar_problemas_renovables_norenovables_demanda(df_estructura_demanda, "Demanda")
    df_demanda_total_mes = df_estructura_demanda_solucionado.groupby(['Año', 'Mes', 'Comunidad'])['_value'].sum().reset_index()
    df_demanda_total_año = df_estructura_demanda_solucionado.groupby(['Año', 'Comunidad'])['_value'].sum().reset_index()
    comunidad_espana_año = df_demanda_total_año[df_demanda_total_año['Comunidad'] == 'España']
    df_demanda_total_año['percentage'] = df_demanda_total_año.apply(lambda row: (row['_value'] * 100 / comunidad_espana_año['_value'].values[0]) if row['Comunidad'] != 'España' else None, axis=1)
    comunidad_espana_mes = df_demanda_total_mes[df_demanda_total_mes['Comunidad'] == 'España']
    df_demanda_total_mes['percentage'] = df_demanda_total_mes.apply(lambda row: (row['_value'] * 100 / comunidad_espana_mes['_value'].values[0]) if row['Comunidad'] != 'España' else None, axis=1)
    return df_demanda_total_mes, df_demanda_total_año

def mapeo_comunidades_rename():
    mapeo_comunidades = {
        'Andalucia': 'Andalucia',
        'Aragon': 'Aragon',
        'Asturias': 'Principado_de_Asturias',
        'C_Madrid': 'Comunidad_de_Madrid',
        'C_Valenciana': 'Comunidad_Valenciana',
        'Canarias': 'Canarias',
        'Cantabria': 'Cantabria',
        'Castilla_Leon': 'Castilla_y_Leon',
        'Castilla_Mancha': 'Castilla_La_Mancha',
        'Cataluna': 'Cataluna',
        'Ceuta': 'Ceuta',
        'Extremadura': 'Extremadura',
        'Galicia': 'Galicia',
        'Islas_Baleares': 'Islas_Baleares',
        'La_Rioja': 'La_Rioja',
        'Melilla': 'Melilla',
        'Navarra': 'Comunidad_Foral_de_Navarra',
        'Pais_Vasco': 'Pais_Vasco',
        'Region_Murcia': 'Region_de_Murcia'
    }
    return mapeo_comunidades

def ratio_energia_norenovable_a_emisiones_de_CO2_anual(CO2_Comunidad_Año, estructura_generacion_anual_norenovable):
    CO2_Comunidad_Año['Comunidad_Autonoma'] = CO2_Comunidad_Año['Comunidad_Autonoma'].replace(mapeo_comunidades)
    CO2_Comunidad_Año.rename(columns={'Comunidad_Autonoma': 'Comunidad', '_value': 'CO2_value'}, inplace=True)
    df_merged = pd.merge(estructura_generacion_anual_norenovable, CO2_Comunidad_Año, on=['Año', 'Comunidad'])
    df_merged['eficiencia_CO2(MWh/(CO2-eq (kt))'] = df_merged['_value'] / df_merged['CO2_value']
    ratio_energia_no_renovable_a_emisiones_de_CO2 = df_merged[['Año', 'Comunidad', 'eficiencia_CO2(MWh/(CO2-eq (kt))']]
    return ratio_energia_no_renovable_a_emisiones_de_CO2

def ratio_energia_norenovable_a_emisiones_de_CO2_mensual(CO2_Comunidad_Mes, estructura_generacion_mensual_norenovable):
    CO2_Comunidad_Mes['Comunidad_Autonoma'] = CO2_Comunidad_Mes['Comunidad_Autonoma'].replace(mapeo_comunidades)
    CO2_Comunidad_Mes.rename(columns={'Comunidad_Autonoma': 'Comunidad', '_value': 'CO2_value'}, inplace=True)
    df_merged = pd.merge(estructura_generacion_mensual_norenovable, CO2_Comunidad_Mes, on=['Año', 'Mes', 'Comunidad'])
    df_merged['eficiencia_CO2(MWh/(CO2-eq (kt))'] = df_merged['_value'] / df_merged['CO2_value']
    ratio_energia_no_renovable_a_emisiones_de_CO2 = df_merged[['Año', 'Mes', 'Comunidad', 'eficiencia_CO2(MWh/(CO2-eq (kt))']]
    return ratio_energia_no_renovable_a_emisiones_de_CO2

def intensidad_de_carbono_por_energia_generada_anual(CO2_Comunidad_Año, estructura_generacion_anual_total):
    merged_df = pd.merge(estructura_generacion_anual_total, CO2_Comunidad_Año, on=['Año', 'Comunidad'])
    merged_df['CO2_value_g'] = merged_df['CO2_value'] * 1_000_000
    merged_df['intensidad_carbono(g_CO₂/kWh)'] = merged_df['CO2_value_g'] / merged_df['_value']
    intensidad_carbono_por_energia_generada_anual = merged_df[['Año', 'Comunidad', 'intensidad_carbono(g_CO₂/kWh)']]
    return intensidad_carbono_por_energia_generada_anual

def intensidad_de_carbono_por_energia_generada_mensual(CO2_Comunidad_Mes, estructura_generacion_mensual_total):
    merged_df = pd.merge(estructura_generacion_mensual_total, CO2_Comunidad_Mes, on=['Año', 'Mes', 'Comunidad'])
    merged_df['CO2_value_g'] = merged_df['CO2_value'] * 1_000_000
    merged_df['intensidad_carbono(g_CO₂/kWh)'] = merged_df['CO2_value_g'] / merged_df['_value']
    intensidad_carbono_por_energia_generada_mensual = merged_df[['Año', 'Mes', 'Comunidad', 'intensidad_carbono(g_CO₂/kWh)']]
    return intensidad_carbono_por_energia_generada_mensual

def calcular_sostenibilidad_energetica_anual(estructura_generacion_anual_renovable, estructura_generacion_anual_norenovable):
    estructura_generacion_anual_renovable.rename(columns={'_value': 'generacion_renovable(MWh)'}, inplace=True)
    estructura_generacion_anual_norenovable.rename(columns={'_value': 'generacion_norenovable(MWh)'}, inplace=True)
    merged_df = pd.merge(estructura_generacion_anual_renovable, estructura_generacion_anual_norenovable, on=['Año', 'Comunidad'])
    merged_df['sostenibilidad_energetica(%)'] = merged_df['generacion_renovable(MWh)'] * 100 / merged_df['generacion_norenovable(MWh)']
    merged_df["generacion_total(MWh)"] = merged_df["generacion_renovable(MWh)"] + abs(merged_df["generacion_norenovable(MWh)"])
    merged_df["porcentaje_generacion_renovable(%)"] = (merged_df["generacion_renovable(MWh)"] * 100) / merged_df["generacion_total(MWh)"]
    merged_df["porcentaje_generacion_no_renovable(%)"] = (merged_df["generacion_norenovable(MWh)"] * 100) / merged_df["generacion_total(MWh)"]
    sostenibilidad_energetica_anual = merged_df[['Año', 'Comunidad', 'sostenibilidad_energetica(%)', 'porcentaje_generacion_renovable(%)', 'porcentaje_generacion_no_renovable(%)']]
    return sostenibilidad_energetica_anual

def calcular_sostenibilidad_energetica_mensual(estructura_generacion_mensual_renovable, estructura_generacion_mensual_norenovable):
    estructura_generacion_mensual_renovable.rename(columns={'_value': 'generacion_renovable(MWh)'}, inplace=True)
    estructura_generacion_mensual_norenovable.rename(columns={'_value': 'generacion_norenovable(MWh)'}, inplace=True)
    merged_df = pd.merge(estructura_generacion_mensual_renovable, estructura_generacion_mensual_norenovable, on=['Año', 'Mes', 'Comunidad'])
    merged_df['sostenibilidad_energetica(%)'] = merged_df['generacion_renovable(MWh)'] / merged_df['generacion_norenovable(MWh)']
    merged_df["generacion_total(MWh)"] = merged_df["generacion_renovable(MWh)"] + abs(merged_df["generacion_norenovable(MWh)"])
    merged_df["porcentaje_generacion_renovable(%)"] = (merged_df["generacion_renovable(MWh)"] * 100) / merged_df["generacion_total(MWh)"]
    merged_df["porcentaje_generacion_no_renovable(%)"] = (merged_df["generacion_norenovable(MWh)"] * 100) / merged_df["generacion_total(MWh)"]
    sostenibilidad_energetica_mensual = merged_df[['Año', 'Mes', 'Comunidad', 'sostenibilidad_energetica(%)', 'porcentaje_generacion_renovable(%)', 'porcentaje_generacion_no_renovable(%)']]
    return sostenibilidad_energetica_mensual

def calcular_balance_resto_energetico_anual(estructura_generacion_anual_total, estructura_demanda_anual_total):
    df_resultado = estructura_generacion_anual_total.copy()  # Copia para mantener los datos originales
    df_resultado['balance_energetico(%)'] = estructura_generacion_anual_total['_value'] * 100 / estructura_demanda_anual_total['_value']
    df_resultado['resto_energetico(MWh)'] = estructura_generacion_anual_total['_value'] - estructura_demanda_anual_total['_value']
    df_resultado = df_resultado.drop(['_value', 'percentage'], axis=1)
    return df_resultado

def calcular_balance_resto_energetico_mensual(estructura_generacion_mensual_total, estructura_demanda_mensual_total):
    df_resultado = estructura_generacion_mensual_total.copy()  # Copia para mantener los datos originales
    df_resultado['balance_energetico(%)'] = estructura_generacion_mensual_total['_value'] * 100 / estructura_demanda_mensual_total['_value']
    df_resultado['resto_energetico(MWh)'] = estructura_generacion_mensual_total['_value'] - estructura_demanda_mensual_total['_value']
    df_resultado = df_resultado.drop(['_value', 'percentage'], axis=1)
    return df_resultado

def clean_column_name(name):
    # Expresión regular para eliminar el texto entre paréntesis
    return re.sub(r"\(.*?\)", "", name).strip()

def get_last_day_of_month(year, month):
    # Obtener el último día del mes
    last_day = calendar.monthrange(year, month)[1]
    return last_day

def create_timestamp(row):
    # Extraer el año y el mes de la fila
    year = int(row['anno'])
    month = int(row['Mes'])
    # Obtener el último día del mes
    last_day = get_last_day_of_month(year, month)
    # Crear la cadena de fecha y hora
    date_str = f"{year}-{month:02d}-{last_day} 23:59:00.000"
    # Convertir la cadena a un objeto datetime
    timestamp_influxdb = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f")
    return timestamp_influxdb

def load_data_anual(df_completo_anual):
    df_completo_anual.columns = [clean_column_name(col) for col in df_completo_anual.columns]
    df_completo_anual.rename(columns=lambda x: clean_column_name(x), inplace=True)
    df_completo_anual = df_completo_anual.rename(columns={'Año': 'anno'})
    df_completo_anual = df_completo_anual.rename(columns={'eficiencia_CO2)': 'eficiencia_CO2'})
    df_completo_anual = df_completo_anual.rename(columns={'CO2_España)': 'CO2_España'})
    df_completo_anual = df_completo_anual.rename(columns={'CO2_Comunidad)': 'CO2_Comunidad'})
    df_completo_anual = df_completo_anual.rename(columns={'CO2_comunidad_ajustado)': 'CO2_comunidad_ajustado'})
    puntos_acumulados = []
    contador = 0
    for index, row in df_completo_anual.iterrows(): 
        timestamp_influxdb = datetime.strptime(f"{row.anno}-12-31 23:59:00.000", "%Y-%m-%d %H:%M:%S.%f")
        point = influxdb_client.Point("anual")
        point = point.tag("Comunidad", row.Comunidad)
        point = point.field("generacion_renovable", row.generacion_renovable)
        point = point.field("porcentaje_renovable", row.porcentaje_renovable)
        point = point.field("generacion_norenovable", row.generacion_norenovable)
        point = point.field("porcentaje_no_renovable", row.porcentaje_no_renovable)
        point = point.field("demanda", row.demanda)
        point = point.field("porcentaje_demanda", row.porcentaje_demanda)
        point = point.field("generacion", row.generacion)
        point = point.field("porcentaje_generacion", row.porcentaje_generacion)
        point = point.field("eficiencia_CO2", row.eficiencia_CO2)
        point = point.field("CO2_España", row.CO2_España)
        point = point.field("CO2_Comunidad", row.CO2_Comunidad)
        point = point.field("porcentaje_CO2_Comunidad", row.porcentaje_CO2_Comunidad)
        point = point.field("intensidad_carbono", row.intensidad_carbono)
        point = point.field("sostenibilidad_energetica", row.sostenibilidad_energetica)
        point = point.field("balance_energetico", row.balance_energetico)
        point = point.field("resto_energetico", row.resto_energetico)
        point = point.field("porcentaje_generacion_renovable", row.porcentaje_generacion_renovable)
        point = point.field("porcentaje_generacion_no_renovable", row.porcentaje_generacion_no_renovable)
        point = point.field("CO2_comunidad_ajustado", row.CO2_comunidad_ajustado)
        point = point.field("CO2_porcentaje_generacion", row.CO2_porcentaje_generacion)
        point = point.field("CO2_ajustado_solo_generacion", row.CO2_ajustado_solo_generacion)
        point = point.time(timestamp_influxdb)

        puntos_acumulados.append(point)
        contador = contador +1
    
    write_api.write(bucket="etl_data", org=org, record=puntos_acumulados)
    print(f'Carga anual Finalizada!!! {contador}')

def load_data_mensual(df_completo_mensual):
    df_completo_mensual.columns = [clean_column_name(col) for col in df_completo_mensual.columns]
    df_completo_mensual.rename(columns=lambda x: clean_column_name(x), inplace=True)
    df_completo_mensual = df_completo_mensual.rename(columns={'Año': 'anno'})
    df_completo_mensual = df_completo_mensual.rename(columns={'eficiencia_CO2)': 'eficiencia_CO2'})
    df_completo_mensual = df_completo_mensual.rename(columns={'CO2_España)': 'CO2_España'})
    df_completo_mensual = df_completo_mensual.rename(columns={'CO2_Comunidad)': 'CO2_Comunidad'})
    df_completo_mensual = df_completo_mensual.rename(columns={'CO2_comunidad_ajustado)': 'CO2_comunidad_ajustado'})
    puntos_acumulados = []
    contador = 0
    for index, row in df_completo_mensual.iterrows():     
        timestamp_influxdb = create_timestamp(row)
        point = influxdb_client.Point("mensual")
        point = point.tag("Comunidad", row.Comunidad)
        point = point.field("generacion_renovable", row.generacion_renovable)
        point = point.field("porcentaje_renovable", row.porcentaje_renovable)
        point = point.field("generacion_norenovable", row.generacion_norenovable)
        point = point.field("porcentaje_no_renovable", row.porcentaje_no_renovable)
        point = point.field("demanda", row.demanda)
        point = point.field("porcentaje_demanda", row.porcentaje_demanda)
        point = point.field("generacion", row.generacion)
        point = point.field("porcentaje_generacion", row.porcentaje_generacion)
        point = point.field("eficiencia_CO2", row.eficiencia_CO2)
        point = point.field("CO2_España", row.CO2_España)
        point = point.field("CO2_Comunidad", row.CO2_Comunidad)
        point = point.field("porcentaje_CO2_Comunidad", row.porcentaje_CO2_Comunidad)
        point = point.field("intensidad_carbono", row.intensidad_carbono)
        point = point.field("sostenibilidad_energetica", row.sostenibilidad_energetica)
        point = point.field("balance_energetico", row.balance_energetico)
        point = point.field("resto_energetico", row.resto_energetico)
        point = point.field("porcentaje_generacion_renovable", row.porcentaje_generacion_renovable)
        point = point.field("porcentaje_generacion_no_renovable", row.porcentaje_generacion_no_renovable)
        point = point.field("CO2_comunidad_ajustado", row.CO2_comunidad_ajustado)
        point = point.field("CO2_porcentaje_generacion", row.CO2_porcentaje_generacion)
        point = point.field("CO2_ajustado_solo_generacion", row.CO2_ajustado_solo_generacion)
        point = point.time(timestamp_influxdb)

        puntos_acumulados.append(point)
        contador = contador +1
    
    write_api.write(bucket="etl_data", org=org, record=puntos_acumulados)
    print(f'Carga mensual Finalizada!!! {contador}')

def load_data_porcentaje_anual(resultado_calculo_final_España):
    puntos_acumulados = []
    contador = 0
    resultado_calculo_final_España['Año'] = resultado_calculo_final_España['Año'].astype(int)
    for index, row in resultado_calculo_final_España.iterrows():
        timestamp_influxdb = datetime.strptime(f"{int(row.Año)}-12-31 23:59:00.000", "%Y-%m-%d %H:%M:%S.%f")
        point = influxdb_client.Point("indice_contaminacion_global_anual")
        point = point.field("CO2_Percentage", row.CO2_Percentage)
        point = point.time(timestamp_influxdb)

        puntos_acumulados.append(point)
        contador = contador +1
    
    write_api.write(bucket="etl_data", org=org, record=puntos_acumulados)
    print(f'Carga indice_contaminacion_global_anual Finalizada!!! {contador}')

def load_data_porcentaje_mensual(resultado_calculo_final_España_mensual):
    puntos_acumulados = []
    contador = 0
    resultado_calculo_final_España_mensual = resultado_calculo_final_España_mensual.rename(columns={'Año': 'anno'})
    for index, row in resultado_calculo_final_España_mensual.iterrows():
        timestamp_influxdb = create_timestamp(row)
        point = influxdb_client.Point("indice_contaminacion_global_mensual")
        point = point.field("CO2_Percentage", row.CO2_Percentage)
        point = point.time(timestamp_influxdb)

        puntos_acumulados.append(point)
        contador = contador +1
    
    write_api.write(bucket="etl_data", org=org, record=puntos_acumulados)
    print(f'Carga indice_contaminacion_global_mensual Finalizada!!! {contador}')


##########
###MAIN###
##########

# Consulta InfluxDB en lenguaje Flux
query_api = client.query_api()
# Consulta InfluxDB en lenguaje Flux
start_time = datetime.strptime("2017-01-01T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ")
end_time = datetime.strptime("2018-01-15T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ")
interval = timedelta(hours=24)
start_total_time = time.time()
df_final_sensores = None
df_final_api = None
df_final_api_CO2 = None
df_estructura_generacion = None
df_estructura_demanda = None


current_start_time = start_time
while current_start_time < end_time:
    current_end_time = current_start_time + interval
    if current_end_time > end_time:
        current_end_time = end_time

    # Extracción sensores CO2
    df_sensores, duration_query = query_influxdb_sensores(client, current_start_time, current_end_time)
    df_final_sensores = pd.concat([df_final_sensores, df_sensores])

    # Extracción generacion no renovable = 100% CO2
    df_api, duration_query_api = query_influxdb_api(client, current_start_time, current_end_time)
    df_final_api = pd.concat([df_final_api, df_api])

    # Extracción api total CO2
    df_api_total_CO2, duration_query_api_CO2 = query_influx_api_totalCO2(client, current_start_time, current_end_time)
    df_final_api_CO2 = pd.concat([df_final_api_CO2, df_api_total_CO2])

    # generacion_total_renovable_norenovale
    df_generacion, duration_generacion = query_influx_api_generacion_total_renovable_norenovale(client, current_start_time, current_end_time)
    df_estructura_generacion = pd.concat([df_estructura_generacion, df_generacion])
    # df_estructura_generacion.to_csv('ETL.csv', index=False, encoding='utf-8')

    df_demanda, duration_demanda = query_influx_api_demanda_total(client, current_start_time, current_end_time)
    df_estructura_demanda = pd.concat([df_estructura_demanda, df_demanda])

    current_start_time = current_end_time

end_total_time = time.time()
print(f"Tiempo total: {end_total_time - start_total_time} segundos")


# Transformación sensores
CO2_Comunidad_Mes, CO2_Comunidad_Año, CO2_España_Total_sensores_Mes, CO2_España_Total_sensores_Año = agrupar_sensores(df_final_sensores)
resultado_api_mes, resultado_api_ano, resultado_api_total_ano = agrupar_api(df_final_api)
resultado_api_CO2_mes, resultado_api_CO2_ano = agrupar_api_CO2(df_final_api_CO2)

resultado_calculo_final_España_anual = calcular_porcentaje_CO2_respecto_contaminacion_total_anual(CO2_España_Total_sensores_Año, resultado_api_CO2_ano)
resultado_calculo_final_España_mensual = calcular_porcentaje_CO2_respecto_contaminacion_total_mensual(CO2_España_Total_sensores_Mes, resultado_api_CO2_mes)

ultimos_calculos_CO2_merged = pd.merge(CO2_Comunidad_Año, resultado_calculo_final_España_anual, on='Año', how='left')
ultimos_calculos_CO2_merged['CO2_ajustado_solo_generacion'] = ultimos_calculos_CO2_merged['_value'] * (ultimos_calculos_CO2_merged['CO2_Percentage'] / 100)
ultimos_calculos_CO2_merged = ultimos_calculos_CO2_merged.rename(columns={'_value': 'CO2_Total', 'CO2_Percentage': 'CO2_porcentaje_generacion'})
ultimos_calculos_CO2_merged_año = ultimos_calculos_CO2_merged.dropna()
CO2_Comunidad_Año_ajustado = ultimos_calculos_CO2_merged_año[['Comunidad_Autonoma', 'Año', 'CO2_ajustado_solo_generacion']]
CO2_Comunidad_Año_ajustado = CO2_Comunidad_Año_ajustado.rename(columns={'CO2_ajustado_solo_generacion': '_value'})

ultimos_calculos_CO2_merged_mensual = pd.merge(CO2_Comunidad_Mes, resultado_calculo_final_España_mensual, on=['Año', 'Mes'], how='left')
ultimos_calculos_CO2_merged_mensual['CO2_ajustado_solo_generacion'] = ultimos_calculos_CO2_merged_mensual['_value'] * (ultimos_calculos_CO2_merged_mensual['CO2_Percentage'] / 100)
ultimos_calculos_CO2_merged_mensual = ultimos_calculos_CO2_merged_mensual.rename(columns={'_value': 'CO2_Total', 'CO2_Percentage': 'CO2_porcentaje_generacion'})
ultimos_calculos_CO2_merged_mensual = ultimos_calculos_CO2_merged_mensual.dropna()
CO2_Comunidad_mes_ajustado = ultimos_calculos_CO2_merged_mensual[['Comunidad_Autonoma', 'Año', 'Mes', 'CO2_ajustado_solo_generacion']]
CO2_Comunidad_mes_ajustado = CO2_Comunidad_mes_ajustado.rename(columns={'CO2_ajustado_solo_generacion': '_value'})


mapeo_comunidades = mapeo_comunidades_rename()

porcentaje_contaminacion_comunidad_respecto_españa_anual = calcular_porcentaje_contaminacion_de_cada_comunidad_respecto_a_españa_anual(CO2_España_Total_sensores_Año, CO2_Comunidad_Año)
porcentaje_contaminacion_comunidad_respecto_españa_anual['Comunidad_Autonoma'] = porcentaje_contaminacion_comunidad_respecto_españa_anual['Comunidad_Autonoma'].replace(mapeo_comunidades)
porcentaje_contaminacion_comunidad_respecto_españa_anual = porcentaje_contaminacion_comunidad_respecto_españa_anual.rename(columns={'Comunidad_Autonoma': 'Comunidad', '_value_esp': 'CO2_España(CO2-eq(kt))', '_value_com': 'CO2_Comunidad(CO2-eq(kt))', 'percentage': 'porcentaje_CO2_Comunidad(%)'})

porcentaje_contaminacion_comunidad_respecto_españa_mensual = calcular_porcentaje_contaminacion_de_cada_comunidad_respecto_a_españa_mensual(CO2_España_Total_sensores_Mes, CO2_Comunidad_Mes)
porcentaje_contaminacion_comunidad_respecto_españa_mensual['Comunidad_Autonoma'] = porcentaje_contaminacion_comunidad_respecto_españa_mensual['Comunidad_Autonoma'].replace(mapeo_comunidades)
porcentaje_contaminacion_comunidad_respecto_españa_mensual = porcentaje_contaminacion_comunidad_respecto_españa_mensual.rename(columns={'Comunidad_Autonoma': 'Comunidad', '_value_esp': 'CO2_España(CO2-eq(kt))', '_value_com': 'CO2_Comunidad(CO2-eq(kt))', 'percentage': 'porcentaje_CO2_Comunidad(%)'})

estructura_generacion_mensual_total, estructura_generacion_mensual_renovable, estructura_generacion_mensual_norenovable, estructura_generacion_anual_total, estructura_generacion_anual_renovable, estructura_generacion_anual_norenovable = estructura_generacion_anual(df_estructura_generacion)
estructura_demanda_mensual_total, estructura_demanda_anual_total = estructura_demanda(df_estructura_demanda)

ratio_energia_no_renovable_a_emisiones_de_CO2_anual = ratio_energia_norenovable_a_emisiones_de_CO2_anual(CO2_Comunidad_Año_ajustado, estructura_generacion_anual_norenovable)
ratio_energia_no_renovable_a_emisiones_de_CO2_mensual = ratio_energia_norenovable_a_emisiones_de_CO2_mensual(CO2_Comunidad_mes_ajustado, estructura_generacion_mensual_norenovable)

intensidad_carbono_por_energia_generada_anual = intensidad_de_carbono_por_energia_generada_anual(CO2_Comunidad_Año_ajustado, estructura_generacion_anual_total)
intensidad_carbono_por_energia_generada_mensual = intensidad_de_carbono_por_energia_generada_mensual(CO2_Comunidad_mes_ajustado, estructura_generacion_mensual_total)

sostenibilidad_energetica_anual = calcular_sostenibilidad_energetica_anual(estructura_generacion_anual_renovable, estructura_generacion_anual_norenovable)
sostenibilidad_energetica_mensual = calcular_sostenibilidad_energetica_mensual(estructura_generacion_mensual_renovable, estructura_generacion_mensual_norenovable)

balance_resto_energetico_anual = calcular_balance_resto_energetico_anual(estructura_generacion_anual_total, estructura_demanda_anual_total)
balance_resto_energetico_mensual = calcular_balance_resto_energetico_mensual(estructura_generacion_mensual_total, estructura_demanda_mensual_total)

#############
###Uniones###
#############

estructura_demanda_anual_total = estructura_demanda_anual_total.rename(columns={'_value': 'demanda(MWh)', 'percentage': 'porcentaje_demanda(%)'})
estructura_generacion_anual_total = estructura_generacion_anual_total.rename(columns={'_value': 'generacion(MWh)', 'percentage': 'porcentaje_generacion(%)'})
estructura_generacion_anual_renovable = estructura_generacion_anual_renovable.rename(columns={'_value': 'generacion_renovable', 'percentage': 'porcentaje_renovable(%)'})
estructura_generacion_anual_norenovable = estructura_generacion_anual_norenovable.rename(columns={'_value': 'generacion_no_renovable', 'percentage': 'porcentaje_no_renovable(%)'})
ultimos_calculos_CO2_merged_año = ultimos_calculos_CO2_merged_año.rename(columns={'Comunidad_Autonoma': 'Comunidad'})
ultimos_calculos_CO2_merged_año['Comunidad'] = ultimos_calculos_CO2_merged_año['Comunidad'].replace(mapeo_comunidades)

df_mitad_anual_1 = pd.merge(estructura_generacion_anual_renovable, estructura_generacion_anual_norenovable, on=['Año', 'Comunidad'])
df_mitad_anual_2 = pd.merge(estructura_demanda_anual_total, estructura_generacion_anual_total, on=['Año', 'Comunidad'])
df_mitad_anual_1_2 = pd.merge(df_mitad_anual_1, df_mitad_anual_2, on=['Año', 'Comunidad'])
df_mitad_anual_3 = pd.merge(ratio_energia_no_renovable_a_emisiones_de_CO2_anual, porcentaje_contaminacion_comunidad_respecto_españa_anual, on=['Año', 'Comunidad'])
df_mitad_anual_4 = pd.merge(intensidad_carbono_por_energia_generada_anual, sostenibilidad_energetica_anual, on=['Año', 'Comunidad'])
df_mitad_anual_3_4 = pd.merge(df_mitad_anual_3, df_mitad_anual_4, on=['Año', 'Comunidad'])
df_mitad_anual_3_4_5 = pd.merge(df_mitad_anual_3_4, balance_resto_energetico_anual, on=['Año', 'Comunidad'])
df_completo_anual = pd.merge(df_mitad_anual_1_2, df_mitad_anual_3_4_5, on=['Año', 'Comunidad'])
df_completo_anual = pd.merge(ultimos_calculos_CO2_merged_año, df_completo_anual, on=['Año', 'Comunidad'])
df_completo_anual = df_completo_anual.rename(columns={'CO2_Total': 'CO2_comunidad_total', 'Total_Value': 'CO2_estatal', 'CO2_Value': 'CO2_comunidad_ajustado(CO2-eq(kt))', 'CO2_porcentaje_generacion': 'CO2_porcentaje_generacion(%)', ' CO2_ajustado_solo_generacion': ' CO2_ajustado_solo_generacion(CO2-eq(kt))'})
df_completo_anual = df_completo_anual.drop(columns=['CO2_comunidad_total', 'CO2_estatal'])
print(df_completo_anual)
# print("---------------------------------------")

estructura_demanda_mensual_total = estructura_demanda_mensual_total.rename(columns={'_value': 'demanda(MWh)', 'percentage': 'porcentaje_demanda(%)'})
estructura_generacion_mensual_total = estructura_generacion_mensual_total.rename(columns={'_value': 'generacion(MWh)', 'percentage': 'porcentaje_generacion(%)'})
estructura_generacion_mensual_renovable = estructura_generacion_mensual_renovable.rename(columns={'_value': 'generacion_renovable', 'percentage': 'porcentaje_renovable(%)'})
estructura_generacion_mensual_norenovable = estructura_generacion_mensual_norenovable.rename(columns={'_value': 'generacion_no_renovable', 'percentage': 'porcentaje_no_renovable(%)'})
ultimos_calculos_CO2_merged_mensual = ultimos_calculos_CO2_merged_mensual.rename(columns={'Comunidad_Autonoma': 'Comunidad'})
ultimos_calculos_CO2_merged_mensual['Comunidad'] = ultimos_calculos_CO2_merged_mensual['Comunidad'].replace(mapeo_comunidades)

df_mitad_mensual_1 = pd.merge(estructura_generacion_mensual_renovable, estructura_generacion_mensual_norenovable, on=['Año', 'Mes', 'Comunidad'])
df_mitad_mensual_2 = pd.merge(estructura_demanda_mensual_total, estructura_generacion_mensual_total, on=['Año', 'Mes', 'Comunidad'])
df_mitad_mensual_1_2 = pd.merge(df_mitad_mensual_1, df_mitad_mensual_2, on=['Año', 'Mes', 'Comunidad'])
df_mitad_mensual_3 = pd.merge(ratio_energia_no_renovable_a_emisiones_de_CO2_mensual, porcentaje_contaminacion_comunidad_respecto_españa_mensual, on=['Año', 'Mes', 'Comunidad'])
df_mitad_mensual_4 = pd.merge(intensidad_carbono_por_energia_generada_mensual, sostenibilidad_energetica_mensual, on=['Año', 'Mes', 'Comunidad'])
df_mitad_mensual_3_4 = pd.merge(df_mitad_mensual_3, df_mitad_mensual_4, on=['Año', 'Mes', 'Comunidad'])
df_mitad_mensual_3_4_5 = pd.merge(df_mitad_mensual_3_4, balance_resto_energetico_mensual, on=['Año', 'Mes', 'Comunidad'])
df_completo_mensual = pd.merge(df_mitad_mensual_1_2, df_mitad_mensual_3_4_5, on=['Año', 'Mes', 'Comunidad'])
df_completo_mensual = pd.merge(ultimos_calculos_CO2_merged_mensual, df_completo_mensual, on=['Año', 'Mes', 'Comunidad'])
df_completo_mensual = df_completo_mensual.rename(columns={'CO2_Total': 'CO2_comunidad_total', 'Total_Value': 'CO2_estatal', 'CO2_Value': 'CO2_comunidad_ajustado(CO2-eq(kt))', 'CO2_porcentaje_generacion': 'CO2_porcentaje_generacion(%)', ' CO2_ajustado_solo_generacion': ' CO2_ajustado_solo_generacion(CO2-eq(kt))'})
df_completo_mensual = df_completo_mensual.drop(columns=['CO2_comunidad_total', 'CO2_estatal'])
print(df_completo_mensual)


###########
###Carga###
###########

load_data_anual(df_completo_anual)
load_data_mensual(df_completo_mensual)
load_data_porcentaje_anual(resultado_calculo_final_España_anual)
load_data_porcentaje_mensual(resultado_calculo_final_España_mensual)

##############
###Capturas###
##############

# columnas = df_completo_anual.columns
# print(columnas)

# columnas_mes = df_completo_mensual.columns
# print(columnas_mes)


# print("resultado_calculo_final_España_anual")
# print(resultado_calculo_final_España_anual)
# print("resultado_calculo_final_España_mensual")
# print(resultado_calculo_final_España_mensual)
# print("-----------------")
# Calculos
# print("estructura_generacion_anual_renovable")
# print(estructura_generacion_anual_renovable)
# print("estructura_generacion_anual_norenovable")
# print(estructura_generacion_anual_norenovable)
# print("estructura_generacion_anual_total")
# print(estructura_generacion_anual_total)
# print("estructura_demanda_anual_total")
# print(estructura_demanda_anual_total)
print("ratio_energia_no_renovable_a_emisiones_de_CO2_anual")
print(ratio_energia_no_renovable_a_emisiones_de_CO2_anual)
print("porcentaje_contaminacion_comunidad_respecto_españa_anual")
print(porcentaje_contaminacion_comunidad_respecto_españa_anual)
print("intensidad_carbono_por_energia_generada_anual")
print(intensidad_carbono_por_energia_generada_anual)
# print("sostenibilidad_energetica_anual")
# print(sostenibilidad_energetica_anual)
# print("balance_resto_energetico_anual")
# print(balance_resto_energetico_anual)
