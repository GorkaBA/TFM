import pandas as pd
import numpy as np

# Especifica la ruta de tu archivo Excel
ruta_archivo = '/home/azureuser/emisiones.xlsx'

# Lee el archivo Excel y carga los datos en un DataFrame
df = pd.read_excel(ruta_archivo)

# Iterar sobre las columnas para dividir cada valor en cinco sensores
for columna in df.columns[1:]:
    sensores = []
    for valor in df[columna]:
        sensor = [valor * (0.85 + np.random.rand() * 0.3) / 5 for _ in range(5)]
        sensor[-1] += valor - sum(sensor)  # Ajuste para asegurar que la suma sea igual al valor original
        sensores.append(sensor)

    # Generar 365 (o 366) días de datos para cada sensor
    fecha_inicio = pd.to_datetime('01-01-1990')  # Cambia la fecha según sea necesario
    fecha_final = pd.to_datetime('31-12-2022')  # Cambia la fecha según sea necesario
    rango_fechas = pd.date_range(start=fecha_inicio, end=fecha_final)

    datos_por_dia = []
    for fecha in rango_fechas:
        for sensor in sensores:
            datos_por_dia.append(sensor)

    # Crear un DataFrame con los datos por día y agregarlo al DataFrame original
    df_temp = pd.DataFrame(datos_por_dia, columns=[f"{columna}_sensor_{i+1}" for i in range(5)])
    df_temp['Fecha'] = pd.to_datetime(rango_fechas)
    df_temp.set_index('Fecha', inplace=True)
    df_temp.reset_index(inplace=True)
    df = pd.merge(df, df_temp, on='Fecha', how='outer')

df.to_excel("emisiones_sensores.xlsx", index=False)