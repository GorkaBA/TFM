
El error que estás experimentando ahora indica que la longitud de los datos de los sensores que estás intentando asignar a df_temp no coincide con la longitud del índice del DataFrame, lo cual causa un desajuste en la asignación de columnas. Esto puede estar ocurriendo debido a que la longitud de sensores (20) no coincide con la longitud de datos_por_dia (12053).

Este problema surge porque estamos intentando asignar los datos de los sensores directamente a df_temp, pero olvidamos replicar los datos de los sensores para cada día. Para resolver esto, necesitamos repetir los datos de los sensores para cada día. Aquí está la corrección:

python
Copiar código
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
        datos_por_dia.extend(sensores)  # Repetir los datos de los sensores para cada día

    # Crear un DataFrame con los datos por día y agregarlo al DataFrame original
    df_temp = pd.DataFrame(datos_por_dia, columns=[f"{columna}_sensor_{i+1}" for i in range(5)])
    df_temp['Fecha'] = rango_fechas.repeat(5 * len(sensores))  # Repetir las fechas para cada conjunto de sensores
    df_temp.set_index('Fecha', inplace=True)

    df = pd.concat([df, df_temp], axis=1)

# Mostrar el DataFrame resultante
print(df)
df.to_excel("emisiones_sensores.xlsx", index=False)