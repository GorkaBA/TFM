import pandas as pd
import numpy as np

# Especifica la ruta de tu archivo Excel
ruta_archivo = '/home/azureuser/emisiones.xlsx'

# Lee el archivo Excel y carga los datos en un DataFrame
df = pd.read_excel(ruta_archivo)

# Crear una lista para almacenar las nuevas columnas
nuevas_columnas = []

# Iterar sobre las columnas para dividir cada valor en cinco sensores y luego por día
for columna in df.columns[1:]:
    sensores_por_dia = []
    for valor in df[columna]:
        # Dividir el valor del sensor entre el número de sensores (5) para obtener el valor de cada sensor
        valor_por_sensor = valor / 5
        sensores = []
        for _ in range(5):
            # Generar un valor aleatorio entre -15% y +15%
            variacion = np.random.uniform(-0.15, 0.15)
            # Calcular el valor del sensor con la variación
            valor_sensor = valor_por_sensor * (1 + variacion)
            sensores.append(valor_sensor)
        sensores_por_dia.append(sensores)
    # Agregar los sensores por día como nuevas columnas a la lista
    for i in range(5):
        nuevas_columnas.append([f"{columna}_sensor_{i+1}_dia_{j+1}" for j in range(365)])
        nuevas_columnas.append([s[i] for s in sensores_por_dia])

# Concatenar todas las nuevas columnas al DataFrame
for columna, valores in zip(nuevas_columnas[::2], nuevas_columnas[1::2]):
    df[columna] = valores

# Mostrar el DataFrame resultante
print(df)