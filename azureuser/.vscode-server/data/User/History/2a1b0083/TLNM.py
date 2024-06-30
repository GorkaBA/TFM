import pandas as pd
import numpy as np

# Especifica la ruta de tu archivo Excel
ruta_archivo = '/home/azureuser/emisiones.xlsx'

# Lee el archivo Excel y carga los datos en un DataFrame
datos = pd.read_excel(ruta_archivo)

# Función para dividir un valor en cinco sensores con variación de ±15%
def dividir_valor(valor):
    sensores = [valor * (0.85 + np.random.rand() * 0.3) / 5 for _ in range(5)]
    sensores[-1] += valor - sum(sensores)  # Ajuste para asegurarse de que la suma sea igual al valor original
    return sensores

# Aplicar la función a cada valor en el DataFrame
datos_divididos = datos.apply(lambda x: pd.Series(dividir_valor(x['Valor'])), axis=1)
datos_divididos.columns = ['Sensor_1', 'Sensor_2', 'Sensor_3', 'Sensor_4', 'Sensor_5']

# Concatenar los DataFrames resultantes
datos_final = pd.concat([datos[['Año', 'Comunidad']], datos_divididos], axis=1)

# Imprimir los primeros registros del DataFrame final
print(datos_final.head())