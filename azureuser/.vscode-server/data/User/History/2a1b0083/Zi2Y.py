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
    # Agregar los sensores como nuevas columnas en el DataFrame
    for i in range(5):
        df[f"{columna}_sensor_{i+1}"] = [s[i] for s in sensores]

# Mostrar el DataFrame resultante
print(df)