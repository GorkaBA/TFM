import pandas as pd
import numpy as np

# Especifica la ruta de tu archivo Excel
ruta_archivo = '/home/azureuser/emisiones.xlsx'

# Lee el archivo Excel y carga los datos en un DataFrame
df = pd.read_excel(ruta_archivo)

# Iterar sobre las columnas para dividir cada valor en cinco sensores
for columna in df.columns[1:]:
    for i in range(len(df)):
        valor = df.at[i, columna]
        sensores = [valor * (0.85 + np.random.rand() * 0.3) / 5 for _ in range(5)]
        sensores[-1] += valor - sum(sensores)  # Ajuste para asegurar que la suma sea igual al valor original
        df.at[i, columna] = sensores

# Mostrar el DataFrame resultante
print(df)