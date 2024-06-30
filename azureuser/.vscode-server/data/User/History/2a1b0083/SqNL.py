import pandas as pd
import numpy as np

# Especifica la ruta de tu archivo Excel
ruta_archivo = '/home/azureuser/emisiones.xlsx'

# Lee el archivo Excel y carga los datos en un DataFrame
df = pd.read_excel(ruta_archivo)

# Iterar sobre las columnas para dividir cada valor en cinco sensores
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
    # Agregar los sensores por día como nuevas columnas en el DataFrame
    for i in range(5):
        for j in range(365):
            df[f"{columna}_sensor_{i+1}_dia_{j+1}"] = [s[i] for s in sensores_por_dia]

# Mostrar el DataFrame resultante
print(df)
# df.to_excel("emisiones_sensores.xlsx", index=False)