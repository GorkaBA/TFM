import pandas as pd
import numpy as np

# Especifica la ruta de tu archivo Excel
ruta_archivo = '/home/azureuser/emisiones.xlsx'

# Lee el archivo Excel y carga los datos en un DataFrame
df = pd.read_excel(ruta_archivo)

# Crear una lista para almacenar los registros
registros = []

# Iterar sobre las filas del DataFrame
for index, row in df.iterrows():
    comunidad = row["COMUNIDAD AUTONOMA"]
    for year, value in row.items():
        if year != "COMUNIDAD AUTONOMA":
            # Obtener el número de días del año (365 o 366)
            num_dias = 366 if pd.to_datetime(f"{year}-12-31").is_leap_year else 365
            # Dividir el valor del sensor entre el número de días del año
            valor_por_dia = value / num_dias
            # Iterar sobre cada día del año
            for dia in pd.date_range(start=f"{year}-01-01", end=f"{year}-12-31"):
                # Generar un valor aleatorio entre -15% y +15%
                variacion = np.random.uniform(-0.15, 0.15)
                # Calcular el valor del sensor para este día con la variación
                valor_sensor = valor_por_dia * (1 + variacion)
                # Agregar el registro a la lista
                registros.append({"Fecha": dia, "Comunidad Autónoma": comunidad, "Valor": valor_sensor})

# Crear un DataFrame a partir de la lista de registros
df_final = pd.DataFrame(registros)

# Mostrar el DataFrame resultante
print(df_final)
df_final.to_excel("emisiones_sensores_2.xlsx", index=False)