import pandas as pd
import numpy as np

# Especifica la ruta de tu archivo Excel
ruta_archivo = '/home/azureuser/emisiones.xlsx'

# Lee el archivo Excel y carga los datos en un DataFrame
df = pd.read_excel(ruta_archivo)

registros = []

# Iterar sobre las filas del DataFrame
for index, row in df.iterrows():
    comunidad = row["COMUNIDAD AUTONOMA"]
    for year, value in row.items():
        if year != "COMUNIDAD AUTONOMA":
            # Generar valores para 5 sensores en total para cada año
            for num_sensor in range(1, 6):
                # Obtener el número de días del año (365 o 366)
                num_dias = 366 if pd.to_datetime(f"{year}-12-31").is_leap_year else 365
                # Dividir el valor del sensor entre el número de días del año
                valor_por_dia = value / num_dias
                # Sumar los valores de los sensores para este año
                suma_sensores = 0
                # Iterar sobre cada día del año
                for dia in pd.date_range(start=f"{year}-01-01", end=f"{year}-12-31"):
                    # Generar un valor aleatorio entre -15% y +15%
                    variacion = np.random.uniform(-0.15, 0.15)
                    # Calcular el valor del sensor para este día con la variación
                    valor_sensor = valor_por_dia * (1 + variacion)
                    # Agregar el valor del sensor a la suma
                    suma_sensores += valor_sensor
                # Agregar el registro a la lista
                registros.append({"Año": int(year), "Comunidad Autónoma": comunidad, "Sensor": f"Sensor {num_sensor}", "Suma Sensores": suma_sensores})

# Crear un DataFrame a partir de la lista de registros
df_final = pd.DataFrame(registros)

# Mostrar el DataFrame resultante
print(df_final)

# Verificar si la suma de los sensores es igual al valor inicial
for index, row in df.iterrows():
    comunidad = row["COMUNIDAD AUTONOMA"]
    for year, value in row.items():
        if year != "COMUNIDAD AUTONOMA":
            suma_sensores = df_final[(df_final["Año"] == int(year)) & (df_final["Comunidad Autónoma"] == comunidad)]["Suma Sensores"].sum()
            print(f"Año: {year}, Comunidad Autónoma: {comunidad}, Suma de sensores: {suma_sensores}, Valor inicial: {value}")

# Mostrar el DataFrame resultante
print(df_final)
# df_final.to_excel("emisiones_sensores_2.xlsx", index=False)