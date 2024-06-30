import pandas as pd
import numpy as np

# Especifica la ruta de tu archivo Excel
ruta_archivo = '/home/azureuser/emisiones.xlsx'

# Lee el archivo Excel y carga los datos en un DataFrame
df = pd.read_excel(ruta_archivo)

# Crear una lista para almacenar los registros
# Crear una lista para almacenar los registros
# Crear una lista para almacenar los registros
registros = []

# Iterar sobre las filas del DataFrame
for index, row in df.iterrows():
    comunidad = row["COMUNIDAD AUTONOMA"]
    for year, value in row.items():
        if year != "COMUNIDAD AUTONOMA":
            # Dividir el valor del sensor entre 5 para obtener el valor de cada sensor
            valor_sensor = value / 5
            # Generar valores para 5 sensores en total para cada año
            for num_sensor in range(1, 6):
                # Obtener el número de días del año (365 o 366)
                num_dias = 366 if pd.to_datetime(f"{year}-12-31").is_leap_year else 365
                # Dividir el valor del sensor entre el número de días del año
                valor_por_dia = valor_sensor / num_dias
                # Iterar sobre cada día del año
                for dia in pd.date_range(start=f"{year}-01-01", end=f"{year}-12-31"):
                    # Generar un valor aleatorio entre -15% y +15%
                    variacion = np.random.uniform(-0.15, 0.15)
                    # Calcular el valor del sensor para este día con la variación
                    valor_final = valor_por_dia * (1 + variacion)
                    # Agregar el registro a la lista
                    registros.append({"Fecha": dia, "Comunidad Autónoma": comunidad, "Sensor": f"Sensor {num_sensor}", "Valor": valor_final})

# Crear un DataFrame a partir de la lista de registros
df_final = pd.DataFrame(registros)

# Mostrar el DataFrame resultante
print(df_final)

# Verificar si la suma de los sensores es igual al valor inicial por día
# for index, row in df.iterrows():
#     comunidad = row["COMUNIDAD AUTONOMA"]
#     for year, value in row.items():
#         if year != "COMUNIDAD AUTONOMA":
#             suma_sensores = df_final[(df_final["Fecha"].dt.year == int(year)) & 
#                                      (df_final["Comunidad Autónoma"] == comunidad)]["Valor"].sum()
#             print(f"Año: {year}, Comunidad Autónoma: {comunidad}, Suma de sensores: {suma_sensores}, Valor inicial: {value}")
# Mostrar el DataFrame resultante
print(df_final)
# df_final.to_excel("emisiones_sensores_2.xlsx", index=False)
df_final.to_csv(ruta_csv = '/home/azureuser/archivo.csv', index=False)