import pandas as pd
import numpy as np
from scipy.ndimage import uniform_filter1d, gaussian_filter1d
from scipy.signal import savgol_filter

# Especifica la ruta de tu archivo Excel
ruta_archivo = '/home/azureuser/emisiones_2.xlsx'

# Lee el archivo Excel y carga los datos en un DataFrame
df = pd.read_excel(ruta_archivo)

# Crear una lista para almacenar los registros
registros = []

# Iterar sobre las filas del DataFrame
for index, row in df.iterrows():
    comunidad = row["COMUNIDAD AUTONOMA"]
    valores_anuales = {year: value for year, value in row.items() if year != "COMUNIDAD AUTONOMA"}
    
    # Obtener el rango de años
    años = sorted(valores_anuales.keys())
    
    for year, value in valores_anuales.items():
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
                # Generar un valor aleatorio entre -5% y +5%
                variacion = np.random.uniform(-0.05, 0.05)
                # Calcular el valor del sensor para este día con la variación
                valor_final = round(valor_por_dia * (1 + variacion), 2)
                # Agregar el registro a la lista
                registros.append({"Fecha": dia, "Comunidad_Autonoma": comunidad, "Sensor": f"Sensor_{num_sensor}", "Valor": valor_final})

# Crear un DataFrame a partir de la lista de registros
df_final = pd.DataFrame(registros)

# Ordenar los datos por 'Fecha'
df_final = df_final.sort_values(by='Fecha')

# Aplicar interpolación para suavizar transiciones abruptas
df_final["Valor"] = df_final["Valor"].interpolate(method='linear')

# Aplicar una media móvil extensa para suavizar las variaciones
df_final["Valor"] = df_final["Valor"].rolling(window=365).mean()

# Aplicar un filtro Gaussian con un sigma significativo
sigma = 100
df_final["Valor"] = gaussian_filter1d(df_final["Valor"], sigma=sigma)

# Aplicar el filtro de Savitzky-Golay
window_length = 51  # Ajusta este valor según la longitud de tus datos
polyorder = 2
df_final["Valor"] = savgol_filter(df_final["Valor"], window_length, polyorder)

# Crear transiciones suaves entre diferentes años
years = df_final['Fecha'].dt.year.unique()

for i in range(len(years) - 1):
    current_year_data = df_final[df_final['Fecha'].dt.year == years[i]]
    next_year_data = df_final[df_final['Fecha'].dt.year == years[i + 1]]
    
    if not next_year_data.empty:
        transition_value = (current_year_data["Valor"].iloc[-30:].mean() + next_year_data["Valor"].iloc[:30].mean()) / 2
        
        df_final.loc[current_year_data.index[-30:], "Valor"] = np.linspace(
            current_year_data["Valor"].iloc[-30:].mean(), transition_value, 30
        )
        df_final.loc[next_year_data.index[:30], "Valor"] = np.linspace(
            transition_value, next_year_data["Valor"].iloc[:30].mean(), 30
        )

# Mostrar el DataFrame resultante
print(df_final)

# Guardar el DataFrame en un archivo CSV
df_final.to_csv('/home/azureuser/archivo_2.csv', index=False)
