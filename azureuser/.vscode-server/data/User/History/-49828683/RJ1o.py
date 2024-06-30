import pandas as pd
import numpy as np
from scipy.signal import savgol_filter

# Especifica la ruta de tu archivo Excel
ruta_archivo = '/home/azureuser/emisiones_gei.xlsx'

# Lee el archivo Excel y carga los datos en un DataFrame
df = pd.read_excel(ruta_archivo)

# Crear una lista para almacenar los registros
registros = []

# Iterar sobre las filas del DataFrame
for index, row in df.iterrows():
    comunidad = row["COMUNIDAD_AUTONOMA"]
    valores_anuales = {int(year): value for year, value in row.items() if year != "COMUNIDAD_AUTONOMA"}
    
    # Crear un DataFrame temporal para los valores anuales
    df_temp = pd.DataFrame(list(valores_anuales.items()), columns=['Año', 'Valor'])
    df_temp['Fecha'] = pd.to_datetime(df_temp['Año'].astype(str) + '-01-01')
    
    # Crear una serie temporal diaria interpolada a partir de los valores anuales
    df_temp.set_index('Fecha', inplace=True)
    df_daily = df_temp.resample('D').interpolate(method='linear')
    
    # Suavizar la serie temporal diaria con Savitzky-Golay
    window_length = min(31, len(df_daily))  # Ajustar ventana a longitud de datos
    poly_order = 3
    if len(df_daily) > window_length:
        df_daily['Valor_Suavizado'] = savgol_filter(df_daily['Valor'], window_length=window_length, polyorder=poly_order)
    else:
        df_daily['Valor_Suavizado'] = df_daily['Valor']  # No suavizar si datos insuficientes
    
    # Iterar sobre los años y generar valores diarios para cada sensor
    for year in df_daily.index.year.unique():
        # Convertir year a entero para asegurar compatibilidad
        year = int(year)
        print(f"Procesando el año: {year}")
        
        if year not in valores_anuales:
            print(f"Advertencia: {year} no está en el diccionario valores_anuales.")
            continue
        
        df_year = df_daily[df_daily.index.year == year]
        num_dias = len(df_year)
        
        for num_sensor in range(1, 6):
            # Obtener el valor total del año y dividirlo entre 5 sensores
            valor_total_anual = valores_anuales[year] / 5
            valor_diario_teorico = valor_total_anual / num_dias
            
            for fecha in df_year.index:
                # Variación aleatoria para cada día
                variacion = np.random.uniform(-0.05, 0.05)
                valor_suavizado = df_year.loc[fecha, 'Valor_Suavizado'] / num_dias
                valor_suavizado = valor_suavizado / 120  # Ajuste adicional
                valor_final = round(valor_diario_teorico * (1 + variacion), 2)
                registros.append({
                    "Fecha": fecha, 
                    "Comunidad_Autonoma": comunidad, 
                    "Sensor": f"Sensor_{num_sensor}", 
                    "Valor": valor_final
                })

# Crear un DataFrame a partir de la lista de registros
df_final = pd.DataFrame(registros)

# Mostrar el DataFrame resultante
print(df_final)

# Guardar el DataFrame en un archivo CSV
df_final.to_csv('/home/azureuser/emisiones_prueba_3_gei_suavizado.csv', index=False)
