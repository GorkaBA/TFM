import pandas as pd
import numpy as np
from scipy.signal import savgol_filter

# Especifica la ruta de tu archivo Excel
ruta_archivo = '/mnt/data/emisiones_gei.xlsx'

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
    window_length = 31  # Longitud de la ventana (asegúrate de que sea un número impar)
    poly_order = 3      # Orden del polinomio
    df_daily['Valor_Suavizado'] = savgol_filter(df_daily['Valor'], window_length=window_length, polyorder=poly_order)
    
    # Iterar sobre los años y generar valores diarios para cada sensor
    for year in df_daily.index.year.unique():
        year = int(year)
        if year not in valores_anuales:
            print(f"Advertencia: {year} no está en el diccionario valores_anuales.")
            continue
        
        df_year = df_daily[df_daily.index.year == year]
        num_dias = len(df_year)
        valor_total_anual = valores_anuales[year]
        
        # Generar datos para cada mes
        for month in range(1, 13):
            df_month = df_year[df_year.index.month == month]
            valor_mensual = valor_total_anual / 12
            valor_diario_teorico = valor_mensual / len(df_month)
            sum_valores_mensuales = 0
            
            for num_sensor in range(1, 6):
                for fecha in df_month.index:
                    # Variación aleatoria para cada día
                    variacion = np.random.uniform(-0.05, 0.05)
                    valor_suavizado = df_month.loc[fecha, 'Valor_Suavizado'] / (num_dias / 12)
                    valor_final = round(valor_suavizado * (1 + variacion), 2)
                    registros.append({
                        "Fecha": fecha, 
                        "Comunidad_Autonoma": comunidad, 
                        "Sensor": f"Sensor_{num_sensor}", 
                        "Valor": valor_final
                    })
                    sum_valores_mensuales += valor_final

            # Ajuste proporcional
            ajuste = (valor_mensual - sum_valores_mensuales) / len(df_month) / 5
            for i in range(len(registros) - len(df_month) * 5, len(registros)):
                registros[i]["Valor"] += ajuste

# Crear un DataFrame a partir de la lista de registros
df_final = pd.DataFrame(registros)

# Verificar la suma mensual generada con los valores reales
df_final['Mes'] = df_final['Fecha'].dt.to_period('M')
resumen = df_final.groupby(['Comunidad_Autonoma', 'Mes']).agg({'Valor': 'sum'}).reset_index()

# Comprobar y ajustar
for comunidad in resumen['Comunidad_Autonoma'].unique():
    for mes in resumen[resumen['Comunidad_Autonoma'] == comunidad]['Mes'].unique():
        valor_real = df[df["COMUNIDAD_AUTONOMA"] == comunidad][mes.year].values[0] / 12
        valor_generado = resumen[(resumen['Comunidad_Autonoma'] == comunidad) & (resumen['Mes'] == mes)]['Valor'].values[0]
        diferencia = valor_real - valor_generado
        print(f"Comunidad: {comunidad}, Mes: {mes}, Valor Real: {valor_real}, Valor Generado: {valor_generado}, Diferencia: {diferencia}")

# Guardar el DataFrame en un archivo CSV
df_final.to_csv('/mnt/data/emisiones_diarias_gei_suavizado_ajustado.csv', index=False)