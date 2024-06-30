import pandas as pd

# Especifica la ruta de tu archivo Excel
ruta_archivo = '/home/azureuser/emisiones.xlsx'

# Lee el archivo Excel y carga los datos en un DataFrame
datos = pd.read_excel(ruta_archivo)

# Ahora puedes trabajar con los datos como desees, por ejemplo:
# Imprimir las primeras filas del DataFrame
print(datos.head())
