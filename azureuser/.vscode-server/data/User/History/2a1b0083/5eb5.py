import pandas as pd 
# Ruta completa del archivo CSV

ruta_csv = "/home/azureuser/emisiones.xlsx"
df = pd.read_csv(ruta_csv) 

print(df)