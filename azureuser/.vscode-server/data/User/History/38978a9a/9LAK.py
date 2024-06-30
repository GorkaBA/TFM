import requests
from var import nombres  # Importar la lista nombres desde config.py

# Lista para almacenar los resultados
resultados = []

def consulta_api(anno, tipo_demanda):
    try:
        api_url = f"https://apidatos.ree.es/es/datos/{tipo_demanda[0]}/{tipo_demanda[1]}?end_date={anno}-12-31T23:59&time_trunc={tipo_demanda[2]}&start_date={anno}-01-01T04:00"
        
        print(f"Consultando API: {api_url}")
        # Obtener datos de la API
        response = requests.get(api_url)
        data = response.json()

        # Verificar si la solicitud fue exitosa
        if response.status_code != 200:
            print(f"Error al obtener datos de la API para el año {anno}:", response.status_code)
            print("Mensaje:", response.text)
            return None, None
        else:
            data_type = data.get('data', {}).get('type', 'N/A')
            description = data.get('data', {}).get('attributes', {}).get('description', 'N/A')
            return data_type, description
    except Exception as e:
        print(f"Error procesando {tipo_demanda[0]}/{tipo_demanda[1]}: {e}")
        return None, None

for anno in range(2022, 2023):
    for elemento in nombres:
        data_type, description = consulta_api(anno, elemento)
        if data_type is not None and description is not None:
            resultados.append(f"Type: {elemento[0]}, Subtype: {elemento[1]}, Data Type: {data_type}, Description: {description}")

# Guardar los resultados en un archivo de texto
with open('resultados.txt', 'w') as file:
    for resultado in resultados:
        file.write(resultado + "\n")

print("Resultados almacenados en 'resultados.txt'.")
