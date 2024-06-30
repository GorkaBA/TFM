import requests
import json

def consulta_balance_electrico():
    api_url = "https://apidatos.ree.es/es/datos/balance/balance-electrico?end_date=2023-12-31T23:59&time_trunc=day&start_date=2023-01-01T00:00"
    print(f"Consultando API: {api_url}")
    
    # Obtener datos de la API
    response = requests.get(api_url)

    # Verificar si la solicitud fue exitosa
    if response.status_code != 200:
        print(f"Error al obtener datos de la API:", response.status_code)
        print("Mensaje:", response.text)
        return
    
    # Parsear los datos a JSON
    try:
        data = response.json()
        print(json.dumps(data, indent=4))  # Imprimir datos en formato JSON con indentación para legibilidad
    except ValueError as e:
        print(f"Error al parsear la respuesta JSON: {e}")

if __name__ == "__main__":
    consulta_balance_electrico()
