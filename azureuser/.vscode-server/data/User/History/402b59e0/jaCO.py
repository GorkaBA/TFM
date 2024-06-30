from codecarbon import EmissionsTracker

# Iniciar el rastreador de emisiones
tracker = EmissionsTracker()
tracker.start()

# Detener el rastreador de emisiones
emissions = tracker.stop()

print(f"Emisiones de carbono estimadas: {emissions} kg CO2")