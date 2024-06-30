import yagmail


# Configuración del email
yag = yagmail.SMTP("gorka.berganza852@comunidadunir.net", "A7Vwl5QQXdBMAUMgXITI")
to = "iker.iborra844@comunidadunir.net"
subject = "EMAIL DE PRUEBA"
body = "Este es el cuerpo del email."

# Enviar el email
try:
    yag.send(to=to, subject=subject, contents=body)
    print("Email enviado exitosamente.")
except Exception as e:
    print(f"Error al enviar el email: {e}")
