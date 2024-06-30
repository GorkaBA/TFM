import yagmail


# Configuración del email
#yag = yagmail.SMTP("gorka.berganza852@comunidadunir.net", "contrasenaSegura_123", host="smtp.office365.com", port=587, smtp_starttls=True, smtp_ssl=False)
yag = yagmail.SMTP("yirek98131@gawte.com")
to = "gorkaberganza2001@gmail.com"
#to = "iker.iborra844@comunidadunir.net"
subject = "EMAIL DE PRUEBA"
body = "Este es el cuerpo del email."

# Enviar el email
try:
    yag.send(to=to, subject=subject, contents=body)
    print("Email enviado exitosamente.")
except Exception as e:
    print(f"Error al enviar el email: {e}")
