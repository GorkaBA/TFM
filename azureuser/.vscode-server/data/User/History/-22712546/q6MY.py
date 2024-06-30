import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Configuración del servidor SMTP del servicio temporal (ejemplo)
smtp_server = "smtp.mailtrap.io"  # Cambia esto al servidor SMTP de tu proveedor temporal
smtp_port = 2525  # Cambia esto al puerto utilizado por tu proveedor temporal

# Configuración del mensaje
from_email = "yirek98131@gawte.com"
to_email = "gorkaberganza2001@gmail.com"
subject = "EMAIL DE PRUEBA"
body = "Este es el cuerpo del email."

# Crear el contenedor del mensaje
msg = MIMEMultipart()
msg['From'] = from_email
msg['To'] = to_email
msg['Subject'] = subject

# Adjuntar el cuerpo del mensaje
msg.attach(MIMEText(body, 'plain'))

# Enviar el mensaje
try:
    server = smtplib.SMTP(smtp_server, smtp_port)
    # Si el servidor no requiere autenticación, no llames a login()
    # server.login(smtp_user, smtp_password)  # Omitido porque no se necesita
    text = msg.as_string()
    server.sendmail(from_email, to_email, text)
    server.quit()
    print("Email enviado exitosamente.")
except Exception as e:
    print(f"Error al enviar el email: {e}")
