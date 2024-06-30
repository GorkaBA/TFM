import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

def send_email(sender_email, sender_password, recipient_email, subject, body, attachment_path=None):
    try:
        # Configurar el servidor SMTP para Gmail
        smtp_server = "smtp.gmail.com"
        smtp_port = 587

        # Crear un objeto MIMEMultipart para el correo electrónico
        message = MIMEMultipart()
        message['From'] = sender_email
        message['To'] = recipient_email
        message['Subject'] = subject

        # Adjuntar el cuerpo del correo electrónico al mensaje
        message.attach(MIMEText(body, 'plain'))

        # Adjuntar un archivo si se proporciona un path
        if attachment_path:
            attachment = open(attachment_path, "rb")
            part = MIMEBase('application', 'octet-stream')
            part.set_payload((attachment).read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f"attachment; filename= {attachment_path}")
            message.attach(part)

        # Iniciar la conexión al servidor SMTP
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)

        # Enviar el correo electrónico
        text = message.as_string()
        server.sendmail(sender_email, recipient_email, text)

        # Cerrar la conexión al servidor SMTP
        server.quit()

        print(f"Correo enviado a {recipient_email} exitosamente.")

    except Exception as e:
        print(f"Error al enviar el correo: {str(e)}")

if __name__ == "__main__":
    # Configura estos valores según sea necesario
    sender_email = "gorka.berganza852@comunidadunir.net"
    sender_password = "contrasenaSegura_123"
    recipient_email = "gorkaberganza2001@gmail.com"
    subject = "Asunto del correo"
    body = "Este es el cuerpo del correo electrónico."
    attachment_path = None  # Pon el path al archivo adjunto, o deja como None

    send_email(sender_email, sender_password, recipient_email, subject, body, attachment_path)
