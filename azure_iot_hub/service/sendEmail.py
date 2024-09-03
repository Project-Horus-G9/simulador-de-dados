import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from dotenv import load_dotenv

def enviar_email(corpo):
    
    load_dotenv()

    sender_email = os.environ.get('SENDER_EMAIL')
    smtp_server = os.environ.get('SMTP_SERVER')
    smtp_port = os.environ.get('SMTP_PORT')
    smtp_password = os.environ.get('SMTP_PASSWORD')

    receiver_email = 'marco.magalhaes@sptech.school'
    subject = 'Alerta de temperatura'
    body = corpo


    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = receiver_email
    message['Subject'] = subject


    message.attach(MIMEText(body, 'plain'))


    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, smtp_password)
        server.sendmail(sender_email, receiver_email, message.as_string())
        print("Enviado")

        server.quit()
    except Exception as e:
        print(f"Houve um erro: {e}")
    finally:
        print("-------------")
