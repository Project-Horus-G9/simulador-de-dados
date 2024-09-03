import numpy as np
import random
import json
import uuid
import time
import service.serviceBus as sb
import service.sendEmail as sd
import os
from datetime import datetime, timedelta
from azure.iot.device import IoTHubDeviceClient, Message

def gerar_potencia():
    horas_do_dia = np.linspace(0, 24, 100)
    potencia_maxima_NOCT = 400
    percentual_captacao = 0.4

    potencia_captada = np.random.uniform(0, potencia_maxima_NOCT * percentual_captacao, len(horas_do_dia))
    potencia_captada[horas_do_dia < 6] *= 0.1
    potencia_captada[horas_do_dia > 18] *= 0.1

    return potencia_captada.tolist()


def gerar_voltagem():
    leituras = []
    for i in range(0, 1):
        for i in range(0, 24):
            random_number = random.uniform(39, 40)
            leituras.append(random_number)

    return leituras


def gerar_luminosidade(num_dias, num_setores):
    dados = {}
    data_atual = datetime.now().date()
    for dia in range(num_dias):
        data_dia = data_atual + timedelta(days=dia)
        for setor in range(1, num_setores + 1):
            for hora in range(24):
                for minuto in range(0, 60, 30):
                    luminosidade = None
                    if hora in range(6, 18):
                        luminosidade = random.uniform(750, 1000) + random.uniform(-10, 10)
                    elif hora in range(18, 20) or hora in range(4, 6):
                        luminosidade = random.uniform(300, 500) + random.uniform(-10, 10)
                    elif hora in range(5, 6) or hora in range(4, 6):
                        luminosidade = random.uniform(500, 100) + random.uniform(-10, 10)
                    else:
                        luminosidade = random.uniform(0, 50) + random.uniform(-10, 10)
                    chave = f"Painel {setor}"
                    if chave not in dados:
                        dados[chave] = []
                    dados[chave].append({
                        "data": data_dia.strftime("%Y-%m-%d"),
                        "hora": f"{hora:02}:{minuto:02}",
                        "luminosidade": luminosidade
                    })
    return dados


def gerar_temperatura(estacao, hora_atual):
    if estacao == "Verão":
        temperatura_externa = random.uniform(30, 35)
    elif estacao == "Outono":
        temperatura_externa = random.uniform(25, 30)
    elif estacao == "Inverno":
        temperatura_externa = random.uniform(20, 25)
    else:
        temperatura_externa = random.uniform(25, 30)

    temperatura_interna = calcular_temperatura(temperatura_externa, hora_atual)
    return temperatura_interna, temperatura_externa


def obter_estacao(mes):
    if mes in [12, 1, 2]:
        return "Verão"
    elif mes in [3, 4, 5]:
        return "Outono"
    elif mes in [6, 7, 8]:
        return "Inverno"
    else:
        return "Primavera"


def calcular_temperatura(temperatura_externa, hora_atual):
    if hora_atual.time() >= datetime.strptime('10:00:00', '%H:%M:%S').time() and hora_atual.time() <= datetime.strptime(
            '16:00:00', '%H:%M:%S').time():
        return round(random.uniform(temperatura_externa, temperatura_externa + 23), 2)
    else:
        return round(random.uniform(temperatura_externa, temperatura_externa + 20), 2)


def envio_azure(dados_hora):
    conn_str = "HostName=hub-horus.azure-devices.net;DeviceId=horusdevice;SharedAccessKey=SJsfu2G5W+F2d+f1iKWm4FhQEMZ70yFWZAIoTIEn9jI="
    device_client = IoTHubDeviceClient.create_from_connection_string(conn_str)

    print("IoTHub Device Client Recurring Telemetry Sample")
    print("Press Ctrl+C to exit")

    device_client.connect()

    msg_dict = dados_hora
    msg = Message(json.dumps(msg_dict))
    msg.message_id = uuid.uuid4()
    msg.correlation_id = "correlation-1234"
    msg.content_encoding = "utf-8"
    msg.content_type = "application/json"

    print(f"Enviando mensagem: {msg_dict}")
    device_client.send_message(msg)

    print("User initiated exit")
    device_client.shutdown()


def gerar_dados(num_paineis):

    connection_string = os.environ.get('CONNECTION_STR')
    queue_name = os.environ.get('QUEUE_NAME')
    
    dados = {}
    potencia_captada = gerar_potencia()
    voltagem = gerar_voltagem()
    luminosidade = gerar_luminosidade(1, num_paineis)

    data_atual = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    estacao_atual = obter_estacao(data_atual.month)

    for hora in range(24):
        for meia_hora in range(0, 60, 30):
            hora_atual = data_atual + timedelta(hours=hora, minutes=meia_hora)
            dados_hora = {}
            dados[hora_atual.strftime("%Y-%m-%d %H:%M")] = []
            alerta = False

            for painel in range(1, num_paineis + 1):
                temperatura_interna, temperatura_externa = gerar_temperatura(estacao_atual, hora_atual)
                if temperatura_interna > temperatura_externa + 21:
                    alerta = True
                dados_hora[f"Painel {painel}"] = {
                    "data": hora_atual.strftime("%Y-%m-%d"),
                    "hora": hora_atual.strftime("%H:%M"),
                    "luminosidade":
                        luminosidade[f"Painel {painel}"][random.randint(0, len(luminosidade[f"Painel {painel}"]) - 1)][
                            "luminosidade"],
                    "temperatura_externa": temperatura_externa,
                    "temperatura_interna": temperatura_interna,
                    "potencia": potencia_captada[random.randint(0, len(potencia_captada) - 1)],
                    "voltagem": voltagem[random.randint(0, len(voltagem) - 1)]
                }
                dados[hora_atual.strftime("%Y-%m-%d %H:%M")].append({
                    "Painel": f"{painel}",
                    "luminosidade":
                        luminosidade[f"Painel {painel}"][random.randint(0, len(luminosidade[f"Painel {painel}"]) - 1)][
                            "luminosidade"],
                    "temperatura_externa": temperatura_externa,
                    "temperatura_interna": temperatura_interna,
                    "potencia": potencia_captada[random.randint(0, len(potencia_captada) - 1)],
                    "voltagem": voltagem[random.randint(0, len(voltagem) - 1)]
                })

            if alerta:
                sb.send_message(connection_string, queue_name)
                print("Enviando alerta")
                mensagem = str(sb.receiver_message(connection_string, queue_name))
                sd.enviar_email(mensagem)

            print(dados_hora)
            envio_azure(dados_hora)
            time.sleep(2)
    return dados

def salvar_dados_json(num_paineis):
    dados = gerar_dados(num_paineis)
    num = len([name for name in os.listdir('dados_gerados') if name.endswith(".json")])
    with open('dados_gerados/dados' + str(num) + '.json', 'w') as arquivo:
        json.dump(dados, arquivo, indent=4)

    print("Dados salvos com sucesso em 'dados_gerados.json'.")

def pegar_env():
    with open("config.env") as f:
        for line in f:
            if line.strip():
                if not line.startswith("#"):  
                    key = line.split('=')[0]
                    if len(line.split('=')) > 2:
                        value = '='.join(line.split('=')[1:]).strip()
                    else:
                        value = line.split('=')[1].strip()
                        
                    if key in os.environ:
                        print(f"Key {key} already exists in environment variables")
                    if key is None or value is None:
                        print("Key or value is None")
                    else:
                        os.environ[key] = value

if __name__ == "__main__":
    pegar_env()
    
    salvar_dados_json(4)