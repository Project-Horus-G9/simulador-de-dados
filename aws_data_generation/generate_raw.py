import numpy as np
import random
import os
import json
import time
from dotenv import load_dotenv
from datetime import datetime
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

class Simulator:
    
    def __init__(self):
        load_dotenv()
        
        self.bucket_name = 'raw-horus'
        self.client = AWSIoTMQTTClient("Simulador")
        
        path_credentials_iot_core = "./credentials/iot_core/"
        
        credentials = self.get_credentials(path_credentials_iot_core)
        
        certificate, private_key, root_ca = None, None, None
        for credential in credentials:
            if "certificate" in credential:
                certificate = credential
            elif "private" in credential:
                private_key = credential
            elif "Root" in credential:
                root_ca = credential

        if not (certificate and private_key and root_ca):
            raise ValueError("Certificado, chave privada ou CA root não foram encontrados nas credenciais.")
                
        endpoint = os.getenv("ENDPOINT")
        port = int(os.getenv("PORT"))
                
        self.client.configureEndpoint(endpoint, port)
        self.client.configureCredentials(root_ca, private_key, certificate)
        
        self.client.configureConnectDisconnectTimeout(10)
        self.client.configureMQTTOperationTimeout(5)
    
    def get_credentials(self, path_credentials):
        return [path_credentials + f for f in os.listdir(path_credentials)]
    
    def data_group(self, info):

        empresa = info["empresa"]
        num_dados = info["num_dados"]
        num_setores = info["num_setores"]
        num_paineis = info["num_paineis"]
        info_paineis = {
            "direcionamento": info["direcionamento"],
            "inclinacao": info["inclinacao"]
        }

        dados = {
            "cliente": empresa,
            "setores": []
        }
        
        for i in range(num_setores):
            setor = {
                "setor": i + 1,
                "paineis": []
            }
            dados["setores"].append(setor)
            
            for j in range(num_paineis):
                painel = {
                    "painel": j + 1,
                    "dados": []
                }
                setor["paineis"].append(painel)
            
        possiveis_ceus = ["ceu limpo", "algumas nuvens", "chuva leve", "nublado", "nuvens dispersas"]
        
        
        for i in range(num_dados):
            print(f"Gerando dados {i+1} de {num_dados}...")
            
            if i != 0 or i+1 != num_dados:
                time.sleep(60)
            
            for setor in dados["setores"]:
                if i % 2 == 0:
                    ceu = random.choice(possiveis_ceus)
                
                num_paineis_totais = num_paineis * num_setores
                
                info_geracao = {
                    "indice": i,
                    "num_paineis_totais": num_paineis_totais,
                    "ceu": ceu,
                    "info_paineis": info_paineis
                }
                
                dados_gerados = self.generate_data(info_geracao)
                for painel in setor["paineis"]:
                    painel["dados"].append(dados_gerados.pop(0))
                
        return dados
    
    def generate_data(self, info):
        num_paineis = info["num_paineis_totais"]
        ceu = info["ceu"]
        direcionamento = info["info_paineis"]["direcionamento"]
        inclinacao = info["info_paineis"]["inclinacao"]
        
        # Data e hora atual
        data_atual = datetime.now()
        data_formatada = data_atual.strftime('%Y-%m-%d %H:%M:%S')

        hora = int(data_formatada.split(" ")[1].split(":")[0])
        mes = int(data_formatada.split("-")[1])
        
        # Obstrução
        conjunto_obstrucao = []
        for i in range(num_paineis):  
            nivel_obstrucao = np.random.uniform(1, 10)
            if nivel_obstrucao <= 5:
                conjunto_obstrucao.append(random.uniform(5, 20))
            elif nivel_obstrucao <= 8:
                conjunto_obstrucao.append(random.uniform(20, 50))
            else:
                conjunto_obstrucao.append(random.uniform(50, 70))
        
        # Luminosidade
        conjunto_luminosidade = []
        for i in range(num_paineis):
            if hora in range(6, 18):
                conjunto_luminosidade.append(random.uniform(750, 1000))
            elif hora in range(18, 20) or hora in range(4, 6):
                conjunto_luminosidade.append(random.uniform(500, 750))
            else:
                conjunto_luminosidade.append(random.uniform(100, 200))
        
        # Temperatura e Umidade
        conjunto_temp_externa = []
        conjunto_temp_interna = []
        conjunto_umidade = []
        for i in range(num_paineis):
            if mes in [12, 1, 2]:  # Verão
                temp_externa = random.uniform(30, 35)
                umidade = random.uniform(70, 90)
            elif mes in [3, 4, 5]:  # Outono
                temp_externa = random.uniform(25, 30)
                umidade = random.uniform(50, 70)
            elif mes in [6, 7, 8]:  # Inverno
                temp_externa = random.uniform(20, 25)
                umidade = random.uniform(30, 50)
            else:  # Primavera
                temp_externa = random.uniform(25, 30)
                umidade = random.uniform(50, 80)
                
            conjunto_temp_externa.append(temp_externa)
            conjunto_umidade.append(umidade)
            
            if hora in range(10, 16):
                temp_interna = temp_externa + random.uniform(4, 8)
            else:
                temp_interna = temp_externa + random.uniform(8, 16)
                
            conjunto_temp_interna.append(temp_interna)
        
        # Potência
        conjunto_potencia = []
        for i in range(num_paineis):
            potencia = np.random.uniform(5, 50) if hora in range(6, 18) else np.random.uniform(5, 10)
            conjunto_potencia.append(potencia)
        
        # Tensão
        conjunto_tensao = [random.uniform(38, 42) for _ in range(num_paineis)]
        
        # UV
        conjunto_uv = []
        for i in range(num_paineis):
            max_uv = 5 if hora in range(6, 18) else 0.5
            media = max_uv * 0.8
            desviopadrao = max_uv * 0.2
            uv_indice = max(0, min(max_uv, random.gauss(media, desviopadrao)))
            conjunto_uv.append(uv_indice)   
        
        # Agrupar dados por painel
        data_group = []
        for i in range(num_paineis):
            dados = {
                "data_hora": data_formatada,
                "obstrucao": round(conjunto_obstrucao[i], 2),
                "luminosidade": round(conjunto_luminosidade[i], 2),
                "temperatura_externa": round(conjunto_temp_externa[i], 2),
                "temperatura_interna": round(conjunto_temp_interna[i], 2),
                "potencia": round(conjunto_potencia[i], 2),
                "tensao": round(conjunto_tensao[i], 2),
                "uv": round(conjunto_uv[i], 2),
                "umidade": round(conjunto_umidade[i], 2),
                "ceu": ceu,
                "direcionamento": direcionamento,
                "inclinacao": inclinacao
            }
            data_group.append(dados)
            
        return data_group  
        
    def send_to_bucket(self, data):
        try:
            self.client.connect()
            self.client.publish('solara/data/iot', json.dumps(data), 1)
            print("Dados enviados para o AWS IoT Core")
        except Exception as ex:
            print(f'Ocorreu um erro ao enviar os dados para o AWS IoT Core: {ex}')
        finally:
            try:
                self.client.disconnect()
            except Exception as disconnect_ex:
                print(f"Ocorreu um erro ao desconectar do AWS IoT Core: {disconnect_ex}")

            
    def main(self, info):
        print("Gerando dados Raw...")
        
        data = self.data_group(info)
        print("Dados Raw gerados com sucesso.")
        
        # self.send_to_bucket(data)
        print("Dados enviados para o AWS IoT Core")

if __name__ == "__main__":
    simulator = Simulator()
    
    info = {
        "empresa": "Solara",
        "num_dados": 20,
        "num_setores": 2,
        "num_paineis": 3,
        "direcionamento": "norte",
        "inclinacao": 22.5
    }

    simulator.main(info)