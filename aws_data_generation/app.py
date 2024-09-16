from datetime import datetime
import numpy as np
import random
import time
import boto3

class Simulador:
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        
    def gerar_dados(self, ceu, direcionamento, inclinacao):
        dados = {
            "data_hora": "",
            "luminosidade": "",
            "temperatura_externa": "",
            "temperatura_interna": "",
            "potencia": "",
            "voltagem": "",
            "uv": "",
            "ceu": "",
            "direcionamento": "",
            "inclinacao": ""
        }
        
        # Data e hora atual
        data_atual = datetime.now()
        dados["data_hora"] = data_atual.strftime("%Y-%m-%d %H:%M")
        
        # Luminosidade
        if data_atual.hour in range(6, 18):
            dados["luminosidade"] = random.uniform(750, 1000) + random.uniform(-10, 10)
        elif data_atual.hour in range(18, 20) or data_atual.hour in range(4, 6):
            dados["luminosidade"] = random.uniform(300, 500) + random.uniform(-10, 10)
        elif data_atual.hour in range(5, 6) or data_atual.hour in range(4, 6):
            dados["luminosidade"] = random.uniform(500, 100) + random.uniform(-10, 10)
        else:
            dados["luminosidade"] = random.uniform(0, 50) + random.uniform(-10, 10)
            
        # Temperatura
        if data_atual.month in [12, 1, 2]:
            dados["temperatura_externa"] = random.uniform(30, 35)
        elif data_atual.month in [3, 4, 5]:
            dados["temperatura_externa"] = random.uniform(25, 30)
        elif data_atual.month in [6, 7, 8]:
            dados["temperatura_externa"] = random.uniform(20, 25)
        else:
            dados["temperatura_externa"] = random.uniform(25, 30)
            
        if data_atual.time() >= datetime.strptime('10:00:00', '%H:%M:%S').time() and data_atual.time() <= datetime.strptime('16:00:00', '%H:%M:%S').time():
            dados["temperatura_interna"] = dados["temperatura_externa"] + random.uniform(-2, 2)
        else:
            dados["temperatura_interna"] = dados["temperatura_externa"] + random.uniform(-5, 5)
            
        # Potência
        potencia_maxima_NOCT = 400
        percentual_captacao = 0.4
        
        dados["potencia"] = np.random.uniform(0, potencia_maxima_NOCT * percentual_captacao)
        
        if data_atual.hour < 6 or data_atual.hour > 18:
            dados["potencia"] *= 0.1
            
        # Voltagem
        dados["voltagem"] = 40 + random.uniform(-2, 2)
        
        # UV
        if data_atual.hour in range(8, 16):
            ruido = 0.5
            uv_indice = 5
        else:
            ruido = 0.1
            uv_indice = 0.5
        
        uv_indice += random.uniform(-ruido, ruido)
        uv_indice = max(0, uv_indice)
        
        dados["uv"] = uv_indice
        
        # Céu
        dados["ceu"] = ceu
        
        # Direcionamento
        dados["direcionamento"] = direcionamento
        
        # Inclinação
        dados["inclinacao"] = inclinacao
            
        return dados        
    
    def conjunto_dados(self, num_dados, num_setores, empresa, info_paineis):
        dados = {
            "cliente": empresa,
            "setores": []
        }
        
        for i in range(num_setores):
            setor = {
                "setor": i + 1,
                "dados": []
            }
            dados["setores"].append(setor)
            
        possiveis_ceus = ["ceu limpo", "algumas nuvens", "chuva leve", "nublado", "nuvens dispersas"]
            
        for i in range(num_dados):
            
            if i % 20 == 0:
                ceu = random.choice(possiveis_ceus)
            
            time.sleep(60)
            for setor in dados["setores"]:
                setor["dados"].append(self.gerar_dados(ceu, info_paineis["direcionamento"], info_paineis["inclinacao"]))
                
        return dados
    
    def dados_trusted(self, dados_raw):
        dados_trusted = []
        
        # for setor in dados_raw["setores"]:
        #     for dado in setor["dados"]:
        #         dado_trusted = {
        #             "data_hora": dado["data_hora"],
        #             "luminosidade": dado["luminosidade"],
        #             "temperatura_externa": dado["temperatura_externa"],
        #             "temperatura_interna": dado["temperatura_interna"],
        #             "potencia": dado["potencia"],
        #             "voltagem": dado["voltagem"]
        #         }
        #         dados_trusted.append(dado_trusted)
                
        return dados_trusted
    
    def dados_client(self, dados_trusted):
        dados_client = []
        
        # for dado in dados_trusted:
        #     dado_cliente = {
        #         "data_hora": dado["data_hora"],
        #         "luminosidade": dado["luminosidade"],
        #         "temperatura_interna": dado["temperatura_interna"],
        #         "potencia": dado["potencia"]
        #     }
        #     dados_cliente.append(dado_cliente)
        
        return dados_client
    
    def enviar_dados_bucket(self, bucket_name, dados):
        try:
            arquivo_nome = f"{dados['cliente']}_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.json"
            
            self.s3_client.upload_file(arquivo_nome, bucket_name, arquivo_nome)
            print(f"Arquivo '{arquivo_nome}' enviado com sucesso para o bucket S3 '{bucket_name}'.")
        except Exception as e:
            print(f"Erro ao enviar o arquivo para o bucket S3: {e}")
    
    def main(self, empresa):
        num_dados = 5
        num_setores = 3
        buckets = ["set-raw", "tote-trusted", "horus-client"]
        
        info_paineis = {
            "direcionamento": "norte",
            "inclinacao": 22.5
        }
        
        # raw
        dados = self.conjunto_dados(num_dados, num_setores, empresa, info_paineis)
        
        self.enviar_dados_bucket(buckets[0], dados)
        
        # trusted
        dados_trusted = self.dados_trusted(dados)
        
        self.enviar_dados_bucket(buckets[1], dados_trusted)
        
        # client
        dados_client = self.dados_client(dados_trusted)
        
        self.enviar_dados_bucket(buckets[2], dados_client)
    
if __name__ == "__main__":
    simulador = Simulador()
    
    empresa = "Horus"
    
    while True:
        simulador.main(empresa)