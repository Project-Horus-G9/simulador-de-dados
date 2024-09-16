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
            "obstrucao": "",
            "luminosidade": "",
            "temperatura_externa": "",
            "temperatura_interna": "",
            "potencia": "",
            "tensao": "",
            "uv": "",
            "ceu": "",
            "direcionamento": "",
            "inclinacao": ""
        }
        
        # Data e hora atual
        data_atual = datetime.now()
        dados["data_hora"] = data_atual.strftime("%Y-%m-%d %H:%M")
        
        # Obstrução
        nivel_obstrucao = np.random.uniform(1, 10)
        
        if nivel_obstrucao <= 5:
            dados["obstrucao"] = random.uniform(5, 20)
        elif nivel_obstrucao <= 8:
            dados["obstrucao"] = random.uniform(20, 50)
        else:
            dados["obstrucao"] = random.uniform(50, 70)
        
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
            
        # Tensão
        dados["tensao"] = 40 + random.uniform(-2, 2)
        
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

        dados = {
            "cliente": dados_raw["cliente"],
            "setores": []
        }
        
        for setor in dados_raw["setores"]:
            setor_trusted = {
                "setor": setor["setor"],
                "dados": []
            }
            
            for dado in setor["dados"]:
                energia_gerada = round((dado['uv'] * 10 * 0.8) / (dado["obstrucao"] * 0.2), 2)
                max_enegia_dia = 44
                max_enegia_noite = 4.8
                
                energia_esperada = round(dado['potencia'] * 0.8, 2)
                
                if dado["data_hora"].time() >= datetime.strptime('06:00:00', '%H:%M:%S').time() and dado["data_hora"].time() <= datetime.strptime('18:00:00', '%H:%M:%S').time():
                    eficiencia = round(energia_gerada / max_enegia_dia, 0)
                else:
                    eficiencia = round(energia_gerada / max_enegia_noite, 0)
                
                dado_trusted = {
                    "data_hora": dado["data_hora"],
                    "obstrucao": dado["obstrucao"],
                    "luminosidade": dado["luminosidade"],
                    "temperatura_externa": dado["temperatura_externa"],
                    "temperatura_interna": dado["temperatura_interna"],
                    "tensao": dado["tensao"],
                    "energia_gerada": energia_gerada,
                    "energia_esperada": energia_esperada,
                    "eficiencia": eficiencia,
                    "ceu": dado["ceu"],
                    "direcionamento": dado["direcionamento"],
                    "inclinacao": dado["inclinacao"]
                }
                setor_trusted["dados"].append(dado_trusted)
                
            dados["setores"].append(setor_trusted)
                
        return dados
    
    def dados_client(self, dados_trusted):
        dados = {
            "cliente": dados_trusted["cliente"],
            "setores": []
        }
        
        for setor in dados_trusted["setores"]:
            setor_client = {
                "setor": setor["setor"],
                "dados": []
            }
            
            for dado in setor["dados"]:
                                
                # horario: 5h as 19h
                # razao temp > 2 ou < 0.5
                # energia gerada > 20
                # energia esperada > 20
                # tensao > 5
                
                razao = dado["temperatura_interna"] / dado["temperatura_externa"]
                
                horario = dado["data_hora"].time() >= datetime.strptime('05:00:00', '%H:%M:%S').time() and dado["data_hora"].time() <= datetime.strptime('19:00:00', '%H:%M:%S').time()
                razao_temp = razao > 2 or razao < 0.5
                energia_gerada = dado["energia_gerada"] > 20
                energia_esperada = dado["energia_esperada"] > 20
                tensao = dado["tensao"] > 5
                
                if horario or razao_temp or energia_gerada or energia_esperada or tensao:
                    setor_client["dados"].append(dado)                    
                
            dados["setores"].append(setor_client)
        
        
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