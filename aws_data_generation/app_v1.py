from datetime import datetime
import numpy as np
import random
import time
import boto3
from datetime import timedelta
import csv
import os
import json

class Simulador:
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        
    def gerar_dados(self, info):
        
        num_paineis = info["num_paineis_totais"]
        ceu = info["ceu"]
        direcionamento = info["info_paineis"]["direcionamento"]
        inclinacao = info["info_paineis"]["inclinacao"]
        modo = info["modo"]
        indice = info["indice"]
        
        # Data e hora atual
        if modo == "simulacao":
            data_atual = datetime.now()
            data_formatada = data_atual.strftime('%Y-%m-%d %H:%M:%S')
        elif modo == "producao":
            data_formatada = datetime.strptime('2024-09-01 09:00:00', '%Y-%m-%d %H:%M:%S')
            data_formatada += timedelta(minutes=(indice * 30))
            data_formatada = data_formatada.strftime('%Y-%m-%d %H:%M:%S')

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
                conjunto_luminosidade.append(random.uniform(750, 1000) + random.uniform(-10, 10))
            elif hora in range(18, 20) or hora in range(4, 6):
                conjunto_luminosidade.append(random.uniform(300, 500) + random.uniform(-10, 10))
            elif hora in range(5, 6) or hora in range(4, 6):
                conjunto_luminosidade.append(random.uniform(500, 100) + random.uniform(-10, 10))
            else:
                conjunto_luminosidade.append(random.uniform(0, 50) + random.uniform(-10, 10))
            
        # Temperatura
        conjunto_temp_externa = []
        conjunto_temp_interna = []
        for i in range(num_paineis):
            if mes in [12, 1, 2]:
                temp_externa = random.uniform(30, 35)
            elif mes in [3, 4, 5]:
                temp_externa = random.uniform(25, 30)
            elif mes in [6, 7, 8]:
                temp_externa = random.uniform(20, 25)
            else:
                temp_externa = random.uniform(25, 30)
                
            conjunto_temp_externa.append(temp_externa)
            
            if hora in range(10, 16):
                temp_interna = temp_externa + random.uniform(-2, 2)
            else:
                temp_interna = temp_externa + random.uniform(-5, 5)
                
            conjunto_temp_interna.append(temp_interna)
            
            
        # Potência
        conjunto_potencia = []

        percentual_captacao = 0.4
        potencia_maxima_NOCT = 400
        for i in range(num_paineis):
            potencia = np.random.uniform(0, potencia_maxima_NOCT * percentual_captacao)
            if hora < 6 or hora > 18:
                potencia *= 0.1
            conjunto_potencia.append(potencia)
            
        # Tensão
        conjunto_tensao = []
        for i in range(num_paineis):
            tensao = 40 + random.uniform(-2, 2)
            conjunto_tensao.append(tensao)
        
        # UV
        conjunto_uv = []
        
        for i in range(num_paineis):
            if hora in range(8, 16):
                max_uv = 5
                media = max_uv * 0.8
                desviopadrao = max_uv * 0.2
            else:
                max_uv = 0.5
                media = max_uv * 0.8
                desviopadrao = max_uv * 0.2
            
            uv_indice = max(0, min(max_uv, random.gauss(media, desviopadrao)))
            conjunto_uv.append(uv_indice)   
        
        
        conjunto_dados = []
        for i in range(num_paineis):
            dados = {
                "data_hora": data_formatada,
                "obstrucao": round(conjunto_obstrucao[i],2),
                "luminosidade": round(conjunto_luminosidade[i],2),
                "temperatura_externa": round(conjunto_temp_externa[i],2),
                "temperatura_interna": round(conjunto_temp_interna[i],2),
                "potencia": round(conjunto_potencia[i],2),
                "tensao": round(conjunto_tensao[i],2),
                "uv": round(conjunto_uv[i],2),
                "ceu": ceu,
                "direcionamento": direcionamento,
                "inclinacao": inclinacao
            }
            
            conjunto_dados.append(dados)
            
        return conjunto_dados        
    
    def conjunto_dados(self, info):

        empresa = info["empresa"]
        num_dados = info["num_dados"]
        num_setores = info["num_setores"]
        num_paineis = info["num_paineis"]
        info_paineis = {
            "direcionamento": info["direcionamento"],
            "inclinacao": info["inclinacao"]
        }
        modo = info["modo"]

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
            
            if i % 2 == 0:
                ceu = random.choice(possiveis_ceus)
            
            if modo == "simulacao":
                time.sleep(60)
            num_paineis_totais = num_paineis * num_setores
            
            info_geracao = {
                "indice": i,
                "num_paineis_totais": num_paineis_totais,
                "ceu": ceu,
                "info_paineis": info_paineis,
                "modo": modo
            }
            
            dados_gerados = self.gerar_dados(info_geracao)
            for setor in dados["setores"]:
                for painel in setor["paineis"]:
                    painel["dados"].append(dados_gerados.pop(0))
                
        return dados
    
    def dados_trusted(self, dados_raw):
        dados = {
            "cliente": dados_raw["cliente"],
            "setores": []
        }
        
        for setor in dados_raw["setores"]:
            setor_trusted = {
                "setor": setor["setor"],
                "paineis": []
            }
            
            for painel in setor["paineis"]:
                painel_trusted = {
                    "painel": painel["painel"],
                    "dados": []
                }
                
                for dado in painel["dados"]:                   
                    energia_gerada = round((dado['uv'] * 10 * 0.8) / (dado["obstrucao"] * 0.2), 2)
                    max_enegia_dia = 40
                    max_enegia_noite = 4
                    
                    # Conversão de string para datetime
                    data_hora = datetime.strptime(dado["data_hora"], '%Y-%m-%d %H:%M:%S')
                    
                    # Cálculo da eficiência baseado na hora
                    if data_hora.time() >= datetime.strptime('06:00:00', '%H:%M:%S').time() and data_hora.time() <= datetime.strptime('18:00:00', '%H:%M:%S').time():
                        eficiencia = round((energia_gerada * 100) / max_enegia_dia, 2)
                        energia_esperada = round(dado['potencia'] * 0.4, 2)
                    else:
                        eficiencia = round((energia_gerada * 100) / max_enegia_noite, 2)
                        energia_esperada = round(dado['potencia'] * 0.1, 2)
                    
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
                    
                    painel_trusted["dados"].append(dado_trusted)
                    
                setor_trusted["paineis"].append(painel_trusted)
            
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
                "paineis": []
            }
            
            for painel in setor["paineis"]:
                painel_client = {
                    "painel": painel["painel"],
                    "dados": []
                }
                
                for dado in painel["dados"]:
                    razao = dado["temperatura_interna"] / dado["temperatura_externa"]
                    
                    data_hora = datetime.strptime(dado["data_hora"], '%Y-%m-%d %H:%M:%S')
    
                    horario = (data_hora.time() >= datetime.strptime('05:00:00', '%H:%M:%S').time() and
                            data_hora.time() <= datetime.strptime('19:00:00', '%H:%M:%S').time())
                    razao_temp = razao > 2 or razao < 0.5
                    energia_gerada = dado["energia_gerada"] > 20
                    energia_esperada = dado["energia_esperada"] > 20
                    tensao = dado["tensao"] > 5
                    
                    if horario or razao_temp or energia_gerada or energia_esperada or tensao:
                        painel_client["dados"].append(dado)
                    
                setor_client["paineis"].append(painel_client)
                
            dados["setores"].append(setor_client)
            
        return dados
        
        
    def enviar_dados_bucket(self, bucket_name, dados):
        try:
            arquivo_nome = f"{dados['cliente']}_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.json"
            
            self.s3_client.upload_file(arquivo_nome, bucket_name, arquivo_nome)
            print(f"Arquivo '{arquivo_nome}' enviado com sucesso para o bucket S3 '{bucket_name}'.")
        except Exception as e:
            print(f"Erro ao enviar o arquivo para o bucket S3: {e}")
       
    def dict_to_csv(self, data):      
        pasta = 'aws_data_generation/csv_producao'
        if not os.path.exists(pasta):
            os.makedirs(pasta)

        header_usuario = ["nome", "email", "senha"]    
        headers_fazenda = ["nome", "id_usuario"]
        headers_setor = ["id_fazenda", "numero_setor"]
        headers_painel = ["id_setor", "numero_painel"]
        headers_dado = ["data_hora", "obstrucao", "luminosidade", "temperatura_externa", "temperatura_interna", "tensao", "energia_gerada", "energia_esperada", "eficiencia", "ceu", "direcionamento", "inclinacao", "id_painel"]

        content_usuario = ["Guilherme Silva", "guilherme@gmail.com", "1234"]

        with open(os.path.join(pasta, "usuario.csv"), mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=header_usuario, delimiter=';')
            writer.writeheader()
            row = {
                "nome": content_usuario[0],
                "email": content_usuario[1],
                "senha": content_usuario[2]
            }
            writer.writerow(row)

        with open(os.path.join(pasta, "fazenda.csv"), mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=headers_fazenda, delimiter=';')
            writer.writeheader()
            row = {
                "nome": data['cliente'],
                "id_usuario": 1
            }
            writer.writerow(row)

        with open(os.path.join(pasta, "setor.csv"), mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=headers_setor, delimiter=';')
            writer.writeheader()
            for setor in data["setores"]:
                row = {
                    "id_fazenda": 1,
                    "numero_setor": setor["setor"]
                }
                writer.writerow(row)

        with open(os.path.join(pasta, "painel.csv"), mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=headers_painel, delimiter=';')
            writer.writeheader()

            id = 0
            for setor in data["setores"]:
                id += 1
                for painel in setor["paineis"]:
                    row = {
                        "id_setor": id,
                        "numero_painel": painel["painel"]
                    }
                    writer.writerow(row)

        with open(os.path.join(pasta, "dado.csv"), mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=headers_dado, delimiter=';')
            writer.writeheader()

            id = 0
            contador = 0
            total_dados = len(data["setores"]) * len(data["setores"][0]["paineis"]) * len(data["setores"][0]["paineis"][0]["dados"])
            for setor in data["setores"]:
                for painel in setor["paineis"]:
                    id += 1
                    for dado in painel["dados"]:
                        contador += 1
                        print(f"Salvando dado {contador} de {total_dados}")
                        row = {
                            "data_hora": dado["data_hora"],
                            "obstrucao": dado["obstrucao"],
                            "luminosidade": dado["luminosidade"],
                            "temperatura_externa": dado["temperatura_externa"],
                            "temperatura_interna": dado["temperatura_interna"],
                            "tensao": dado["tensao"],
                            "energia_gerada": dado["energia_gerada"],
                            "energia_esperada": dado["energia_esperada"],
                            "eficiencia": dado["eficiencia"],
                            "ceu": dado["ceu"],
                            "direcionamento": dado["direcionamento"],
                            "inclinacao": dado["inclinacao"],
                            "id_painel": id
                        }
                        writer.writerow(row)
                        
    def main(self, info):
        buckets = ["set-raw", "tote-trusted", "horus-client"]
        
        # raw
        dados = self.conjunto_dados(info)
        print("Dados Raw gerados com sucesso.")
        
        if info["modo"] == "simulacao":
            self.enviar_dados_bucket(buckets[0], dados)
        
        # trusted
        dados_trusted = self.dados_trusted(dados)
        print("Dados Trusted gerados com sucesso.")
        
        # Abrir (ou criar) um arquivo e salvar o JSON
        with open("dados.json", "w") as arquivo_json:
            json.dump(dados_trusted, arquivo_json, indent=4)
    
if __name__ == "__main__":
    simulador = Simulador()
    
    info = {
        "empresa": "Empresa X",
        "num_dados": 20,
        "num_setores": 3,
        "num_paineis": 10,
        "direcionamento": "norte",
        "inclinacao": 22.5,
        "modo": "producao"
        # possui dois modos: simulacao e producao
        # simulacao: gera dados a cada 1 minuto
        # producao: gera uma massa de dados com 30 minutos de intervalo
    }

    if info["modo"] == "simulacao":
        while True:
            simulador.main(info)
    else:
        simulador.main(info)