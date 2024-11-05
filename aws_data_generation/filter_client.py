import boto3
import json
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import os

class Filter:
    
    def __init__(self):
        
        
        
        self.s3 = boto3.client('s3')
        self.buckets = {
            "trusted": "tote-trusted",
            "client": "horus-client"
        }
        self.final_object = ''
        self.buckets_objects = {
            "trusted": [],
            "client": []
        }
        self.file_path = '/home/ubuntu/horus/last_archive_client.txt'
        
        scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
        path_credential = '/credentials/sheets/'
        credential = self.get_credential(path_credential)
        creds = Credentials.from_service_account_file(credential, scopes=scope)
        self.client = gspread.authorize(creds)
        self.sheet_key = os.getenv('SHEET_KEY')
        
    def get_credential(self, path_credential):
        with open(path_credential, 'r') as f:
            return json.load(f)
        
    def get_objects(self):
        for bucket in self.buckets:
            response = self.s3.list_objects_v2(Bucket=self.buckets[bucket])
            objects = response.get('Contents', [])
            self.buckets_objects[bucket] = sorted(objects, key=lambda obj: obj['LastModified'], reverse=True)
            
    def process_data(self):
        for obj in self.buckets_objects['trusted']:
            key_object = obj['Key']
            if key_object not in [f['Key'] for f in self.buckets_objects['client']]:
                
                obj = self.s3.get_object(Bucket=self.buckets['client'], Key=key_object)
                dados_trusted = json.loads(obj['Body'].read().decode('utf-8'))
                dados_clients = self.data_client(dados_trusted)
                
                self.s3.put_object(
                    Bucket=self.buckets['client'],
                    Key=key_object,
                    Body=json.dumps(dados_clients, indent=4)
                )
                
                self.final_object = key_object
                
                print(f"Arquivo {key_object} processado e transferido para o bucket client.")
                
                self.send_data_sheets(dados_clients)
                
                print("Dados enviados com sucesso para o Google Sheets!")
    
    def save_last_file(self):
        if self.buckets_objects['trusted']:
            last_object = self.buckets_objects['trusted'][0]['Key']
            with open(self.file_path, 'w') as f:
                f.write(last_object)
            print(f"Nome do último arquivo salvo em {self.file_path}: {last_object}")
        else:
            print("Nenhum arquivo encontrado no bucket.")
                
    def data_client(self, dados_trusted):
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
                    dado["data_hora"] = datetime.strptime(dado["data_hora"], '%Y-%m-%d %H:%M:%S')

                    razao = dado["temperatura_interna"] / dado["temperatura_externa"]

                    horario = dado["data_hora"].time() >= datetime.strptime('05:00:00', '%H:%M:%S').time() and dado["data_hora"].time() <= datetime.strptime('19:00:00', '%H:%M:%S').time()
                    razao_temp = razao > 2 or razao < 0.5
                    energia_gerada = dado["energia_gerada"] > 20
                    energia_esperada = dado["energia_esperada"] > 20
                    tensao = dado["tensao"] > 5

                    if horario or razao_temp or energia_gerada or energia_esperada or tensao:
                        painel_client["dados"].append(dado)

                setor_client["paineis"].append(painel_client)

            dados["setores"].append(setor_client)

        return dados
    
    def send_data_sheets(self, data_client):
        sheet = self.client.open_by_key(self.sheet_key)
        worksheet = sheet.get_worksheet(0)
        
        headers = ["Cliente", "Setor", "Painel", "Data/Hora", "Obstrução", "Luminosidade", 
                    "Temperatura Externa", "Temperatura Interna", "Tensão", 
                    "Energia Gerada", "Energia Esperada", "Eficiência", "Céu", "Umidade",
                    "Direcionamento", "Inclinação"]
        
        worksheet.update('A1', [headers])
        
        try:
            with open(data_client, 'r', encoding='utf-8') as file:
                dados = json.load(file)
        except Exception as e:
            print(f"Erro ao carregar o arquivo JSON: {e}")
            return
        
        cliente = dados['cliente']
        
        for setor_data in dados['setores']:
            setor = setor_data['setor']

            for painel_data in setor_data['paineis']:
                painel = painel_data['painel']

                for dado in painel_data['dados']:
                    linha = [
                        cliente,                    
                        setor,                      
                        painel,                     
                        dado['data_hora'],          
                        dado['obstrucao'],          
                        dado['luminosidade'],       
                        dado['temperatura_externa'], 
                        dado['temperatura_interna'],
                        dado['tensao'],             
                        dado['energia_gerada'],     
                        dado['energia_esperada'],   
                        dado['eficiencia'],         
                        dado['ceu'],                
                        dado['umidade'],                
                        dado['direcionamento'],     
                        dado['inclinacao']          
                    ]

                    worksheet.append_row(linha)

                print("Dados enviados com sucesso!")
    
    def run(self):
        self.get_objects()
        self.process_data()
        self.save_last_file()
        
if __name__ == '__main__':
    filter = Filter()
    filter.run()