import boto3
import json
from datetime import datetime

class Treatment:
    
    def __init__(self):
        self.s3 = boto3.client('s3')
        self.buckets = {
            "raw": "set-raw",
            "trusted": "tote-trusted"
        }
        self.final_object = ''
        self.buckets_objects = {
            "raw": [],
            "trusted": []
        }
        
    def get_objects(self):
        for bucket in self.buckets:
            response = self.s3.list_objects_v2(Bucket=self.buckets[bucket])
            objects = response.get('Contents', [])
            self.buckets_objects[bucket] = sorted(objects, key=lambda obj: obj['LastModified'], reverse=True)
            
    def process_data(self):
        objects_raw_keys = [obj['Key'] for obj in self.buckets_objects['raw']]
        objects_trusted_keys = [obj['Key'] for obj in self.buckets_objects['trusted']]
        
        objects_to_process = set(objects_raw_keys) - set(objects_trusted_keys)
        
        objects_to_process_sorted = [obj for obj in self.buckets_objects['raw'] if obj['Key'] in objects_to_process]
        
        for obj in objects_to_process_sorted:
            obj_content = self.s3.get_object(Bucket=self.buckets['raw'], Key=obj['Key'])
            data_raw = json.loads(obj_content['Body'].read().decode('utf-8'))
            
            data_trusted = self.data_trusted(data_raw)
            
            self.s3.put_object(
                Bucket=self.buckets['trusted'],
                Key=obj['Key'],
                Body=json.dumps(data_trusted, indent=4)
            )
            
            self.final_object = obj['Key']
            print(f"Arquivo {obj['Key']} processado com sucesso.")
            
        if self.final_object != '':
            self.save_last_file()
            
    def save_last_file(self):
        if self.final_object:
            with open('last_archive_trusted.txt', 'w') as f:
                f.write(self.final_object)
            print(f"Nome do último arquivo salvo em last_archive_trusted.txt: {self.final_object}")
        
    def data_trusted(self, data_raw):
        data = {
            "cliente": data_raw["empresa"],
            "setores": []
        }
        
        for setor in data_raw['setores']:
            setor_trusted = {
                "setor": setor["setor"],
                "paineis": []
            }
            
            for painel in setor['paineis']:
                painel_trusted = {
                    "painel": painel["painel"],
                    "dados": []
                }
                
                for dado in painel['dados']:
                    energia_gerada = round((dado['uv'] * 10 * 0.8) / (dado["obstrucao"] * 0.2), 2)
                    max_energia_dia = 44
                    max_energia_noite = 4.8

                    energia_esperada = round(dado['potencia'] * 0.8, 2)

                    if datetime.strptime(dado["data_hora"], '%Y-%m-%d %H:%M:%S').time() >= datetime.strptime('06:00:00', '%H:%M:%S').time() and datetime.strptime(dado["data_hora"], '%Y-%m-%d %H:%M:%S').time() <= datetime.strptime('18:00:00', '%H:%M:%S').time():
                        eficiencia = round(energia_gerada / max_energia_dia, 2)
                    else:
                        eficiencia = round(energia_gerada / max_energia_noite, 2)

                    dado_trusted = {
                        "data_hora": dado["data_hora"],
                        "obstrucao": dado["obstrucao"],
                        "luminosidade": dado["luminosidade"],
                        "temperatura_externa": dado["temperatura_externa"],
                        "temperatura_interna": dado["temperatura_interna"],
                        "tensao": dado["tensao"],
                        "umidade": dado["umidade"],
                        "energia_gerada": energia_gerada,
                        "energia_esperada": energia_esperada,
                        "eficiencia": eficiencia,
                        "ceu": dado["ceu"],
                        "direcionamento": dado["direcionamento"],
                        "inclinacao": dado["inclinacao"]
                    }
                    painel_trusted["dados"].append(dado_trusted)  # Adiciona o dado ao painel

                setor_trusted["paineis"].append(painel_trusted)  # Adiciona o painel ao setor

            data["setores"].append(setor_trusted)  # Adiciona o setor ao resultado final

        return data  

    def run(self):
        print("Iniciando tratamento dos dados...")
        self.get_objects()
        self.process_data()
        print("Tratamento dos dados finalizado.")
    
if __name__ == "__main__":
    treatment = Treatment()
    treatment.run()