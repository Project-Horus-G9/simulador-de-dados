# sensor de clima

import requests
import mysql.connector
import matplotlib.pyplot as plt  
import json

API_Key = "cf66d379214da8cbc2e6dbe4064aa622"
city = "amazonas"
link = f"https://api.openweathermap.org/data/2.5/forecast?q={city}&cnt=30&appid={API_Key}&lang=pt_br"

con = requests.get(link, verify=False)
req = con.json()

# colocar dados em um json 
with open('clima.json', 'w') as arquivo:
   json.dump(req, arquivo, indent=4)
  
print("Dados de clima salvos com sucesso")

# abrir o json clima.json
with open('clima.json', 'r') as arquivo:
   req = json.load(arquivo)
   
tipos_clima = []
tipos_descricao = []
   
for item in req["list"]:
   if item["weather"][0]["main"] not in tipos_clima:
      tipos_clima.append(item["weather"][0]["main"])
      tipos_descricao.append(item["weather"][0]["description"])
      
print("Tipos de clima:", tipos_clima)
print("Descrição dos tipos de clima:", tipos_descricao)

# parametros = {
#    "clima": [],
#    "nuvens": [],
#    "data": [],
# }


# for item in req["list"]:
#    parametros["clima"].append(item["main"]["temp"])
#    parametros["nuvens"].append(item["weather"])
#    parametros["data"].append(item["dt_txt"])

# clima = parametros["clima"]
# dia = parametros["data"]

# plt.plot(dia, clima, marker="o")
# plt.title("Clima em Santos")
# plt.xlabel("Dias")

# plt.show()

#   print("Gerando dados de clima")
  
#   API_Key = "cf66d379214da8cbc2e6dbe4064aa622"
#   city = "são paulo"
#   link = f"https://api.openweathermap.org/data/2.5/forecast?q={city}&cnt=30&appid={API_Key}&lang=pt_br"

#   con = requests.get(link, verify=False)
#   req = con.json()

#   for item in req["list"]:
#     dataMedicao = item["dt_txt"]
#     clima = item["weather"][0]["description"]
#     tempo = item["weather"][0]["main"]
    
#     insert = f"INSERT INTO clima (dataMedicao, clima, tempo)VALUES('{dataMedicao}','{clima}','{tempo}')"
#     cursor.execute(insert)
#     connection.commit()
     
#   print("Dados de clima gerados com sucesso")