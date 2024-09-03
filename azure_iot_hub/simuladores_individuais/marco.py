# Simula a captura de voltagem transmitida do painel solar para a usina

import mysql.connector
import random
import time
import matplotlib.pyplot as plt

host = 'localhost'
user = 'root'
password = 'root'
database = 'horus'
port = 3306

connection = mysql.connector.connect(
  host=host,
  user=user,
  password=password,
  database=database,
  port=port)

cursor = connection.cursor()

leituras = []
for i in range(0,1):
  for i in range(0,24):
    random_number = random.uniform(39, 40)
    leituras.append(random_number)
    
data_inicial = time.time()
datas = []
voltagens = []

if connection.is_connected():
  print("Connected to MySQL server")

  for voltagem in leituras:
      
    dataMedicao = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(data_inicial))
    data_inicial += 3600

    voltagens.append(voltagem)
    datas.append(time.strftime('%Hh', time.localtime(data_inicial)))
    
    painel = 'A'
    insert = f"INSERT INTO voltagem (dataMedicao,painel,voltagem)VALUES('{dataMedicao}','{painel}',{voltagem})"
    cursor.execute(insert)
    connection.commit()

cursor.close()
connection.close()

# print(leituras)

plt.plot(datas, voltagens)
plt.xlabel('Hora')
plt.ylabel('Voltagem (Volts)')
plt.show()


