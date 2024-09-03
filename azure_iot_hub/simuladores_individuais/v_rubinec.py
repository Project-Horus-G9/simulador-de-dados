# sensor de luminosidade

import random
import matplotlib.pyplot as plt
import mysql.connector

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

if connection.is_connected():
    print("Connected to MySQL server")

def gerar_dados_luminosidade(num_dias):
    leituras = []
    for dia in range(num_dias):
        
        # gerar um número de 1 a 5 de forma aleatória
        nublado = random.randint(1, 5)
        
        if nublado == 1:
            for hora in range(24):
                if hora in range(6, 18):
                    leituras.append(random.uniform(100, 200))
                elif hora in range(18, 20) or hora in range(4, 6):
                    leituras.append(random.uniform(100, 150))
                else:
                    leituras.append(random.uniform(50, 100))  
        else:
            for hora in range(24):
                if hora in range(6, 18):
                    leituras.append(random.uniform(300, 400))
                elif hora in range(18, 20) or hora in range(4, 6):
                    leituras.append(random.uniform(250, 300))
                else:
                    leituras.append(random.uniform(100, 200))        
    return leituras

# leituras_semanais = gerar_dados_luminosidade(7)


def puxar_dados():
    cursor.execute("SELECT * FROM luminosidade")
    leituras = cursor.fetchall()
    return leituras

dias_da_semana = ['Segunda-feira', 'Terça-feira', 'Quarta-feira', 'Quinta-feira', 'Sexta-feira', 'Sábado', 'Domingo']

leituras = puxar_dados()

leituras_semanais = []
    
for leitura in leituras:
    leituras_semanais.append(leitura[2])

fig, ax = plt.subplots(figsize=(10, 6))
fig.suptitle('Luminosidade por Dia', fontsize=16)

for dia, leituras_dia in enumerate(zip(*[iter(leituras_semanais)]*24)):
    horas = list(range(24))
    ax.plot(horas, leituras_dia, marker='o', label=dias_da_semana[dia])
    
ax.set_title('Luminosidade por Dia')
plt.legend()
plt.grid(True)
plt.ylabel('Luminosidade (lux)')
plt.xlabel('Hora do Dia')
plt.show()

# # quero que o gráfico seja de barra
# fig, ax = plt.subplots(figsize=(10, 6))
# fig.suptitle('Luminosidade por Dia', fontsize=16)

# for dia, leituras_dia in enumerate(zip(*[iter(leituras_semanais)]*24)):
#     horas = list(range(24))
#     ax.bar(dias_da_semana[dia], sum(leituras_dia)/24)
    
# ax.set_title('Luminosidade por Dia')
# plt.grid(True)
# plt.ylabel('Luminosidade (lux)')
# plt.xlabel('Dia da Semana')
# plt.show()