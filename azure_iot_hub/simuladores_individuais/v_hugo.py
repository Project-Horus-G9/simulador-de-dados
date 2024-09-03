import mysql.connector
import random
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

# Configurações do MySQL
ip = '192.168.56.1'
host = ip
user = 'aluno'
password = 'aluno'
database = 'metricas'
port = 3306
current_datetime = None
tabela = 'leitura_temperatura'
sensor = 'Sensor1'



def obter_estacao(mes):
    if mes in [12, 1, 2]:
        return "Verão"
    elif mes in [3, 4, 5]:
        return "Outono"
    elif mes in [6, 7, 8]:
        return "Inverno"
    else:
        return "Primavera"

def calcular(cursor, conexao, temperatura):
    try:
        if conexao.is_connected():
            global current_datetime
            global tabela
            global sensor

            if current_datetime is None:
                current_datetime = datetime.now()
            
            if current_datetime.time() >= datetime.strptime('10:00:00', '%H:%M:%S').time() and current_datetime.time() <= datetime.strptime('16:00:00', '%H:%M:%S').time():
                random_temperature = round(random.uniform(temperatura, temperatura + 23), 2)

            else:
                random_temperature = round(random.uniform(temperatura, temperatura + 20), 2)

            current_datetime += timedelta(hours=0.30)
            tabela = 'leitura_temperatura'
            insert = f"INSERT INTO {tabela} (dataMedicao, sensor, temperaturaExterna, temperaturaPlaca) VALUES ('{current_datetime}', '{sensor}', {round(temperatura, 2)},{round(random_temperature, 2)})"
            cursor.execute(insert)
            conexao.commit()
            
            
            return random_temperature

    except mysql.connector.Error as error:
        print("Erro ao inserir dados no banco de dados:", error)


def gerar_temperatura(cursor, conexao, estacao):
    if estacao == "Verão":
        temperatura = random.uniform(30, 35)
    elif estacao == "Outono":
        temperatura = random.uniform(25, 30)
    elif estacao == "Inverno":
        temperatura = random.uniform(20, 25)
    else:
        temperatura = random.uniform(25, 30)

    temperatura_placa = calcular(cursor, conexao, temperatura)  # Chama a função calcular passando a temperatura gerada

    return temperatura, temperatura_placa

dias_semana = {
    "Monday": "Segunda-feira",
    "Tuesday": "Terça-feira",
    "Wednesday": "Quarta-feira",
    "Thursday": "Quinta-feira",
    "Friday": "Sexta-feira",
    "Saturday": "Sábado",
    "Sunday": "Domingo"
}

try:
    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port
    )
    cursor = connection.cursor()

    # Criar e popular dicionário de temperaturas por dia da semana
    temperaturas_por_dia = {dia: {'data_hora': [], 'temperaturas': [], 'temperaturaPlaca' : []} for dia in dias_semana.values()}
    data_atual = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    horas_simulacao = 24

    # Gerar e inserir dados de temperatura
    for hora in range(horas_simulacao):
        for meia_hora in range(0, 60, 30):
            for dia_semana in dias_semana.values():
                data_hora = data_atual + timedelta(hours=hora, minutes=meia_hora)
                estacao_atual = obter_estacao(data_hora.month)
                temperatura, temperatura_placa = gerar_temperatura(cursor, connection, estacao_atual)
                temperaturas_por_dia[dia_semana]['data_hora'].append(data_hora)
                temperaturas_por_dia[dia_semana]['temperaturas'].append(temperatura)
                temperaturas_por_dia[dia_semana]['temperaturaPlaca'].append(temperatura_placa)


    # Commit e fechar conexão
    connection.commit()
    cursor.close()
    connection.close()

    
    # Plotar os dados de temperatura ao longo de 24 horas
    plt.figure(figsize=(10, 6))
    for dia, dados in temperaturas_por_dia.items():
        plt.plot(dados['data_hora'], dados['temperaturas'], label=dia)
    plt.title('Temperatura ao longo de 24 horas')
    plt.xlabel('Data e Hora')
    plt.ylabel('Temperatura (°C)')
    plt.legend(loc='upper right')
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    # # Calcular a média de temperatura por dia da semana
    medias_temperatura = [sum(dados['temperaturas']) / len(dados['temperaturas']) for dados in temperaturas_por_dia.values()]
    dias_semana_lista = list(dias_semana.values())

    # # Plotar a média de temperatura por dia da semana
    plt.figure(figsize=(10, 6))
    plt.bar(dias_semana_lista, medias_temperatura, color='skyblue')
    plt.title('Média de temperatura por dia da semana')
    plt.xlabel('Dia da semana')
    plt.ylabel('Temperatura média (°C)')
    plt.grid(axis='y')
    plt.show()


    medias_temperatura_placa = [sum(dados['temperaturaPlaca']) / len(dados['temperaturaPlaca']) for dados in temperaturas_por_dia.values()]
    dias_semana_lista = list(dias_semana.values())

   # # Plotar a média de temperatura por dia da semana
    plt.figure(figsize=(10, 6))
    plt.bar(dias_semana_lista, medias_temperatura_placa, color='skyblue')
    plt.title('Média de temperatura da placa por dia da semana')
    plt.xlabel('Dia da semana')
    plt.ylabel('Temperatura média (°C)')
    plt.grid(axis='y')
    plt.show()

    
    # Plotar um gráfico de barra dupla
    plt.figure(figsize=(14, 6))
    bar_width = 0.35

    r1 = range(len(dias_semana_lista))
    r2 = [x + bar_width for x in r1]

    # Plotar as barras de temperatura
    bar1 = plt.bar(r1, medias_temperatura, color='skyblue', width=bar_width, label='Temperatura')
    bar2 = plt.bar(r2, medias_temperatura_placa, color='orange', width=bar_width, label='Temperatura da Placa')

    plt.xlabel('Dia da semana')
    plt.ylabel('Temperatura média (°C)')
    plt.title('Comparação de temperatura externa e a temperatura da placa')
    plt.xticks([r + bar_width / 2 for r in range(len(dias_semana_lista))], dias_semana_lista)
    plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
    plt.show()

except mysql.connector.Error as error:
    print("Erro ao conectar ao banco de dados:", error)
