# sensor de temperatura externa

import datetime
import random
import matplotlib.pyplot as plt

def obter_estacao(mes):
    if mes in [12, 1, 2]:
        return "Verão"
    elif mes in [3, 4, 5]:
        return "Outono"
    elif mes in [6, 7, 8]:
        return "Inverno"
    else:
        return "Primavera"

def gerar_temperatura(estacao):
    if estacao == "Verão":
        return random.uniform(30, 35)
    elif estacao == "Outono":
        return random.uniform(25, 30)
    elif estacao == "Inverno":
        return random.uniform(20, 25)
    else:
        return random.uniform(25, 30)

dias_semana = {
    "Monday": "Segunda-feira",
    "Tuesday": "Terça-feira",
    "Wednesday": "Quarta-feira",
    "Thursday": "Quinta-feira",
    "Friday": "Sexta-feira",
    "Saturday": "Sábado",
    "Sunday": "Domingo"
}

temperaturas_por_dia = {dia: [] for dia in dias_semana.values()}
data_atual = datetime.datetime.now()
dias_simulacao = 7
horario_inicio_pico = 9
horario_fim_pico = 15

for dia in range(dias_simulacao):
    dia_semana = dias_semana[data_atual.strftime("%A")]
    
    for hora in range(horario_inicio_pico, horario_fim_pico + 1):
        for meia_hora in range(0, 60, 30):
            data_hora = datetime.datetime(data_atual.year, data_atual.month, data_atual.day, hora, meia_hora)
            estacao_atual = obter_estacao(data_hora.month)
            temperatura = gerar_temperatura(estacao_atual)
            temperaturas_por_dia[dia_semana].append(temperatura)

    data_atual += datetime.timedelta(days=1)

for dia, temperaturas in temperaturas_por_dia.items():
    print(f"Dia da semana: {dia}")
    for idx, temp in enumerate(temperaturas, start=1):
        print(f"  - Coleta {idx}: {temp:.2f}°C")

plt.figure(figsize=(10, 6))
for dia, temperaturas in temperaturas_por_dia.items():
    plt.plot(range(1, len(temperaturas) + 1), temperaturas, label=dia)
plt.title('Temperatura ao longo da semana')
plt.xlabel('Período de coleta')
plt.ylabel('Temperatura (°C)')
plt.legend(loc='upper right')
plt.grid(True)
plt.show()

medias_temperatura = [sum(temperaturas) / len(temperaturas) for temperaturas in temperaturas_por_dia.values()]
dias_semana_lista = list(dias_semana.values())

plt.figure(figsize=(10, 6))
plt.bar(dias_semana_lista, medias_temperatura, color='skyblue')
plt.title('Média de temperatura por dia da semana')
plt.xlabel('Dia da semana')
plt.ylabel('Temperatura média (°C)')
plt.grid(axis='y')
plt.show()