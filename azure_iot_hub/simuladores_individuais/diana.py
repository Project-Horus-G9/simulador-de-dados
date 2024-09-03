#  sensor de potência eletrica

import numpy as np
import matplotlib.pyplot as plt


horas_do_dia = np.linspace(0, 24, 100)
potencia_maxima_NOCT = 400
percentual_captacao = 0.4

potencia_captada = np.random.uniform(0, potencia_maxima_NOCT * percentual_captacao, len(horas_do_dia))
potencia_captada[horas_do_dia < 6] *= 0.1
potencia_captada[horas_do_dia > 18] *= 0.1

plt.figure(figsize=(10, 6))
plt.plot(horas_do_dia, potencia_captada, color='blue', label='Potência Captada')
plt.axhline(y=potencia_maxima_NOCT, color='red', linestyle='--', label='Potência Máxima NOCT')
plt.xlabel('Período analisado (h)')
plt.ylabel('Potência (W)')
plt.title('Análise da Placa Solar KuMax CS3U')
plt.legend()
plt.grid(True)
plt.ylim(0, potencia_maxima_NOCT + 50)
plt.xticks(np.arange(0, 25, 2))
plt.show()
