import mysql.connector
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# 1. Configurar as credenciais do Google Sheets
scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credenciais = ServiceAccountCredentials.from_json_keyfile_name('credenciais.json', scopes)
cliente = gspread.authorize(credenciais)

# 2. Conectar ao Google Sheets
nome_planilha = "HorusGroup"  # Substitua pelo nome da sua planilha
planilha = cliente.open(nome_planilha)

# 3. Conectar ao banco de dados MySQL
conexao = mysql.connector.connect(
    host="localhost",           # Substitua pelo host do seu banco
    user="root",         # Substitua pelo usuário do banco
    password="Horus123",       # Substitua pela senha do banco
    database="horus_dimensional"    # Substitua pelo nome do banco
)
cursor = conexao.cursor()

# 4. Extrair os dados da tabela fato
tabela = "fato_dados"  # Substitua pelo nome da tabela
cursor.execute(f"SELECT * FROM {tabela}")
colunas = [desc[0] for desc in cursor.description]  # Nomes das colunas
dados = cursor.fetchall()  # Dados da tabela

# 5. Verificar se existe uma quarta aba, caso contrário, criar uma
aba_nome = "Nova Aba"  # Nome da nova aba

# Verificar se a aba já existe
try:
    sheet = planilha.worksheet(aba_nome)
except gspread.exceptions.WorksheetNotFound:
    # Caso a aba não exista, cria a nova aba
    sheet = planilha.add_worksheet(title=aba_nome, rows="100", cols="20")  # Ajuste as dimensões conforme necessário

# 6. Verificar a quantidade de linhas e colunas na nova aba
num_linhas = len(sheet.get_all_values())

# 7. Atualizar cabeçalho se a planilha estiver vazia
if num_linhas == 0:
    sheet.update("A1", [colunas])  # Atualiza o cabeçalho

# 8. Atualizar os dados abaixo do cabeçalho
sheet.update(f"A{num_linhas + 1}", dados)  # Insere os dados nas linhas abaixo do cabeçalho

print("Dados enviados com sucesso!")

# Fechar a conexão com o banco de dados
cursor.close()
conexao.close()

