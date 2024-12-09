import mysql.connector
import json

# Conectar ao banco de dados MySQL
def conectar_bd():
    return mysql.connector.connect(
        host="localhost",  # Ajuste se for diferente
        user="root",  # Seu usuário MySQL
        password="Hotus123",  # Sua senha MySQL
        database="horus_trusted"  # Banco de dados
    )

# Função para inserir os dados da empresa
def inserir_empresa(cursor, nome_empresa):
    query = "INSERT INTO empresa (nome) VALUES (%s)"
    cursor.execute(query, (nome_empresa,))
    return cursor.lastrowid

# Função para inserir um setor
def inserir_setor(cursor, nome_setor, fk_empresa):
    query = "INSERT INTO setor (nome, fk_empresa) VALUES (%s, %s)"
    cursor.execute(query, (nome_setor, fk_empresa))
    return cursor.lastrowid

# Função para inserir painel
def inserir_painel(cursor, nome_painel, fk_setor):
    query = "INSERT INTO painel (nome, fk_setor) VALUES (%s, %s)"
    cursor.execute(query, (nome_painel, fk_setor))
    return cursor.lastrowid

# Função para inserir dados de leitura
def inserir_dados_leitura(cursor, dados, fk_painel):
    query = """
    INSERT INTO dados_leitura 
    (temperatura_interna, temperatura_externa, energia_gerada, energia_esperada, tensao, eficiencia,
    direcionamento, inclinacao, luminosidade, ceu, obstrucao, umidade, data, fk_painel) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(query, (
        dados['temperatura_interna'], dados['temperatura_externa'], dados['energia_gerada'], 
        dados['energia_esperada'], dados['tensao'], dados['eficiencia'], dados['direcionamento'], 
        dados['inclinacao'], dados['luminosidade'], dados['ceu'], dados['obstrucao'], 
        dados['umidade'], dados['data_hora'], fk_painel
    ))

# Função para processar o JSON e inserir os dados no banco de dados
def processar_dados(json_data):
    conexao = conectar_bd()
    cursor = conexao.cursor()

    # Iterar pelas informações do JSON
    empresa_id = inserir_empresa(cursor, json_data['cliente'])  # Inserir empresa uma única vez
    
    for setor_data in json_data['setores']:
        setor_id = inserir_setor(cursor, f"Setor {setor_data['setor']}", empresa_id)  # Inserir setor
        
        for painel_data in setor_data['paineis']:
            painel_id = inserir_painel(cursor, f"Painel {painel_data['painel']}", setor_id)  # Inserir painel
            
            for dados in painel_data['dados']:
                inserir_dados_leitura(cursor, dados, painel_id)  # Inserir dados de leitura
                
    # Confirmar as inserções e fechar a conexão
    conexao.commit()
    cursor.close()
    conexao.close()

# Exemplo de como carregar o JSON de um arquivo
if __name__ == "__main__":
    with open('dados.json', 'r') as f:
        json_data = json.load(f)
    
    processar_dados(json_data)
    print("Dados inseridos com sucesso!")
