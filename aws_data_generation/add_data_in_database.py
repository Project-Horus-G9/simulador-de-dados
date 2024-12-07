import subprocess
import boto3
import os
import json
import mysql.connector
from mysql.connector import Error

s3 = boto3.client('s3')

def conectar_banco():
    try:
        conexao = mysql.connector.connect(
            host='localhost',
            database='horus_trusted',
            user='root',
            password='root.123'
        )
        if conexao.is_connected():
            print("Conexão com o banco de dados estabelecida com sucesso!")
            return conexao
    except Error as e:
        print(f"Erro ao conectar com o banco de dados: {e}")
        return None

def buscar_ou_inserir_empresa(conexao, nome_empresa, cnpj_empresa):
    """Busca o ID da empresa com o nome especificado. Insere uma nova empresa se não for encontrada."""
    cursor = conexao.cursor()
    cursor.execute("SELECT id_empresa FROM empresa WHERE nome = %s", (nome_empresa,))
    resultado = cursor.fetchone()

    if resultado:
        return resultado[0]
    else:
        cursor.execute("INSERT INTO empresa (nome, cnpj) VALUES (%s, %s)", (nome_empresa, cnpj_empresa))
        conexao.commit()
        return cursor.lastrowid

def verificar_ou_inserir_setor(conexao, nome_setor, id_empresa):
    cursor = conexao.cursor()
    cursor.execute("SELECT id_setor FROM setor WHERE nome = %s AND fk_empresa = %s", (nome_setor, id_empresa))
    resultado = cursor.fetchone()

    if resultado:
        return resultado[0]
    else:
        cursor.execute("INSERT INTO setor (nome, fk_empresa) VALUES (%s, %s)", (nome_setor, id_empresa))
        conexao.commit()
        return cursor.lastrowid

def verificar_ou_inserir_painel(conexao, nome_painel, id_setor):
    cursor = conexao.cursor()
    cursor.execute("SELECT id_painel FROM painel WHERE nome = %s AND fk_setor = %s", (nome_painel, id_setor))
    resultado = cursor.fetchone()

    if resultado:
        return resultado[0]
    else:
        cursor.execute("INSERT INTO painel (nome, fk_setor) VALUES (%s, %s)", (nome_painel, id_setor))
        conexao.commit()
        return cursor.lastrowid

import json

def inserir_dados_banco(dados_json):
    conexao = conectar_banco()
    if not conexao:
        return

    dados = json.loads(dados_json)
    nome_empresa = dados.get("cliente")
    cnpj_empresa = dados.get("cnpj", "00000000000000")  # CNPJ padrão se não fornecido

    id_empresa = buscar_ou_inserir_empresa(conexao, nome_empresa, cnpj_empresa)
    if not id_empresa:
        print(f"Erro: Empresa '{nome_empresa}' não encontrada ou inserida no banco de dados.")
        conexao.close()
        return

    setores = dados.get("setores", [])

    for setor in setores:
        nome_setor = setor.get("setor")
        if nome_setor is None:
            print("Erro: Nome do setor não encontrado.")
            continue

        id_setor = verificar_ou_inserir_setor(conexao, nome_setor, id_empresa)
        print(f'Setor: {id_setor}')

        paineis = setor.get("paineis", [])
        for painel in paineis:
            nome_painel = painel.get("painel")
            if nome_painel is None:
                print("Erro: Nome do painel não encontrado.")
                continue

            id_painel = verificar_ou_inserir_painel(conexao, nome_painel, id_setor)
            print(f'Painel: {id_painel}')

            dados_painel = painel.get("dados", [])
            if not dados_painel:
                print(f"Erro: Dados não encontrados para o painel '{nome_painel}'.")
                continue

            for leitura in dados_painel:
                cursor = conexao.cursor()
                cursor.execute("""
                    INSERT INTO dados_leitura (
                        data, obstrucao, luminosidade, temperatura_externa, temperatura_interna, tensao,
                        energia_gerada, energia_esperada, eficiencia, ceu, direcionamento, inclinacao,
                        umidade, fk_painel
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    leitura.get("data_hora"), leitura.get("obstrucao"), leitura.get("luminosidade"),
                    leitura.get("temperatura_externa"), leitura.get("temperatura_interna"), leitura.get("tensao"),
                    leitura.get("energia_gerada"), leitura.get("energia_esperada"), leitura.get("eficiencia"),
                    leitura.get("ceu"), leitura.get("direcionamento"), leitura.get("inclinacao"),
                    leitura.get("umidade", 0.0), id_painel  # Colocando um valor padrão para umidade se não estiver presente
                ))
                conexao.commit()

    conexao.close()


def executar_script_sql(usuario, senha, arquivo_sql):
    try:
        comando = f"mysql -u {usuario} -p{senha} < {arquivo_sql}"
        subprocess.run(comando, shell=True, check=True)
        print("Script SQL executado com sucesso.")
    except subprocess.CalledProcessError as e:
        print("Erro ao executar o script SQL:", e)

def listar_arquivos(bucket_name):
    resposta = s3.list_objects_v2(Bucket=bucket_name)
    arquivos = [obj['Key'] for obj in resposta.get('Contents', [])]
    return arquivos

def obter_data_arquivo(bucket_name, arquivo_key):
    resposta = s3.head_object(Bucket=bucket_name, Key=arquivo_key)
    return resposta['LastModified']

def ler_nome_ultimo_arquivo(arquivo_txt):
    if os.path.exists(arquivo_txt):
        with open(arquivo_txt, 'r') as f:
            ultimo_arquivo = f.read().strip()
            return ultimo_arquivo
    return None

def baixar_conteudo_arquivo(bucket_name, arquivo_key):
    resposta = s3.get_object(Bucket=bucket_name, Key=arquivo_key)
    return resposta['Body'].read().decode('utf-8')

def exibir_arquivos_mais_recentes(bucket_name, arquivo_txt):
    ultimo_arquivo = ler_nome_ultimo_arquivo(arquivo_txt)
    tst = "tst"

    if not ultimo_arquivo:
        print("Arquivo de texto não encontrado ou vazio.")
        return

    arquivos = listar_arquivos(bucket_name)

    if ultimo_arquivo not in arquivos:
        print(f"O arquivo {ultimo_arquivo} não encontrado no bucket.")
        return

    data_ultimo_arquivo = obter_data_arquivo(bucket_name, ultimo_arquivo)

    arquivos_recentes = [
        arquivo for arquivo in arquivos
        if arquivo.endswith('.json') and obter_data_arquivo(bucket_name, arquivo) >= data_ultimo_arquivo
    ]

    if ultimo_arquivo in arquivos_recentes:
        arquivos_recentes.insert(0, ultimo_arquivo)

    for arquivo in arquivos_recentes:
        try:
            conteudo = baixar_conteudo_arquivo(bucket_name, arquivo)
            dados_json = json.loads(conteudo)

            # Inserir dados no banco de dados
            if arquivo != tst:
                print(f"Inserindo dados do arquivo: {arquivo}")
                inserir_dados_banco(conteudo)
                tst = arquivo

            # Atualizar o nome do último arquivo processado
            with open(arquivo_txt, 'w') as f:
                f.write(arquivo)

        except Exception as e:
            print(f"Erro ao processar o arquivo {arquivo}: {e}")

    arquivo_sql = "/home/ubuntu/horus/simulador-de-dados/aws_data_generation/transfer_data.sql"
    executar_script_sql("root", "root.123", arquivo_sql)

# Defina o nome do bucket e o arquivo de texto
bucket_trusted = 'client-horus'
arquivo_txt = '/home/ubuntu/horus/simulador-de-dados/aws_data_generation/last_archive.txt'

# Exibir arquivos JSON adicionados após o último arquivo registrado e inserir os dados no banco de dados
exibir_arquivos_mais_recentes(bucket_trusted, arquivo_txt)
