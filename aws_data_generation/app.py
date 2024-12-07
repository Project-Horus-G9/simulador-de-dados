import subprocess

def execute_script(script_name):
    try:
        print(f"Executando {script_name}...")
        result = subprocess.run(['python', script_name], check=True)
        print(f"{script_name} executado com sucesso.\n")
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar {script_name}: {e}")
        exit(1)

# Ordem de execução dos scripts
scripts = ["generate_raw.py", "treatment_trusted.py", "filter_client.py"]

for script in scripts:
    execute_script(script)
