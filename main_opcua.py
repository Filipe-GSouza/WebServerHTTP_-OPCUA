import requests
import pandas as pd 
from datetime import datetime
from io import StringIO
# Credenciais de autenticação server draegger
username = "RVP3900"
password = "srvc"

while(1):
    # Construir o nome do arquivo CSV usando a data atual
    data_atual = datetime.now().strftime("%y%m%d")
    nome_arquivo_csv = f"{data_atual}00.csv"
    # URL do servidor web do display dragger
    url_regard_1 = f"http://10.55.20.69/logs/D1_3min/{nome_arquivo_csv}"
    url_regard_2 = f"http://10.55.20.69/logs/D1_3min/{nome_arquivo_csv}"

    # Requisição GET para obter os dados do display dragger com autenticação
    response = requests.get(url_regard_1, auth=(username, password))

    # Verificar se a requisição foi bem-sucedida (código 200)
    if response.status_code == 200:
        # Carregar os dados CSV diretamente para um DataFrame do pandas
        df = pd.read_csv(StringIO(response.text))
        last_reading = df.tail(1)
    else:
        print(f"Falha na requisição. Código de status: {response.status_code}")
        continue
