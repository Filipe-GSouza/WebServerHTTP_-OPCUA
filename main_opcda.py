# Tutorial de OPC DA Cliente 
# Link: https://medium.com/@gabrielveigalima/conectando-opc-da-via-dcom-com-python-3-82f12db75d

import requests
from win32com.client import Dispatch
import pandas as pd 
from datetime import datetime
from io import StringIO

# Endereço do servidor OPC DA
opcda_server_address = "opcda://localhost/Your.OPC.DA.Server"

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
        
        # Inicializar o cliente OPC DA
        #opc_da_client = Dispatch("OPC.DA.Automation")
        opc_da_client =
        # Conectar ao servidor OPC DA
        opc_da_client.Connect(opcda_server_address)

        #Iterar sobre as colunas do DataFrame e enviar os dados para o servidor OPC DA
        for coluna in last_reading.columns:
            item_nome = f"Your.OPC.Item.{coluna}"  
            valores = last_reading[coluna].tolist()
            # Adicionar o item ao grupo OPC DA e escrever os valores
            opc_da_client.OPCGroups.DefaultGroup.OPCItems.AddItem(item_nome).Write(valores)
        
        # Desconecta do servidor OPC DA
        opc_da_client.Disconnect()
        
        #debug test - print("Dados enviados para o servidor OPC DA com sucesso.")
    else:
        print(f"Falha na requisição. Código de status: {response.status_code}")
        continue
