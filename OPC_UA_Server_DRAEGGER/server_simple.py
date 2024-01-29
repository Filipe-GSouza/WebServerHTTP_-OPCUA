import sys
sys.path.insert(0, "..")
import time
import requests
import pandas as pd 

from datetime import datetime
from io import StringIO
from opcua import ua, Server

#---------------------------Credenciais de autenticação server http draegger---------------------------
username = "RVP3900"
password = "srvc"
#-------------------------------------------------------------------------------------------------------


if __name__ == "__main__":

    #-------------------------Cria, configura e inicia um servidor OPC UA-------------------------------
    server = Server()
    server.set_endpoint("opc.tcp://127.0.0.1:4840/freeopcua/server/")

    server.set_security_policy([ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt])
    # load server certificate and private key. This enables endpoints
    server.load_certificate("C:/Users/bruno/Desktop/Projeto_Regard/teste cliente/my_cert.pem")
    server.load_private_key("C:/Users/bruno/Desktop/Projeto_Regard/teste cliente/private_key.pem")
    # setup our own namespace, not really necessary but should as spec
    uri = "http://examples.freeopcua.github.io"
    idx = server.register_namespace(uri)
    # get Objects node, this is where we should put our nodes
    objects = server.get_objects_node()

    #---------------------------------Populando o espaço de endereços-----------------------------------
    regard_1 = objects.add_object(idx, "Regard_1")
    regard_2 = objects.add_object(idx, "Regard_2")
    
    r1_comStatus = regard_1.add_variable(idx, "Comunic_Status_R1", 0.0)
    r1_ch01 = regard_1.add_variable(idx, "R1_CH01", 0.0)
    r1_ch02 = regard_1.add_variable(idx, "R1_CH02", 0.0)
    r1_ch03 = regard_1.add_variable(idx, "R1_CH03", 0.0)
    r1_ch04 = regard_1.add_variable(idx, "R1_CH04", 0.0) 
    r1_ch05 = regard_1.add_variable(idx, "R1_CH05", 0.0)
    r1_ch06 = regard_1.add_variable(idx, "R1_CH06", 0.0)
    r1_ch07 = regard_1.add_variable(idx, "R1_CH07", 0.0)
    r1_ch08 = regard_1.add_variable(idx, "R1_CH08", 0.0)

    r2_comStatus = regard_1.add_variable(idx, "Comunic_Status_R2", 0.0)
    r2_ch09 = regard_2.add_variable(idx, "R2_CH09", 0.0)
    r2_ch10 = regard_2.add_variable(idx, "R2_CH10", 0.0)
    r2_ch11 = regard_2.add_variable(idx, "R2_CH11", 0.0)
    r2_ch12 = regard_2.add_variable(idx, "R2_CH12", 0.0)
    r2_ch13 = regard_2.add_variable(idx, "R2_CH13", 0.0)
    r2_ch14 = regard_2.add_variable(idx, "R2_CH14", 0.0)
   #----------------------------------------------------------------------------------------------------
    #Verificar se realmente não é necessário usar 
    #myvar.set_writable()    # Set MyVariable to be writable by clients 
    
   #------------------------Inicia o servidor-----------------------------------------------------------
    server.start()
   #---------------------------------------------------------------------------------------------------- 
    
    try:
        while True:
            #----------Construir o nome do arquivo CSV usando a data atual-------
            data_atual = datetime.now().strftime("%y%m%d")
            nome_arquivo_csv = f"{data_atual}00.csv"
            
            #------Define o endereço de consulta do datalog de cada DRAEGGER-----
            url_regard_1 = f"http://10.55.20.69/logs/D1_3min/{nome_arquivo_csv}"
            url_regard_2 = f"http://10.55.20.69/logs/D2_3min/{nome_arquivo_csv}"
            
            time.sleep(1)
            response1 = requests.get(url_regard_1, auth=(username, password))
            response2 = requests.get(url_regard_2, auth=(username, password))    
            #---------------------------------------------------------------------
           
            if response1.status_code == 200:
            #----Carregar os dados CSV diretamente para um DataFrame do pandas----
                df1 = pd.read_csv(StringIO(response1.text))
            #----Armazena a quantidade de linha do dataframe---------------------- 
                linha_r1 = len(df1)
            #--------------Atualiza o valor de cada variável---------------------- 
                r1_ch01.set_value(df1.iloc[linha_r1-1,2])
                r1_ch02.set_value(df1.iloc[linha_r1-1,3])
                r1_ch03.set_value(df1.iloc[linha_r1-1,4])
                r1_ch04.set_value(df1.iloc[linha_r1-1,5])
                r1_ch05.set_value(df1.iloc[linha_r1-1,6])
                r1_ch06.set_value(df1.iloc[linha_r1-1,7])
                r1_ch07.set_value(df1.iloc[linha_r1-1,8])
                r1_ch08.set_value(df1.iloc[linha_r1-1,9])
                r1_comStatus.set_value(1.0)
            else:
                print(f"Falha na requisição. Código de status: {response1.status_code}")
                r1_comStatus.set_value(0.0)
            
            if response2.status_code == 200:
            #----Carregar os dados CSV diretamente para um DataFrame do pandas----
                df2 = pd.read_csv(StringIO(response2.text))
            #----Armazena a quantidade de linha do dataframe----------------------         
                linha_r2 = len(df2)

            #--------------Atualiza o valor de cada variável---------------------- 
                r2_ch09.set_value(df2.iloc[linha_r2-1,2])
                r2_ch10.set_value(df2.iloc[linha_r2-1,3])
                r2_ch11.set_value(df2.iloc[linha_r2-1,4])
                r2_ch12.set_value(df2.iloc[linha_r2-1,5])
                r2_ch13.set_value(df2.iloc[linha_r2-1,6])
                r2_ch14.set_value(df2.iloc[linha_r2-1,7])
                r2_comStatus.set_value(1.0)
                
            else:
                print(f"Falha na requisição. Código de status: {response2.status_code}")
                r2_comStatus.set_value(0.0)
    finally:
        #close connection, remove subcsriptions, etc
        server.stop()