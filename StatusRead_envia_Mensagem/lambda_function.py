from utils.config import *
from utils.connect_db import *
import boto3
import json
import requests
from Models.unidade import Unidade
import time
import datetime
import pytz
from utils.connect_db import DatabaseConnection

def lambda_handler(event, context): 

    #############################################################################################
    # RESGATANDO AS INFORMACOES
    #############################################################################################
    print(json.dumps(event))

    record =  event['Records'][0]
 
    #Parse o corpo da requisição
    if isinstance(record['body'], dict):
        body_json = record['body']
    else:
        body_json = json.loads(record['body'])
 
    if osFunc('Ambiente_Lambda'):
        receipt_handle = record['receiptHandle']
        queue_url = osFunc('urlSQS_status_read') 
        sqs = boto3.client('sqs')
        try:
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        except Exception as e:
            print(f"ERROR(lambda_function.lambda_handler): Erro no delete do sqs {e}")

    telefone = body_json['telefone']
    telefone_wpp = body_json['telefone_wpp']
 

    #############################################################################################
    # IMPORTANDO AS INFORMAÇÕES DE CLIENTE
    #############################################################################################

    df = query_to_dataframe(f"""SELECT CONTATO.NOME_CLIENTE, CONVERSA.*, CAMP_AGENDAMENTO.DATA, CAMP_AGENDAMENTO.UNIDADE_PSD, CAMP_AGENDAMENTO.HORARIO FROM CONVERSA 
                                LEFT JOIN CAMP_AGENDAMENTO USING(TELEFONE)
                                LEFT JOIN CONTATO USING(TELEFONE)
                                WHERE TELEFONE = {telefone} AND ID_RESULTADO != {osFunc("ADICIONADO")} ORDER BY DATETIME_CONVERSA DESC LIMIT 1""")
 
    if df['ID_LAST_FLAG'][0] == osFunc('HSM_LIDO'):
        
        HSM_df = query_to_dataframe('SELECT * FROM HSM') 
        if df['ID_CAMPANHA'][0] in  [osFunc('NOSHOW'), osFunc('CANCELADOS')] :
            
            message_payload = HSM_df[HSM_df['ID_HSM'] == osFunc('HSM_LIDO_REAGENDAR')]['MESSAGE_PAYLOAD'].iloc[0]
            conteudo = HSM_df[HSM_df['ID_HSM'] == osFunc('HSM_LIDO_REAGENDAR')]['CONTEUDO'].iloc[0]
            
        elif df['ID_CAMPANHA'][0] ==  osFunc('CONFIRMACAO'):
            
            message_payload = HSM_df[HSM_df['ID_HSM'] == osFunc('HSM_LIDO_CONFIRMACAO')]['MESSAGE_PAYLOAD'].iloc[0]
            conteudo = HSM_df[HSM_df['ID_HSM'] == osFunc('HSM_LIDO_CONFIRMACAO')]['CONTEUDO'].iloc[0]
            unidade = Unidade(df['UNIDADE_PSD'][0])         
            message_payload = message_payload.replace('{unidade_agendamento}', '"' +unidade.nome + '"')
            message_payload = message_payload.replace('{data_agendamento}', '"' +df['DATA'].iloc[0].strftime("%d/%m/%Y") + '"')
            message_payload = message_payload.replace('{horario_agendamento}', '"' + str(df['HORARIO'].iloc[0])[-8:-3]+ '"')
            
            conteudo = conteudo.replace('{unidade_agendamento}', '"' +unidade.nome + '"')
            conteudo = conteudo.replace('{data_agendamento}', '"' +df['DATA'].iloc[0].strftime("%d/%m/%Y") + '"')
            conteudo = conteudo.replace('{horario_agendamento}', '"' + str(df['HORARIO'].iloc[0])[-8:-3]+ '"')
        
        elif df['ID_CAMPANHA'][0] ==  osFunc('LEADS_WHATSAPP'):
            
            message_payload = HSM_df[HSM_df['ID_HSM'] == osFunc('HSM_LIDO_LEADS_WHATSAPP')]['MESSAGE_PAYLOAD'].iloc[0]
            conteudo = HSM_df[HSM_df['ID_HSM'] == osFunc('HSM_LIDO_LEADS_WHATSAPP')]['CONTEUDO'].iloc[0]

        else:
            
            message_payload = HSM_df[HSM_df['ID_HSM'] == osFunc('HSM_LIDO_AGENDAR')]['MESSAGE_PAYLOAD'].iloc[0]
            conteudo = HSM_df[HSM_df['ID_HSM'] == osFunc('HSM_LIDO_AGENDAR')]['CONTEUDO'].iloc[0]

        message_payload = message_payload.replace('{cliente}', f''' "{df['NOME_CLIENTE'][0]}" ''')
        message_payload = message_payload.replace('{telefone}', f''' "{telefone}" ''')
        
        message_payload = json.loads(message_payload)
        message_payload = json.dumps(message_payload, ensure_ascii=False).encode('utf-8')
        
        # # Substituir vogais com acento
        # accented_vowels = {'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u', 'à': 'a', 'è': 'e', 'ì': 'i', 'ò': 'o', 'ù': 'u', 'â': 'a', 'ê': 'e', 'î': 'i', 'ô': 'o', 'û': 'u', 'ä': 'a', 'ë': 'e', 'ï': 'i', 'ö': 'o', 'ü': 'u', 'ã': 'a', 'õ': 'o', 'ñ': 'n', 'ç': 'c',
        #           'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U', 'À': 'A', 'È': 'E', 'Ì': 'I', 'Ò': 'O', 'Ù': 'U', 'Â': 'A', 'Ê': 'E', 'Î': 'I', 'Ô': 'O', 'Û': 'U', 'Ä': 'A', 'Ë': 'E', 'Ï': 'I', 'Ö': 'O', 'Ü': 'U', 'Ã': 'A', 'Õ': 'O', 'Ñ': 'N', 'Ç': 'C'}

        # for accented_vowel, replacement in accented_vowels.items():
        #     message_payload = message_payload.replace(accented_vowel, replacement)
        
        conteudo = conteudo.replace('{cliente}', f''' "{df['NOME_CLIENTE'][0]}" ''')
        conteudo = conteudo.replace('{telefone}', f''' "{telefone}" ''')
        
        
        print(message_payload)
        #############################################################################################
        # ENVIANDO O HSM
        #############################################################################################
        
        df_token = query_to_dataframe(f"SELECT TOKEN FROM TELEFONE_WPP WHERE TELEFONE_WPP = {telefone_wpp}")
        wpp_token = df_token["TOKEN"].iloc[0]
        
        URL_DIALOG_MESSAGE = os.environ['URL_DIALOG_MESSAGE']

        HEADERS = {
            'D360-Api-Key': wpp_token,
            'Content-Type': "application/json",
        }
 
        response = requests.post(URL_DIALOG_MESSAGE, data=message_payload, headers=HEADERS)  # Use HEADERS variable
        if response.headers['Content-Type'] == 'application/json':
            response_json = json.loads(response.text)
            print(response_json)
            if 'errors' in response_json:
                retorno = 'False'
            elif 'meta' in response_json and 'api_status' in response_json['meta'] and response_json['meta']['api_status'] == "stable":
                retorno = 'True'
            else:
                retorno = 'False'
        else:
            retorno = 'False'
            print(f"WARNING(send_wpp.enviar_wpp): Received non-JSON response. Status code: {response.status_code}, Content-Type: {response.headers.get('Content-Type')}")
         
        print('upload dos resultados')
        execute_query_db(f"UPDATE CONVERSA SET ID_LAST_FLAG = {osFunc('HSM_ENVIADO')} WHERE TELEFONE = {telefone} AND DATETIME_CONVERSA = '{df['DATETIME_CONVERSA'].iloc[0]}'")
        
        execute_query_db(f"""INSERT INTO MENSAGEM (TELEFONE, ID_CAMPANHA, DATETIME_CONVERSA, MENSAGEM_DATETIME, CONTEUDO, CUSTO, AUTOR_ID_AUTOR, ID_MENSAGEM, HSM) VALUES
                        ({df['TELEFONE'].iloc[0]}, 
                        {df['ID_CAMPANHA'].iloc[0]},
                        '{df['DATETIME_CONVERSA'].iloc[0]}',
                        '{datetime.datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S")}',
                         '{conteudo}',
                        0,
                        0,
                        '',
                        1)""")
                        
        DatabaseConnection.dispose_engine()