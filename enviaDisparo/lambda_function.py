from utils.config import *

import requests
import time
import json 
import boto3
import os 

URL_DIALOG_MESSAGE = 'https://waba.360dialog.io/v1/messages'


def NEW_enviar_HSM(http_session = None, payload = None, token = None):
    # Preparando a mensagem para ser enviada
    HEADERS = {
    'D360-Api-Key': token,
    'Content-Type': "application/json",
    }


    print("payload:", payload, " | type: ", type(payload))
    try:
        payload = json.loads(payload)
        payload = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    except Exception as e:
        print(f"ERROR(lambda_function.NEW_enviar_HSM): Erro no payload: {e}")
        return 'False'

    try:
        if http_session:
            response = http_session.post(URL_DIALOG_MESSAGE, data=payload, headers=HEADERS)
        else:
            response = requests.post(URL_DIALOG_MESSAGE, data=payload, headers=HEADERS)
        print(f"INFO(send_wpp.NEW_enviar_HSM): HSM Enviado = {payload}, Resposta POST Dialog360 = {response.text}")
        
        if response.headers['Content-Type'] == 'application/json':
            response_json = json.loads(response.text)
            if 'errors' in response_json:
                if 'code' not in response_json['errors'][0]:
                    return 'False'
                if response_json['errors'][0]['code'] in [2001, 2060, 2062]:
                    return 'Problema no HSM'
                if response_json['errors'][0]['code'] == 1013:
                    return 'Sem Whatsapp'
                return 'False'
            elif 'meta' in response_json and 'api_status' in response_json['meta'] and response_json['meta']['api_status'] == "stable":
                return 'True'
        else:
            print(f"WARNING(send_wpp.enviar_wpp): Received non-JSON response. Status code: {response.status_code}, Content-Type: {response.headers.get('Content-Type')}")
            
        return 'False'

    except Exception as e:
        print(f"ERROR(lambda_function.NEW_enviar_HSM): Erro no pos na dialog: {e}")
        # Se houver alguma exceção, retornar False
        if 'response' in locals() or 'response' in globals():
            print("Response:",response)
        return 'False'


def lambda_handler(event, context):  
    t_inicio = time.time()

    print('EVENT ', event)
    batch_hsm_payload = event['hsm_payload']
    batch_id = event['id']
    batch_campanha = event['campanha']
    batch_datetime = event['datetime']
    token = event['token_wpp']
    http_session = requests.Session() 
    
    print('IDS ', batch_id)

    ################## VERIFICAÇÃO DO HORÁRIO ATUAL ####################
    neg_id = []
    neg_campanha = []
    neg_date_time = []
    neg_response = []
    for count, payload in enumerate(batch_hsm_payload):
        response = NEW_enviar_HSM(http_session, payload, token)
        if response != 'True':
            neg_id.append(batch_id[count])
            neg_campanha.append(batch_campanha[count])
            neg_date_time.append(batch_datetime[count])
            neg_response.append(response)
            

    if osFunc('Ambiente_Lambda'):
        sqs_client = boto3.client('sqs')
        if 'producao' in os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            queue_url = "https://sqs.sa-east-1.amazonaws.com/191246610649/producao_posProcessamento.fifo"
        else:
            queue_url = "https://sqs.sa-east-1.amazonaws.com/191246610649/homolog_posProcessamento.fifo"
        # Envia a mensagem para a fila SQS
        if len(neg_response) > 0:
            MessageBody = json.dumps({'responses': neg_response, 'id':neg_id, 'campanha':neg_campanha, 'datetime':neg_date_time})
            print("INFO(lambda_function.lambda_handler): sqs MessageBody: ", MessageBody)
            sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=MessageBody, 
                MessageGroupId='envio_direto')
    t_fim = time.time() 
    print("T Final:", t_fim - t_inicio)    
    print("Tempo por disparo:", (t_fim-t_inicio)/len(batch_hsm_payload))
    return {'statusCode': 200}
 


