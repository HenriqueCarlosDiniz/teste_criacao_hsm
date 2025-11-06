import json 
import boto3
from utils.config import *
from utils.send_wpp import *

def lambda_handler(event, context):
    
    print(json.dumps(event))

    record = event['Records'][0]
 
    #Parse o corpo da requisição
    if isinstance(record['body'], dict):
        body_json = record['body']
    else:
        body_json = json.loads(record['body'])

    receipt_handle = record['receiptHandle']
    queue_url = osFunc('urlSQS_retry_enviar_msg') 
    sqs = boto3.client('sqs')
    try:
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    except Exception as e:
        print(f"ERROR(lambda_function.lambda_handler): Erro no delete do sqs {e}")
    
    ########################RESGATANDO OS DADOS###############################
    telefone = body_json['telefone']
    header = body_json['header']
    msg = body_json['msg']
     
    ########################ENVIANDO AS MENSAGENS############################
    enviar_wpp(telefone, msg, quebrar_mensagem_condition=True, header = header)
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
