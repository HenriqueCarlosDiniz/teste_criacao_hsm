import json, os
import boto3
import time
import datetime
import pytz

def lambda_handler(event, context):
    
    print("EVENT:", json.dumps(event))
    
    header = event['headers']
    telefone_wpp = header['telefone_wpp']
 
    result = event['body']
    print("INPUT: ", result)
    result = json.loads(result)
    #result['telefone_wpp'] = telefone_wpp
    
    # Calcula o timestamp atual
    timestamp = datetime.datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S.%f")
    
    # Verificar se é status diferente de 'read'
    if 'statuses' in result.keys():
        print('STATUS RECEBIDO')
        status = result['entry'][0]['changes'][0]['statuses'][0]['status']
        telefone = result['entry'][0]['changes'][0]["statuses"][0]["recipient_id"]
        
        if not status in ['sent','delivered','deleted','failed','warning']: 
            # Configura o cliente do SQS
            sqs_client = boto3.client('sqs')
            # URL da fila SQS
            queue_url = os.environ['urlSQS_post_status']
            
            # Envia a mensagem para a fila SQS
            response = sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps({'telefone': str(telefone), 'status': status, 'timestamp': timestamp, 'telefone_wpp': telefone_wpp}), 
                MessageGroupId='envio_direto')  
            print('STATUS ENVIADO - ', response)
        return {
            'statusCode': 200,
            'body': json.dumps('STATUS ENVIADO')
        }
            
    
    
    # URL da fila SQS
    queue_url = os.environ['urlSQS_post']
     
    # Envia a mensagem para a fila SQS
    output = json.dumps({'post': json.dumps(result), 'timestamp': str(timestamp), 'telefone_wpp': telefone_wpp})
    sqs_client = boto3.client('sqs')
    response = sqs_client.send_message(
        QueueUrl=queue_url, 
        MessageBody=output,
        MessageGroupId='envio_direto', 
    )
    print("OUTPUT: ", output)
    print('Enviou para homolog')
    
    return {
        'statusCode': 200,
        'body': json.dumps('Enviado para homolog_recebeMensagem!')
    }
