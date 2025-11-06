import json, os
import boto3
import datetime
import pytz

def lambda_handler(event, context):
    print("EVENT:", json.dumps(event))

    if event['httpMethod'] == 'GET':
        # Extrair parâmetros de consulta da solicitação GET
        query_params = event.get('queryStringParameters', {})
        
        hub_mode = query_params.get('hub.mode')
        hub_verify_token = query_params.get('hub.verify_token')
        hub_challenge = query_params.get('hub.challenge')
        
        if hub_mode == 'subscribe' and hub_verify_token == '':
            return {
                'statusCode': 200,
                'body': hub_challenge
            }
        return {
            'statusCode': 403,
            'body': 'Error, invalid token'
        }

   
    result = event['body']
 
    print("INPUT: ", result)
    result = json.loads(result)
    
    # Calcula o timestamp atual
    timestamp = datetime.datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S.%f")
    payload = {}

    # Verificar se é um payload de mensagem
    if not any(key in result for key in ('message', 'messaging', 'value', 'entry')):
        print('STATUS RECEBIDO')
        return {
            'statusCode': 200,
            'body': json.dumps('STATUS ENVIADO')
        }
    
    # verifica o tipo de payload de mensagem e faz os tratamentos        
    if any(key in result for key in ('message', 'messaging')):
        if result['message']:
            payload = result
        elif result['messaging']:
            payload = result['messaging'][0]
    elif 'entry' in result.keys():
        result = result['entry'][0]
        if 'messaging' in result.keys():
            payload = result['messaging'][0]
    elif 'value' in result.keys():
        if 'postback' in result['value'].keys():
            payload = result['value']
    else:
        return {
            'statusCode': 200,
            'body': json.dumps('STATUS ENVIADO')
        }
    
    # URL da fila SQS
    queue_url = os.environ['urlSQS_post']
    
    # Envia a mensagem para a fila SQS
    output = json.dumps({'post': json.dumps(payload), 'timestamp': str(timestamp)})
    sqs_client = boto3.client('sqs')
    response = sqs_client.send_message(
        QueueUrl=queue_url, 
        MessageBody=output,
        MessageGroupId='envio_direto',
    )
    print("OUTPUT: ", output)
    print('Enviou para produção')
    
    return {
        'statusCode': 200,
        'body': json.dumps('Enviado para producao_recebeMensagemMeta!')
    }