from utils.config import *
import boto3
import json
import time
from datetime import datetime, timedelta
import pytz
from utils.connect_db import *

#TESTE

def lambda_handler(event, context):
    start_time = time.time()

    # Registrar o início da execução
    print("Lambda function started")

    # Parse o evento
    event_parse_start_time = time.time()
    print(json.dumps(event))
    record = event['Records'][0]
    event_parse_end_time = time.time()

    # Parse o corpo da requisição
    body_start_time = time.time()
    if isinstance(record['body'], dict):
        body_json = record['body']
    else:
        body_json = json.loads(record['body'])
    body_end_time = time.time()

    # Delete da mensagem do SQS
    receipt_handle = record['receiptHandle']
    queue_url = osFunc('urlSQS_post_status') 
    sqs = boto3.client('sqs')

    delete_start_time = time.time()
    try:
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    except Exception as e:
        print(f"ERROR(lambda_function.lambda_handler): Erro no delete do sqs {e}")
    delete_end_time = time.time()

    # Processamento inicial do corpo da mensagem
    process_body_start_time = time.time()
    status = body_json['status']
    telefone = body_json['telefone']
    telefone_wpp = body_json['telefone_wpp']
    
    if  status not in ['read', 'delivered', 'failed']: # lidar com outros statuses mais pra frente
        return {
            'statusCode': 200,
            'body': json.dumps('STATUS PROCESSADO')
        }

    if len(telefone) == 12 and telefone[:2] == '55':
        telefone = telefone[:4] + '9' + telefone[4:]
    process_body_end_time = time.time()

    # Execução da query
    query_start_time = time.time()
    data_antiguidade_maxima = datetime.now(pytz.timezone('America/Sao_Paulo')) - timedelta(days=osFunc('ANTIGUIDADE_MAXIMA'))
    
    query = f'''SELECT 
                C.ID_LAST_FLAG,
                C.DATETIME_CONVERSA,
                C.ID_CAMPANHA,
                (SELECT COUNT(M.TELEFONE) 
                 FROM MENSAGEM M 
                 WHERE M.TELEFONE = C.TELEFONE 
                 AND M.DATETIME_CONVERSA = C.DATETIME_CONVERSA) AS NUM_MSG
            FROM CONVERSA C
            WHERE C.TELEFONE = {telefone} AND C.ID_RESULTADO != {osFunc("ADICIONADO")} AND C.DATETIME_CONVERSA >= '{data_antiguidade_maxima}'
            ORDER BY C.DATETIME_CONVERSA DESC
            LIMIT 1;'''

    df = query_to_dataframe(query)
    query_end_time = time.time()

    # Verificação do resultado da query
    verify_query_start_time = time.time()
    if df.empty or df['NUM_MSG'].iloc[0] >= 2:
        return {
            'statusCode': 200,
            'body': json.dumps('STATUS PROCESSADO')
        }
    verify_query_end_time = time.time()

    # Atualização de flags
    update_flags_start_time = time.time()
    current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')) 
    datetime_flag = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    
    if status == 'delivered':
        if df['ID_LAST_FLAG'].iloc[0] == osFunc('HSM_ENVIADO'):
            id_flag = osFunc("HSM_RECEBIDO")
        else:
            return {
                'statusCode': 200,
                'body': json.dumps('STATUS PROCESSADO')
            } 
        
    # Inicializar tempos de resposta do SQS
    sqs_response_start_time = None
    sqs_response_end_time = None

    if status == 'read':
        if df['ID_LAST_FLAG'].iloc[0] == osFunc('HSM_ENVIADO'):
            id_flag = osFunc("HSM_RECEBIDO")
            execute_query_db(f"UPDATE CONVERSA SET ID_LAST_FLAG = {id_flag} WHERE TELEFONE = {telefone} AND DATETIME_CONVERSA = '{df['DATETIME_CONVERSA'].iloc[0]}'")
            execute_query_db(
            f"""
            INSERT INTO FLAG_HISTORICO (DATETIME_CONVERSA, DATETIME_FLAG, TELEFONE, ID_CAMPANHA, ID_FLAG)
            VALUES ('{df['DATETIME_CONVERSA'].iloc[0]}','{datetime_flag}',{telefone},{df['ID_CAMPANHA'].iloc[0]},{id_flag});
            """)
            df = query_to_dataframe(query)
            
        if df['ID_LAST_FLAG'].iloc[0] == osFunc('HSM_RECEBIDO'):
            id_flag = osFunc("HSM_LIDO")
    
            if osFunc('Ambiente_Lambda'):
                sqs_client = boto3.client('sqs')
                queue_url = osFunc('urlSQS_status_read')
                sqs_response_start_time = time.time()
                sqs_response = sqs_client.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps({'telefone': str(telefone), 'telefone_wpp': str(telefone_wpp)}), 
                    MessageGroupId='envio_direto')
                sqs_response_end_time = time.time()
                print('SQS Response ', sqs_response)
    
    elif df['ID_LAST_FLAG'].iloc[0] == osFunc('HSM_ENVIADO') and status == 'failed':
        id_flag = osFunc("REENVIAR_HSM")
        id_resultado = osFunc("ADICIONADO")
        execute_query_db(f"UPDATE CONVERSA SET ID_RESULTADO = {id_resultado} WHERE TELEFONE = {telefone} AND DATETIME_CONVERSA = '{df['DATETIME_CONVERSA'].iloc[0]}'")
    
    execute_query_db(f"UPDATE CONVERSA SET ID_LAST_FLAG = {id_flag} WHERE TELEFONE = {telefone} AND DATETIME_CONVERSA = '{df['DATETIME_CONVERSA'].iloc[0]}'")
    execute_query_db(
    f"""
    INSERT INTO FLAG_HISTORICO (DATETIME_CONVERSA, DATETIME_FLAG, TELEFONE, ID_CAMPANHA, ID_FLAG)
    VALUES ('{df['DATETIME_CONVERSA'].iloc[0]}','{datetime_flag}',{telefone},{df['ID_CAMPANHA'].iloc[0]},{id_flag});
    """)
    update_flags_end_time = time.time()

    # Disposição do banco de dados
    dispose_db_start_time = time.time()
    DatabaseConnection.dispose_engine()
    dispose_db_end_time = time.time()

    end_time = time.time()

    # Imprimir o relatório de tempos
    print("Relatório de Tempos:")
    print(f"Tempo total de execução: {end_time - start_time:.2f} segundos")
    print(f"Tempo para parse do evento: {event_parse_end_time - event_parse_start_time:.2f} segundos")
    print(f"Tempo para parse do corpo da requisição: {body_end_time - body_start_time:.2f} segundos")
    print(f"Tempo para deletar mensagem do SQS: {delete_end_time - delete_start_time:.2f} segundos")
    print(f"Tempo para processamento inicial do corpo da mensagem: {process_body_end_time - process_body_start_time:.2f} segundos")
    print(f"Tempo para execução da query: {query_end_time - query_start_time:.2f} segundos")
    print(f"Tempo para verificação do resultado da query: {verify_query_end_time - verify_query_start_time:.2f} segundos")
    print(f"Tempo para atualização de flags: {update_flags_end_time - update_flags_start_time:.2f} segundos")
    print(f"Tempo para disposição do banco de dados: {dispose_db_end_time - dispose_db_start_time:.2f} segundos")
    print(f"Tempo para resposta do SQS: {sqs_response_end_time - sqs_response_start_time:.2f} segundos")

    return {
        'statusCode': 200,
        'body': json.dumps('STATUS PROCESSADO')
    }
