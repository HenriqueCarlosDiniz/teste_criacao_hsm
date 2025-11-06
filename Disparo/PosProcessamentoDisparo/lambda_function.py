from utils.config import *
from datetime import datetime
from utils.connect_db import execute_query_db
import pytz
import boto3
import json


def lambda_handler(event, context): 
    #########################################################
    ################ APAGA MSG NA FILA ######################
    #########################################################
    record = event['Records'][0]
    receipt_handle = record['receiptHandle'] 
    queue_url = 'https://sqs.sa-east-1.amazonaws.com/191246610649/producao_posProcessamento.fifo'
    sqs = boto3.client('sqs')
    try:
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    except Exception as e:
            print(f"ERROR(lambda_function.lambda_handler): Erro no delete do sqs {e}")
            
    if isinstance(record['body'], dict):
            body_json = record['body']
    else:
        body_json = json.loads(record['body'])
    current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo'))
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    responses = body_json['responses']
    batch_id = body_json['id']
    batch_campanha = body_json['campanha']
    batch_datetime = body_json['datetime']
    print(f"INFO(lambda_handler:posProcessamentoDisparo): {responses}, {batch_id}, {batch_campanha}, {batch_datetime}")
    queries = []
    for count, response in enumerate(responses):
        key = [batch_datetime[count],batch_id[count],batch_campanha[count]]
        if response == 'Telemarketing':
            #mensagem = "Agendamento Telemarketing"
            flag = "FINALIZADO"
            resultado = "TELEMARKETING" 
        elif response in ['False', 'Sem WhatsApp', 'Usuário em experimento']: 
            flag = "NONE"
            resultado = "FALHA" 
        elif response == 'Problema no HSM': 
            #mensagem = "Erro na API do HSM"
            flag = "REENVIAR_HSM" 
            resultado = "ADICIONADO"
        else: #condições nao tratadas
            flag = "NONE"
            resultado = "FALHA"
        # Query de atualização para a tabela CONVERSA

        update_query_ativo = f"""
            UPDATE CONVERSA
            SET ATIVO = 1
            WHERE TELEFONE = {key[1]} AND           
            DATETIME_CONVERSA = (
                SELECT DATETIME_CONVERSA
                FROM CONVERSA
                WHERE DATETIME_CONVERSA < '{key[0]}' AND ATIVO = 0
                ORDER BY DATETIME_CONVERSA DESC
                LIMIT 1
            )
            AND ATIVO = 0;
                    """
        queries.append(update_query_ativo)
        
        update_query_conversa = f"""
            UPDATE CONVERSA
            SET ID_LAST_FLAG = {osFunc(flag)}, ID_RESULTADO = {osFunc(resultado)}, ATIVO = 0
            WHERE TELEFONE = {key[1]}
            AND DATETIME_CONVERSA = '{key[0]}'
            AND ID_CAMPANHA = {key[2]};
        """
        queries.append(update_query_conversa)
    
        # Query de inserção para a tabela FLAG_HISTORICO
        insert_query_flag_historico = f"""
            INSERT INTO FLAG_HISTORICO (DATETIME_CONVERSA, DATETIME_FLAG, TELEFONE, ID_CAMPANHA, ID_FLAG)
            VALUES ('{key[0]}', '{formatted_datetime}', {key[1]}, {key[2]}, {osFunc(flag)});
        """
        queries.append(insert_query_flag_historico)
        
        # Query de inserção para a tabela MENSAGEM
        # insert_query_mensagem = f"""
        #     INSERT INTO MENSAGEM (TELEFONE, ID_CAMPANHA, DATETIME_CONVERSA, MENSAGEM_DATETIME, CONTEUDO, CUSTO, AUTOR_ID_AUTOR, ID_MENSAGEM)
        #     VALUES ({key[1]}, {key[2]}, '{key[0]}', '{formatted_datetime}', '{mensagem}', 0, 0, NULL);
        # """
        # queries.append(insert_query_mensagem)
    
    # Executar cada query
    count = 1
    for query in queries:
        count+=1
        execute_query_db(query)
    
    # return mensagem, osFunc(flag), osFunc(resultado), formatted_datetime
    return {'status': 200}