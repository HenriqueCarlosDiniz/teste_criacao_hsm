from utils.config import *

import boto3
import json
import os
import pandas as pd
import pytz
import re 
from datetime import datetime

from utils.connect_db import insert_data_into_db, DatabaseConnection, execute_query_db
from utils.send_wpp import enviar_wpp
from Models.contato import Contato
from Models.conversa import Conversa

if osFunc('Ambiente_Lambda'): 
    from invoke_lambda import invokeFunction
else: 
    from logicaEncerramento.lambda_function import lambda_handler as logica_encerramento
    from geraResposta.lambda_function import lambda_handler as gera_resposta_bot
    
def processa_nome(name): 
    name = name.replace("", "ã")
    name = name.replace("'", "")
    name = re.sub('[^\w\s]', '', name)
    words = name.split()

    processed_words = []
    for word in words:
        if word.isupper():
            processed_words.append(word[0] + word[1:].lower())
        else:
            processed_words.append(word)

    processed_name = ' '.join(processed_words)

    return processed_name
    
def lambda_handler(event, context):
    print('EVENT ', json.dumps(event))

    if osFunc('Ambiente_Lambda'): 
        record = event['Records'][0]
        body = json.loads(record['body']) 
    else:
        body = event

    id_conversa = body['id_conversa'] 
    datetime_message = body['timestamp']

    if osFunc('Ambiente_Lambda'): 
        receipt_handle = record['receiptHandle']
        if 'producao' in os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            queue_url = osFunc('urlSQS_mensagem_producao')
        else:
            queue_url = osFunc('urlSQS_mensagem_homolog')
        sqs = boto3.client('sqs')
        try:
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        except Exception as e:
            print(f"ERROR(lambda_function.lambda_handler): Erro no delete do sqs {e}")

        if "attributes" in record and "ApproximateReceiveCount" in record["attributes"] and int(record["attributes"]["ApproximateReceiveCount"]) > 1:
            print("WARNING(lambda_function.lambda_handler): ApproximateReceiveCount > 1. Valor: ", record["attributes"]["ApproximateReceiveCount"])
            return {'statusCode': 400}

    id_contato = id_conversa[0]
    campanha = id_conversa[1]
    datetime_conversa = id_conversa[2]
    contato = Contato(id_contato)
    conversa = Conversa(contato, campanha, datetime_conversa) 
    datetime_message = datetime.strptime(datetime_message, '%Y-%m-%d %H:%M:%S.%f')
    current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo'))
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    
    _,horario_ultima_msg = conversa.get_horario_ultima_msg()
    time_diff = horario_ultima_msg - datetime_message

    if time_diff.total_seconds() > 0:
        print("INFO(time_diff:filtro_tempo): ", time_diff.total_seconds())
        return {'statusCode': 200} 

    # if contato.nome == 'receptivo' or contato.nome == 'leads_whatsapp':
    #     inputGeraResposta = {
    #                     'id_conversa' : id_conversa,
    #                     'tarefa': 'CADASTRO_RECEPTIVO'
    #                     }
    #     resposta_cadastro = call_geraResposta(inputGeraResposta)
    #     if resposta_cadastro['error'] != False:
    #         dados = resposta_cadastro['resposta']
    #         nome_contato = processa_nome(dados['Nome'])
    #         lugar = dados['Lugar']
    #         if nome_contato != 'False':
    #             contato.update_nome_contato(nome_contato)
    #     else:
    #         print(f"WARNING(receptivo:resposta_cadastro)-{id_contato}: error GPT")

    resposta_gpt, custo_mensagem, error = get_resposta_gpt(conversa)
    resposta_bot = resposta_gpt

    DatabaseConnection.dispose_engine()
    if error == False:
        current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo'))
        formatted_datetime_flag = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        conversa.set_flag("RESPONDER_REPESCAGEM", formatted_datetime_flag)
        print(f"ERROR(lambda_handler)-{id_contato}: Erro no GPT")
        return {'statusCode': 400}

    if conversa.get_flag() != int(os.environ['ON_HOLD']):
        current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo'))
        formatted_datetime_msg = current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
        formatted_datetime_flag = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

        data_resposta_gpt = {
                    'ID_AUTOR': [0],
                    'CONTEUDO': [resposta_gpt],
                    'CUSTO': [custo_mensagem],
                    'ID_CAMPANHA': [conversa.campanha],
                    "DATETIME_CONVERSA": [conversa.datetime_conversa],
                    "MENSAGEM_DATETIME": [formatted_datetime_msg],
                    "ID_CONTATO": [contato.id_contato]
        }
        insert_data_into_db(pd.DataFrame(data_resposta_gpt), "OMNI_MENSAGEM" )         

        id_conversa = [conversa.id_conversa[0], conversa.id_conversa[1], conversa.id_conversa[2]]
        ret_enc, resp_enc = logica_Encerramento(id_conversa)

        if ret_enc:
            if resp_enc and resp_enc['encerramento_ret']:
                deletar_mensagem_anterior(conversa, formatted_datetime_msg)
                resposta_bot = resp_enc['encerramento_msg']
                current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo'))
                formatted_datetime_resp_aut = current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
                formatted_datetime_flag = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
                data_resposta_gpt = {
                            'ID_AUTOR': [0],
                            'CONTEUDO': [resposta_bot],
                            'CUSTO': [custo_mensagem],
                            'ID_CAMPANHA': [conversa.campanha],
                            "DATETIME_CONVERSA": [conversa.datetime_conversa],
                            "MENSAGEM_DATETIME": [formatted_datetime_resp_aut],
                            "ID_CONTATO": [contato.id_contato]
                }
                insert_data_into_db(pd.DataFrame(data_resposta_gpt), "MENSAGEM" )    

            ret_env, resp_env = enviar_resposta(contato, conversa, formatted_datetime, resposta_bot)
            if ret_env is False:
                current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo'))
                formatted_datetime_flag = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
                conversa.set_flag("RESPONDER_REPESCAGEM", formatted_datetime_flag)
                deletar_mensagem_anterior(conversa, formatted_datetime_msg)
            else:
                current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo'))
                formatted_datetime_mensagem_atualizada = current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
                atualiza_horario_mensagem(conversa, formatted_datetime_msg, formatted_datetime_mensagem_atualizada)
                formatted_datetime_flag = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
                conversa.set_flag("AGUARDAR", formatted_datetime_flag)
        else:
            current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo'))
            formatted_datetime_flag = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
            conversa.set_flag("RESPONDER_REPESCAGEM", formatted_datetime_flag)
            deletar_mensagem_anterior(conversa, formatted_datetime_msg)
            return {'statusCode': 400}

    DatabaseConnection.dispose_engine()

    return {'resposta':resposta_gpt, 'statusCode': 200}
    
def logica_Encerramento(id_conversa):
    campanha = id_conversa[1]
    ret = True
    resp = None
    
    #################### ENCERRAMENTO ##################

    if osFunc('Ambiente_Lambda'):
        input_logicaEncerramento = {'id_conversa': id_conversa, 'tarefa': 'ENCERRAMENTO'}
        if 'producao' in os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            resposta_logicaEncerramento = invokeFunction(input_logicaEncerramento, "arn:aws:lambda:sa-east-1:191246610649:function:producao_logicaEncerramento", type='RequestResponse')
        else:
            resposta_logicaEncerramento = invokeFunction(input_logicaEncerramento, "arn:aws:lambda:sa-east-1:191246610649:function:homolog_logicaEncerramento", type='RequestResponse')
    else:
        resposta_logicaEncerramento = logica_encerramento(input_logicaEncerramento, {})
    
    ret = resposta_logicaEncerramento['success']
    resp = resposta_logicaEncerramento
    #################### INTERVENCAO ##################
    input_intervencao= {'id_conversa': id_conversa, 'tarefa': 'INTERVENCAO'}

    if osFunc('Ambiente_Lambda'):
        if 'producao' in os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            invokeFunction(input_intervencao, "arn:aws:lambda:sa-east-1:191246610649:function:producao_logicaEncerramento", type='Event')
        else:
            invokeFunction(input_intervencao, "arn:aws:lambda:sa-east-1:191246610649:function:homolog_logicaEncerramento", type='Event')
    else:
        logica_encerramento(input_intervencao, {})
        

    
    return ret, resp

def enviar_resposta(contato, conversa, formatted_datetime, resposta_gpt):
    if osFunc('Ambiente_Lambda'): 
        response = enviar_dm(contato.id_contato, resposta_gpt, header = conversa.wpp_header)
        if response == 'False':
            print(f"ERROR(lambda_handler)-{contato.id_contato}: Erro na Dialog360")
            return False, {'statusCode': 400}
        elif response == "No authorization provided":
            print(f"ERRO(lambda_function.lambda_handler) - {contato.id_contato}: Erro na dialog")
            conversa.set_flag("FINALIZADO", formatted_datetime)
            return False, {'statusCode': 400}
        else:
            return True, {'statusCode': 200}
    else:
        return True, {'statusCode': 200}
        
def get_resposta_gpt(conversa):
    
    inputGeraResposta = {
                        'id_conversa' : [conversa.id_conversa[0], conversa.id_conversa[1], conversa.id_conversa[2]],
                        'tarefa': 'ATENDENTE'
                        }
                        
    responseGeraResposta = call_geraResposta(inputGeraResposta)
        
    resposta_gpt = responseGeraResposta['resposta']
    custo_mensagem = responseGeraResposta['custo']
    error = responseGeraResposta['error']
    return resposta_gpt, custo_mensagem, error

def call_geraResposta(inputGeraResposta):
    if osFunc('Ambiente_Lambda'):  
        if 'producao' in os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            resultado = invokeFunction(inputGeraResposta, "arn:aws:lambda:sa-east-1:191246610649:function:producao_geraResposta")
        else:
            resultado = invokeFunction(inputGeraResposta, "arn:aws:lambda:sa-east-1:191246610649:function:homolog_geraResposta")
    else: 
        resultado = gera_resposta_bot(inputGeraResposta,{})
    return resultado

def deletar_mensagem_anterior(conversa, datetime_msg):
    delete_query = f"""DELETE FROM OMNI_MENSAGEM 
    WHERE ID_CONTATO = {conversa.contato.id_contato} 
    AND ID_CAMPANHA = {conversa.campanha}
    AND DATETIME_CONVERSA = '{conversa.datetime_conversa}'
    AND MENSAGEM_DATETIME = '{datetime_msg}'
    LIMIT 1"""
    execute_query_db(delete_query)
    
def atualiza_horario_mensagem(conversa, formatted_datetime_msg, formatted_datetime_mensagem_atualizada):
    query = f"""UPDATE `OMNI_MENSAGEM` SET `MENSAGEM_DATETIME` = '{formatted_datetime_mensagem_atualizada}'
    WHERE (`ID_CONTATO` = {conversa.contato.id_contato}) 
    and (`ID_CAMPANHA` = '{conversa.campanha}') 
    and (`DATETIME_CONVERSA` = '{conversa.datetime_conversa}') 
    and (`MENSAGEM_DATETIME` = '{formatted_datetime_msg}');"""
    execute_query_db(query)