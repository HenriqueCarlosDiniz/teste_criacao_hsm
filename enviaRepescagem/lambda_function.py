from utils.config import *

import boto3
import json
import os
import pytz
import time
import traceback
from datetime import datetime

from utils.send_wpp import enviar_wpp, enviar_payload
from utils.connect_db import  query_to_dataframe
from utils.api_communication import POST_buscar_telefone, POST_cancelar_agendamento, POST_confirmar_agendamento, registra_agendamentoAPI, verificar_confirmacao 
from utils.locations import agendamento_passado
from Models.conversa import Conversa
from Models.contato import Contato

if osFunc('Ambiente_Lambda'): 
    from invoke_lambda import invokeFunction
else: 
    from geraResposta.lambda_function import lambda_handler as geraResposta
    from logicaEncerramento.lambda_function import lambda_handler as logicaEncerramento

def lambda_handler(event, context):
    start_time = time.time()
    print("INFO(lambda_function.lambda_handler): EVENT ", event)
    if 'body' in event:
        # Acessa as informações dentro de 'body'
        batch_id_contato = event['body']['id_contato']
        batch_id_campanha = event['body']['id_campanha']
        batch_datetime_conversa = event['body']['datetime_conversa']
    else:
        # Acessa as informações diretamente do evento
        batch_id_contato = event['id_contato']
        batch_id_campanha = event['id_campanha']
        batch_datetime_conversa = event['datetime_conversa']
    tempo_maximo = int(os.environ['TEMPO_MAXIMO_REPESCAGEM']) # 570s 60 * 9.5
    
    t = time.time()
    df_repescagem, df_rep_msg = get_tabelas_repescagem_rep_msg()
    print("Tempo de baixar tabelas REPESCAGEM e REP_MSG:", round(time.time() - t, 3))
    
    for i in range(len(batch_id_contato)):
        t = time.time()
        if (t-start_time)> tempo_maximo: 
            print(f"WARNING(lambda_function.lambda_handler): Tempo máximo da repescagem atingido = {tempo_maximo} s")
            break
        id_contato = batch_id_contato[i]
        id_campanha = batch_id_campanha[i]
        datetime_conversa = batch_datetime_conversa[i]
        
        contato = Contato(id_contato)
        conversa = Conversa(contato, id_campanha, datetime_conversa)
        print("Tempo de gerar contato e conversa:", round(time.time() - t, 3))
        
        t = time.time()
        repescagem(conversa, df_repescagem, df_rep_msg)
        print("Tempo de repescagem:", round(time.time() - t, 3))
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

def repescagem(conversa, df_repescagem, df_rep_msg): 
    """
    Avalia a situação de uma conversa e toma ações específicas com base em certas condições.

    A função utiliza informações como a flag da conversa, o resultado, o número de mensagens e o horário da última mensagem.

    :param conversa: Objeto que representa uma conversa.
    :type conversa: Conversa (classe ou tipo de objeto representando uma conversa)

    :return: Nenhum valor de retorno explícito.
    :rtype: None

    :raises: Exception se ocorrer um erro durante a repescagem.
    """
    flag = conversa.get_flag()
    print(f"INFO(lambda_function.repescagem)-{conversa.contato.id_contato}: Iniciando repescagem. Flag = {flag}, Campanha = {conversa.campanha}")

    try: #horario_ultima_msg já filtrada por flag na query. num_mensagem certos filtrados na query. resultados certos filtrados na query
        df_repescagem_filtrado = df_repescagem[(df_repescagem['FLAG'] == flag) & (df_repescagem['ID_CAMPANHA'] == conversa.campanha)]
        if df_repescagem_filtrado is not None and not df_repescagem_filtrado.empty:
            hash_result = int(df_repescagem_filtrado['HASH_RESULT'].iloc[0])
            print(f'hash result: {hash_result}')

            if hash_result not in [0, 6, 7]: #Se não for RESPONDER nem AGUARDAR_NEG
                df_rep_msg_filtrado = df_rep_msg[df_rep_msg['HASH_REPESCAGEM'] == hash_result]
                msg = df_rep_msg_filtrado['MENSAGEM'].iloc[0]
                payload = df_rep_msg_filtrado['PAYLOAD'].iloc[0]
                data_agendamento = datetime.strptime(conversa.contato.agendamento.data_agendamento, "%Y-%m-%d").strftime("%d/%m")
                horario_agendamento = f"{conversa.contato.agendamento.horario_agendamento}"
                unidade_agendamento = f"Unidade {conversa.contato.agendamento.unidade_agendamento.nome}"

                msg = msg.replace('{cliente}', str("\""+conversa.contato.nome.split()[0]+"\"")) \
                    .replace('{telefone}', str("\""+conversa.contato.id_contato+"\"")) \
                    .replace('{data_agendamento}', str("\""+data_agendamento+"\"")) \
                    .replace('{horario_agendamento}', str("\""+horario_agendamento+"\"")) \
                    .replace('{unidade_agendamento}', str("\""+unidade_agendamento+"\"")) \
                    .replace('{nome_cliente}', str(conversa.contato.nome.split()[0])) \
                    .replace('{nome_data_agendamento}', str(data_agendamento)) \
                    .replace('{nome_horario_agendamento}', str(horario_agendamento)) \
                    .replace('{nome_unidade_agendamento}', str(unidade_agendamento))
                if flag == osFunc("LEMBRETE_1H"):
                    link = conversa.get_link_forms()
                    msg = msg.replace('{link_forms}', f"https://psdsaopaulo.net.br/psd2/app/questionario/responder-questionario/{str(link)}")
                
                if payload: 
                    payload = payload.replace('{cliente}', str("\""+conversa.contato.nome.split()[0]+"\"")) \
                    .replace('{telefone}', str("\""+conversa.contato.id_contato+"\"")) \
                    .replace('{data_agendamento}', str("\""+data_agendamento+"\"")) \
                    .replace('{horario_agendamento}', str("\""+horario_agendamento+"\"")) \
                    .replace('{unidade_agendamento}', str("\""+unidade_agendamento+"\"")) \
                    .replace('{nome_cliente}', str(conversa.contato.nome.split()[0])) \
                    .replace('{nome_data_agendamento}', str(data_agendamento)) \
                    .replace('{nome_horario_agendamento}', str(horario_agendamento)) \
                    .replace('{nome_unidade_agendamento}', str(unidade_agendamento))
                    
                    if flag == osFunc("LEMBRETE_1H"):
                        payload = payload.replace('{link_forms}', f"\"https://psdsaopaulo.net.br/psd2/app/questionario/responder-questionario/{str(link)}\"") 

            flag, agendamento = conversa.get_flag_e_agendamento()
            horario_ultima_msg_repescagem, _ = conversa.get_horario_ultima_msg_repescagem()
            print('ultima msg:',horario_ultima_msg_repescagem)
            if hash_result == 14:
                    # msg = msg.format(nome_cliente = conversa.contato.nome.split()[0])
                    print("Lembrete 1H") 
                    direto_lembrete_1h(conversa, msg, payload)
            elif hash_result == 0:
                formatted_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S")
                conversa.set_flag("FINALIZADO", formatted_datetime)
                
            elif hash_result == 6: #RESPONDER
                    enviar_para_sqs(conversa)
                    
            elif horario_ultima_msg_repescagem > osFunc('REPESCAGEM_CONDICAO_TEMPO1'): #Passou mais de 10 minutos desde a última mensagem
                if hash_result == 3: #FINALIZAR
                    finalizar(conversa, payload)
                elif hash_result == 7: #AGUARDAR_NEG (INSUCESSO)
                    HSM_negativo(conversa)
                elif hash_result in [4,9,12,15]:
                    if horario_ultima_msg_repescagem > osFunc('REPESCAGEM_CONDICAO_TEMPO2'): #REPESCAGEM1
                        if agendamento:
                            finalizar(conversa, payload)
                        else:
                            arr_msg = [msg, df_rep_msg_filtrado['MENSAGEM'].iloc[1]]
                            verificar_desfecho(conversa, arr_msg, payload, flag)
                    else:
                        if not agendamento:
                            arr_msg = [msg, df_rep_msg_filtrado['MENSAGEM'].iloc[1]]
                            verificar_desfecho(conversa, arr_msg, payload, flag)

                elif horario_ultima_msg_repescagem > osFunc('REPESCAGEM_CONDICAO_TEMPO2'): #Passou mais de 60 minutos desde a última mensagem
                    if hash_result in [5,16]: #REPESCAGEM2 
                        msg = msg.format(nome_cliente = conversa.contato.nome.split()[0])
                        arr_msg = [msg, df_rep_msg_filtrado['MENSAGEM'].iloc[1]]
                        verificar_desfecho(conversa, arr_msg, payload, flag)
                        
                        
                    elif horario_ultima_msg_repescagem > osFunc('REPESCAGEM_CONDICAO_TEMPO3'):
                        if hash_result == 13: 
                            finalizar(conversa, payload)
                    
            
                        

    except Exception as e:
        print(f"ERROR(lambda_function.repescagem)-{conversa.contato.id_contato}: Erro na repescagem {traceback.format_exc()}")


def finalizar(conversa, payload):
    msg = "Estou encerrando seu atendimento, mas se precisar de alguma coisa é só chamar!"
    # Finalizar conversa, enviando mensagem de encerramento
    formatted_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S")
    telefone = conversa.contato.id_contato
    payload = json.loads(payload)
    conversa.set_flag("FINALIZADO", formatted_datetime)
     
    response = enviar_wpp(telefone, msg, True, header= conversa.wpp_header)
    formatted_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S.%f")
    conversa.add_mensagem(msg, formatted_datetime)
    
    response_payload = enviar_payload(payload, header=conversa.wpp_header)
    formatted_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S.%f")
    conversa.add_mensagem("Por favor, avalie nosso atendimento até o momento. Selecione abaixo uma nota de 5 a 1 para nossa conversa.", formatted_datetime)


def verificar_desfecho(conversa, msg, payload, flag): 
    #Verificar status da conversa (repescagem) e mandar lembrete para cliente, ou salvar agendamento caso seja detectado.
    formatted_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S")
    telefone = conversa.contato.id_contato

    input_gera_resposta = {
            'id_conversa': [str(conversa.id_conversa[0]), str(conversa.id_conversa[1]), str(conversa.id_conversa[2])],
            'tarefa': 'CLASSIFICADOR_REPESCAGEM'
    }
    
    if osFunc('Ambiente_Lambda'):  
        if 'producao' in os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            resultado_repescagem = invokeFunction(input_gera_resposta, "arn:aws:lambda:sa-east-1:191246610649:function:producao_geraResposta")
        else:
            resultado_repescagem = invokeFunction(input_gera_resposta, "arn:aws:lambda:sa-east-1:191246610649:function:homolog_geraResposta")
    else: 
        resultado_repescagem = geraResposta(input_gera_resposta, {})
    if resultado_repescagem['error'] == False:
        print(f"ERROR(lambda_function.lambda_handler)-{conversa.contato.id_contato}: ERRO NO GPT - resultado_intervencao = {resultado_repescagem}")
        return 400
    print(f'INFO(lambda_function:verificar_desfecho)-{conversa.contato.id_contato}: resultado_repescagem = {resultado_repescagem}')

    # 1 - 'Interesse': Usuário está interessado em agendar uma nova avaliação, ou o agendamento já foi concluído.
    # 2 - 'Sem interesse': Usuário NÃO está interessado em agenda uma nova avaliação, ou o usuário pediu para o assistente parar de entrar em contato.
    # 3 - 'Contatar Futuramente': Usuário pediu explicitamente para entrarmos em contato futuramente, ou o usuário falou que vai entrar em contato depois.
    # 4 - 'Indefinido': Caso não se enquadre em nenhuma das outras categorias.

    if resultado_repescagem['resposta'] == "Interesse":
        input_logicaEncerramento = {
            'id_conversa': [str(conversa.id_conversa[0]), str(conversa.id_conversa[1]), str(conversa.id_conversa[2])],
            'tarefa': 'REPESCAGEM'
        }

        if osFunc('Ambiente_Lambda'):
            if 'producao' in os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
                resultado_logicaEncerramento = invokeFunction(input_logicaEncerramento, "arn:aws:lambda:sa-east-1:191246610649:function:producao_logicaEncerramento")
            else:
                resultado_logicaEncerramento = invokeFunction(input_logicaEncerramento, "arn:aws:lambda:sa-east-1:191246610649:function:homolog_logicaEncerramento")
        else: 
            resultado_logicaEncerramento = logicaEncerramento(input_logicaEncerramento, {})
        
        if resultado_logicaEncerramento['success'] == False:
                    print(f"ERROR(lambda_function.lambda_handler)-{conversa.contato.id_contato}: ERRO NO GPT - resultado_logicaEncerramento = {resultado_logicaEncerramento}")
                    return 400
        print(f'INFO(lambda_function:verificar_desfecho)-{conversa.contato.id_contato}: resultado_logicaEncerramento = {resultado_logicaEncerramento}')

        if resultado_logicaEncerramento['deteccao_agendamento'] == False:
            
            if (flag == osFunc("AGUARDAR_POS") or flag == osFunc("AGUARDAR")) and flag != osFunc("REPESCAGEM"):
                conversa.set_flag("REPESCAGEM", formatted_datetime)
                
            elif flag==osFunc("REPESCAGEM"): # flag == osFunc("REPESCAGEM"):
                conversa.set_flag("REPESCAGEM2", formatted_datetime)
                
            elif flag == osFunc("REPESCAGEM2"):
                conversa.set_flag("FINALIZAR", formatted_datetime)
            conversa.add_mensagem(msg[0], formatted_datetime)
            response = enviar_wpp(telefone, msg[0], False, header= conversa.wpp_header)
        elif resultado_logicaEncerramento['deteccao_agendamento'] == True:
            if resultado_logicaEncerramento['encerramento_ret']:
                resposta_bot = resultado_logicaEncerramento['encerramento_msg']
                ret_env, resp_env = enviar_resposta(conversa.contato, conversa, formatted_datetime, resposta_bot)


    elif resultado_repescagem['resposta'] == "Indefinido":
        if (flag == osFunc("AGUARDAR_POS") or flag == osFunc("AGUARDAR")) and flag != osFunc("REPESCAGEM"):
            conversa.set_flag("REPESCAGEM", formatted_datetime)
            
        elif flag == osFunc("REPESCAGEM"):
            conversa.set_flag("REPESCAGEM2", formatted_datetime)
            
        elif flag == osFunc("REPESCAGEM2"):
            conversa.set_flag("FINALIZAR", formatted_datetime)
            
        conversa.add_mensagem(msg[0], formatted_datetime)
        response = enviar_wpp(telefone, msg[0], False, header= conversa.wpp_header)
            
    elif resultado_repescagem['resposta'] == "Sem interesse":
        try:
            resultado_api = cancela_agendamento(telefone, id_campanha=conversa.campanha, banco_dados=conversa.banco_dados)
        except Exception as e:
            print(f"ERROR(lambda_function.repescagem)-{conversa.contato.id_contato}: Erro ao cancelar agendamento {traceback.format_exc()}")
            return False
        conversa.set_flag("FINALIZADO", formatted_datetime)
        conversa.set_resultado("INSUCESSO")
        conversa.add_mensagem(msg[1], formatted_datetime)
        response = enviar_wpp(telefone, msg[1], False, header= conversa.wpp_header)
        
        payload = json.loads(payload)
        response_payload = enviar_payload(payload, header=conversa.wpp_header)
        formatted_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S.%f")
        conversa.add_mensagem("Por favor, avalie nosso atendimento até o momento. Selecione abaixo uma nota de 5 a 1 para nossa conversa.", formatted_datetime)


    elif resultado_repescagem['resposta'] == "Contatar Futuramente":
        try:
            resultado_api = cancela_agendamento(telefone, id_campanha=conversa.campanha, banco_dados=conversa.banco_dados)
        except Exception as e:
            print(f"ERROR(lambda_function.repescagem)-{conversa.contato.id_contato}: Erro ao cancelar agendamento {traceback.format_exc()}")
            return False
        conversa.set_flag("FINALIZADO", formatted_datetime)
        conversa.set_resultado("CONTATAR_DEPOIS")
        
        conversa.add_mensagem(msg[1], formatted_datetime)
        response = enviar_wpp(telefone, msg[1], False, header= conversa.wpp_header)
        
        payload = json.loads(payload)
        response_payload = enviar_payload(payload, header=conversa.wpp_header)
        formatted_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S.%f")
        conversa.add_mensagem("Por favor, avalie nosso atendimento até o momento. Selecione abaixo uma nota de 5 a 1 para nossa conversa.", formatted_datetime)
        
    elif resultado_repescagem['resposta'] == "Confirmou Agendamento":
        conversa.set_flag("FINALIZADO", formatted_datetime)
        agendamento = conversa.contato.agendamento
        if not agendamento_passado(agendamento.data_agendamento, agendamento.horario_agendamento, agendamento.unidade_agendamento.sigla):
            resultado_api = confirma_agendamento(conversa.contato.id_contato, id_campanha = conversa.campanha, banco_dados=conversa.banco_dados)
            if resultado_api:
                conversa.set_resultado("SUCESSO")
            else:
                #tentar agendar
                datetime_agendamento = [f" {agendamento.data_agendamento} + ' ' + {agendamento.horario_agendamento}"]
                resposta_agendamento, _, _ = registra_agendamentoAPI(conversa, agendamento.unidade_agendamento, [conversa.contato.nome], datetime_agendamento)
                if resposta_agendamento:
                    resultado_api = confirma_agendamento(conversa.contato.id_contato, id_campanha = conversa.campanha, banco_dados=conversa.banco_dados)
                    if resultado_api:
                        conversa.set_resultado("SUCESSO")
                    else:
                        conversa.set_resultado("FALHA_AGENDAMENTO")
                else:
                    #não conseguiu agendar no horário passado (está cheio)
                    #precisa avisar a pessoa
                    resp_change = "Desculpe, parece que o horário da sua avaliação já passou ou não está mais disponível. Lembre-se de que oferecemos 15 minutos de tolerância para atrasos. Seria necessário reagendar para outro momento. Gostaria de verificar a disponibilidade para outra data ou horário?"
                    response = enviar_wpp(telefone, resp_change, False, header=conversa.wpp_header)
                    conversa.set_resultado("FALHA_AGENDAMENTO")

def enviar_para_sqs(conversa):
    if osFunc('Ambiente_Lambda'): 
        formatted_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%Y-%m-%d %H:%M:%S.%f')
        # Configura o cliente do SQS
        sqs_client = boto3.client('sqs')
        # URL da fila SQS
        if 'producao' in os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            queue_url = osFunc('urlSQS_mensagem_producao')
        else:
            queue_url = osFunc('urlSQS_mensagem_homolog')
        # Envia a mensagem para a fila SQS
        sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({'id_conversa': [str(conversa.id_conversa[0]), str(conversa.id_conversa[1]), str(conversa.id_conversa[2])], 'timestamp': str(formatted_datetime)}), 
            MessageGroupId='envio_direto')
        
def HSM_negativo(conversa):
    conversa.set_resultado('INSUCESSO')

def confirma_agendamento(telefone, id_campanha = 0, banco_dados = 0):
    try:
        resposta = json.loads(POST_buscar_telefone(telefone, id_campanha=id_campanha, banco_dados=banco_dados).text)["resposta"]
        agendado = resposta["sucesso"] == True

        if agendado:
            token_confirmacao = resposta["token_confirmacao"]
            horario_agendamento = resposta["atendimento_em"][11:16]
            data_agendamento = resposta["atendimento_em"][0:10]
            POST_confirmar_agendamento(token_confirmacao, id_campanha=id_campanha, banco_dados=banco_dados)
            return True
        return False #naõ tem agendamento pra confirmar
    except Exception as e:
        print(f"ERROR(lambda_function.confirma_agendamento)-{telefone}: Erro ao confirmar agendamento {traceback.format_exc()}")
        return False
        
def cancela_agendamento(telefone, id_campanha = 0, banco_dados = 0):
    resposta = json.loads(POST_buscar_telefone(telefone, id_campanha=id_campanha, banco_dados=banco_dados).text)["resposta"]
    agendado = resposta["sucesso"] == True

    if agendado:
        token_cancelamento = resposta["token_cancelamento"]
        horario_agendamento = resposta["atendimento_em"][11:16]
        data_agendamento = resposta["atendimento_em"][0:10]
        POST_cancelar_agendamento(token_cancelamento, id_campanha=id_campanha, banco_dados=banco_dados)
    return True

def get_tabelas_repescagem_rep_msg():
    query_repescagem = "SELECT * FROM REPESCAGEM"
    df_repescagem = query_to_dataframe(query_repescagem)

    query_rep_msg = "SELECT * FROM REP_MSG"
    df_rep_msg = query_to_dataframe(query_rep_msg)

    return df_repescagem, df_rep_msg
    
def direto_lembrete_1h(conversa, msg, payload):
    formatted_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S")
    
    telefone = conversa.contato.id_contato

    resp = verificar_confirmacao(telefone, conversa.campanha)
    print(resp) 
    if not resp['confirmado'] and resp['agendado']:
        # response = enviar_wpp(telefone, msg, True, header= conversa.wpp_header)
        print('payload_str:', payload)
        payload = json.loads(payload)   
        print('payload_json:', payload)
        response_payload = enviar_payload(payload, header=conversa.wpp_header)
        print(f"INFO(lambda_function.direto_lembrete_1h)-{conversa.contato.id_contato}: Whatsapp Response: {response_payload}")
        formatted_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S.%f")
        conversa.add_mensagem(msg, formatted_datetime)
        formatted_datetime_flag = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S")
        conversa.set_flag("FINALIZADO", formatted_datetime_flag)
        conversa.set_resultado("SEM_INTERACAO")
    else:
        formatted_datetime_flag = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S.%f")
        conversa.set_flag("FINALIZADO", formatted_datetime_flag)  
        

def enviar_resposta(contato, conversa, formatted_datetime, resposta_gpt):
    if osFunc('Ambiente_Lambda'): 
        response = enviar_wpp(contato.telefone, resposta_gpt, header = conversa.wpp_header)
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
