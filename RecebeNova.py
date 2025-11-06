from utils.config import * 

import boto3
import json
import pandas as pd
import pytz 
import requests
from datetime import datetime, timedelta

from utils.send_wpp import enviar_wpp, enviar_HSM, enviar_payload
from utils.api_communication import verificar_confirmacao
from Models.agente import *

if osFunc('Ambiente_Lambda'):
    from message import extract_message, gerar_variaveis_mensagem_automatica
    from receptivo import cadastrar_receptivo 
    from utils_reset import reset_conversa, get_fake_number, get_true_number, check_reset_presence_and_extract_value
else:
    from recebeMensagem.message import extract_message, gerar_variaveis_mensagem_automatica
    from recebeMensagem.receptivo import cadastrar_receptivo
    from processaMensagem.lambda_function import lambda_handler as processa_mensagem
    from recebeMensagem.utils_reset import reset_conversa, get_fake_number, get_true_number, check_reset_presence_and_extract_value


from utils.connect_db import query_to_dataframe, insert_data_into_db, execute_query_db
from utils.text import formatar_data

from utils.text import remover_acentos


def lambda_handler(event, context):
    id_campanha = osFunc("RECEPTIVO") # Inicializando a variavel com 10 
    
    if osFunc('Ambiente_Lambda'):
        record =  event['Records'][0]
        if isinstance(record['body'], dict):
            body_json = record['body']
        else:
            body_json = json.loads(record['body'])

        # print("INFO(lambda_function.lambda_handler): EVENT ", body_json)
        
        if isinstance(body_json['post'], dict):
            timestamp = body_json['timestamp']
            body_json = body_json['post']
        else:
            timestamp = body_json['timestamp'] 
            body_json = json.loads(body_json['post'])
    else:
        body_json = event['body']['post']
        timestamp = event['body']['timestamp']
        
    http_session = requests.Session()
    current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')) 
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    formatted_datetime_msg = timestamp
    
    #########################################################
    ################ APAGA MSG NA FILA ######################
    #########################################################

    if osFunc('Ambiente_Lambda'):
        sqs_client = boto3.client('sqs')
        
        receipt_handle = record['receiptHandle']
        if 'producao' in os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            queue_url = osFunc('urlSQS_post_producao') 
        else:
            queue_url = osFunc('urlSQS_post_homolog') 
        try:
            sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        except Exception as e:
            print(f"ERROR(lambda_function.lambda_handler): Erro no delete do sqs {e}")
 
    ############################################################################
    # EXTRAIR ID CONTATO E PAGE ID
    ############################################################################
    print("INFO(lambda_function.lambda_handler): EVENT ",record['body'])

    if 'sender' in body_json.keys():
        if 'id' in body_json['sender'].keys():
            id_contato = body_json['sender']['id']
        elif 'user_ref' in body_json['sender'].keys():
            id_contato = body_json['sender']['user_ref']
    else:
        print(f"ERROR: formato do payload inválido:{body_json}")

    if 'recipient' in body_json.keys():
        if 'id' in body_json['recipient'].keys():
            id_pagina = body_json['recipient']['id']
    else:
        print(f"ERROR: formato do payload inválido:{body_json}")

    print(f"INFO(lambda_function.lambda_handler) - {id_contato}: Antes de resgatar informações da conversa")
    
    
    ############################################################################
    #RESAGATE DAS INFORMAÇÕES DE CLIENTE (CONVERSA, NOME, AGENDAMENTO)
    ############################################################################

    conversa = query_to_dataframe(f"""SELECT 
                                    OMNI_CONVERSA.*,
                                    OMNI_CONTATO.NOME_CLIENTE,
                                    OMNI_CONTATO.GENERO,
                                    OMNI_CONTATO.TELEFONE,
                                    CAMP_AGENDAMENTO.UNIDADE_PSD,
                                    CAMP_AGENDAMENTO.DATA,
                                    CAMP_AGENDAMENTO.HORARIO,
                                    CAMPANHA.COMPLETAR_CADASTRO,
                                    CAMPANHA.AGENDAMENTO_CADASTRADO
                                FROM
                                    OMNI_CONVERSA
                                        LEFT JOIN
                                    OMNI_CONTATO ON OMNI_CONVERSA.ID_CONTATO = OMNI_CONTATO.ID_CONTATO
                                        LEFT JOIN
                                    CAMP_AGENDAMENTO ON OMNI_CONTATO.TELEFONE = CAMP_AGENDAMENTO.TELEFONE
                                        LEFT JOIN
                                    CAMPANHA ON OMNI_CONVERSA.ID_CAMPANHA = CAMPANHA.ID_CAMPANHA
                                WHERE
                                    OMNI_CONVERSA.ID_CONTATO = {id_contato}
                                    AND OMNI_CONVERSA.ID_RESULTADO != {osFunc("ADICIONADO")}
                                    AND OMNI_CONVERSA.DATETIME_CONVERSA >= DATE_SUB(CURDATE(), INTERVAL 14 DAY)
                                ORDER BY OMNI_CONVERSA.DATETIME_CONVERSA DESC
                                LIMIT 1;  
                                    """)
                                    
    df_token = query_to_dataframe(f"SELECT TOKEN FROM OMNI_TOKEN WHERE ID_PAGINA = {id_pagina}")
    meta_token = df_token["TOKEN"].iloc[0]
    HEADER = {'Content-Type': "application/json", 'Token': meta_token}

    ############################################################################
    #TRATANDO CONVERSAS RECEPTIVAS
    ############################################################################
    
    print(f"INFO(lambda_function.lambda_handler) - {id_contato}: Depois de resgatar informações da conversa, {conversa.empty}")
    
    # CASO NÃO EXISTA UMA CONVERSA PARA O CONTATO OU A CONVERSA SEJA ANTIGA, CRIA-SE UMA NOVA CONVERSA AQUI                    
    if conversa.empty or pytz.timezone('America/Sao_Paulo').localize(conversa['DATETIME_CONVERSA'][0]) < datetime.now(pytz.timezone('America/Sao_Paulo')) - timedelta(days=osFunc('ANTIGUIDADE_MAXIMA')):
        print(f"INFO(lambda_function.lambda_handler)-{id_contato}: Começando cadastro receptivo")
        response2Client = cadastrar_receptivo(id_contato, body_json, id_pagina, header = HEADER)
        print(f"INFO(lambda_function.lambda_handler)-{id_contato}: Cliente receptivo cadastrado")
        if osFunc('Ambiente_Lambda'):
            return {'statusCode': 200} 
        else:
            return {'resposta':response2Client['content']}
        
    else: 
        # Acessando horas e minutos
        if conversa['AGENDAMENTO_CADASTRADO'][0]:
            conversa['HORARIO'] = conversa['HORARIO'].apply(lambda x: f"{x.components.hours:02d}:{x.components.minutes:02d}")
        datetime_conversa = conversa['DATETIME_CONVERSA'][0]
        id_campanha = conversa['ID_CAMPANHA'][0]
        if conversa['TELEFONE'][0]:
            telefone = conversa['TELEFONE'][0]
    
    print(f"INFO(lambda_function.lambda_handler) - {id_contato}: Antes de extrair mensagem")
     
    ############################################################################
    #EXTRAIR MENSAGEM ENVIADO PELO CLIENTE DAS DIFERENTES MIDIAS
    ############################################################################
    response2Client = {'type': None, 'content': None}
    message_text, id_mensagem, response2Client, id_flag, id_resultado, id_campanha = extract_message(body_json, conversa, current_datetime, http_session)
    conversa.loc[0, 'ID_CAMPANHA'] = id_campanha
    ############################################################################
    # RESET DA CONVERSA
    ############################################################################
    print(f"INFO(lambda_function.lambda_handler) - {id_contato}: Mensagem recebida = {message_text}")
    
    # reset_bool, nova_campanha_list  = check_reset_presence_and_extract_value(message_text)
    # if reset_bool:
    #     id_campanha = conversa['ID_CAMPANHA'][0]
    #     if len(nova_campanha_list) == 0:
    #         nova_campanha = None
    #     else:
    #         nova_campanha = nova_campanha_list[0]
    #     response2Client = reset_conversa(telefone, id_campanha, telefone_wpp, header = HEADER, novo_id_campanha = nova_campanha, banco_dados=conversa["BANCO_DADOS"][0])
    #     print(f"INFO(lambda_function.lambda_handler)-{telefone}: Conversa Resetada")
    #     if osFunc('Ambiente_Lambda'):
    #         return {'statusCode': 200} 
    #     else:
    #         return {'resposta':response2Client['content']}

    ############################################################################
    # UPLOAD DO RESULTADO
    ############################################################################
    
    #Mudar resultado se for a primeira resposta do cliente (resultado anterior SEM_INTERACAO) 
    if response2Client['type'] == None and conversa['ID_RESULTADO'][0] == osFunc('SEM_INTERACAO'):
        execute_query_db(f"UPDATE OMNI_CONVERSA SET ID_RESULTADO = '{id_resultado}' WHERE ID_CONTATO = '{id_contato}' AND DATETIME_CONVERSA = '{datetime_conversa}' LIMIT 1;")
    #Mudar resultado em qualquer mensagem da pessoa
    elif conversa['ID_RESULTADO'][0] not in [osFunc('SUCESSO'), osFunc('FALHA_AGENDAMENTO'), osFunc('INTERVENCAO'), osFunc('TELEMARKETING'), osFunc('SUCESSO_REMARKETING')]: #Se o resultado anterior era SEM_INTERACAO, ou seja, trata-se da primeira mensagem do usuário
        if telefone:
            verificacao_confirmacao = verificar_confirmacao(telefone, id_campanha = id_campanha)
            if id_campanha in [osFunc("FACEBOOK"), osFunc("INSTAGRAM")] and verificacao_confirmacao['agendado']: #Cliente já possui agendamento no futuro
                print(f"INFO(lambda_function.lambda_handler) - {id_contato}: Cliente já foi agendado pelo telemarketing")
                execute_query_db(f"UPDATE OMNI_CONVERSA SET ID_RESULTADO = {osFunc('TELEMARKETING')} WHERE ID_CONTATO = {id_contato} AND DATETIME_CONVERSA = '{datetime_conversa}';")
                response2Client['type'] = 'text'
                response2Client['content'] = f"Verifiquei aqui no sistema que você já possui uma avaliação gratuita marcada para {formatar_data((verificacao_confirmacao['data']))}, às {(verificacao_confirmacao['horario'])}. Se quiser reagendar ou tiver alguma dúvida, estou à disposição para ajudar!"
                id_flag = osFunc('AGUARDAR')
        else: #Cliente não possui agendamento
            execute_query_db(f"UPDATE OMNI_CONVERSA SET ID_RESULTADO = '{id_resultado}' WHERE ID_CONTATO = '{id_contato}' AND DATETIME_CONVERSA = '{datetime_conversa}' LIMIT 1;")
    
    #Mudar resultado se fazer_acao retornar SUCESSO ou FALHA_AGENDAMENTO
    elif id_resultado in [osFunc('SUCESSO'),osFunc('FALHA_AGENDAMENTO')]:
        execute_query_db(f"UPDATE OMNI_CONVERSA SET ID_RESULTADO = '{id_resultado}' WHERE ID_CONTATO = '{id_contato}' AND DATETIME_CONVERSA = '{datetime_conversa}' LIMIT 1;")
    
    print(f"INFO(lambda_function.lambda_handler) - {id_contato}: depois de upload resultado")

    ############################################################################
    # SALVAR A MENSAGEM RECEPTIVA NO DB
    ############################################################################

    if message_text != 'reset' and message_text != 'Reset':    
        inputMessage = {
                        'ID_CONTATO': [id_contato], 
                        'ID_CAMPANHA': [id_campanha],
                        'DATETIME_CONVERSA': [datetime_conversa],
                        'MENSAGEM_DATETIME': [formatted_datetime_msg],
                        'CONTEUDO': [message_text],
                        'CUSTO': [0],
                        'ID_AUTOR': [1]
        } 
        
        insert_data_into_db(pd.DataFrame(inputMessage), 'OMNI_MENSAGEM')   
    print(f"INFO(lambda_function.lambda_handler) - {id_contato}: Depois de salvar mensagem receptiva")
    ############################################################################
    # ON HOLD - TERMINA A FUNÇÃO
    ############################################################################ 
    if conversa['ID_LAST_FLAG'][0] == int(osFunc("ON_HOLD")) or id_flag == int(osFunc("ON_HOLD")):
        print(f"INFO(lambda_function.lambda_handler) - {id_contato}: Cliente ON-HOLD")
        return {'resposta':'ON_HOLD','statusCode': 200} 

    ############################################################################
    # UPLOAD DA FLAG
    ############################################################################

    execute_query_db(f'''UPDATE OMNI_CONVERSA SET 
                                ID_LAST_FLAG = {id_flag} 
                                WHERE 
                                DATETIME_CONVERSA = "{datetime_conversa}" AND
                                ID_CAMPANHA = {id_campanha} AND
                                ID_CONTATO = {id_contato} LIMIT 1;''') 
    
    # upload das flags
    inputMessage = {
                'ID_CONTATO': [id_contato], 
                'ID_CAMPANHA': [id_campanha],
                'DATETIME_CONVERSA': [datetime_conversa],
                'DATETIME_FLAG': [formatted_datetime],
                'ID_FLAG': [id_flag]
    } 
    insert_data_into_db(pd.DataFrame(inputMessage), 'OMNI_FLAG_HISTORICO')  
    
    print(f"INFO(lambda_function.lambda_handler) - {id_contato}: Depois de upload flag")
    
    
    ############################################################################
    # UPLOAD DO GÊNERO
    ############################################################################
    if conversa['GENERO'][0] is None or conversa['GENERO'][0] == '':
        nome = remover_acentos(conversa['NOME_CLIENTE'][0].split()[0].upper()) 
        df_nomes = query_to_dataframe(f"SELECT * FROM NOME_GENERO WHERE NOME = '{nome}'") 
        genero = df_nomes['GENERO'].values
        if not genero:
            # contexto = "Será passado um nome e sobrenome para você, quero que você classifique o gênero do nome: Masculino ou Feminino ou Indefinido. O output deve ser uma das seguintes opções exclusivamente: 'M' ou 'F' ou 'I'"
            # contato = Contato(telefone)
            # agente = Agent(contexto, model="gpt-3.5-turbo-1106")
            # resp = agente.get_response_simples([{"role": "system", "content": f"{contexto}"}, {"role": "user", "content": f"{nome}"}], conversa = Conversa(contato, conversa['ID_CAMPANHA'][0]))
            # genero = resp.choices[0].message.content
            # execute_query_db(f"INSERT INTO NOME_GENERO (NOME, GENERO) VALUES ('{nome}','{genero}')")
            genero = 'null'
        else: genero = genero[0]
        if genero != 'null':
            execute_query_db(f"UPDATE OMNI_CONTATO SET GENERO = '{genero}' WHERE ID_CONTATO = {id_contato} LIMIT 1")
    
    print(f"INFO(lambda_function.lambda_handler) - {id_contato}: Depois de upload genero")
    
    ############################################################################
    # SALVAR A MENSAGEM ENVIADA NO DB E ENCERRA A FUNÇÃO
    ############################################################################
    if response2Client['content'] is not None and not response2Client['content'] == '':
        formatted_datetime_msg = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S.%f")
        id_mensagem_response = 'None'
        if response2Client['type'] == 'text':
            if osFunc('Ambiente_Lambda'):
                # if int(telefone)<1000000:
                #     telefone = get_true_number(telefone)
                #     response = enviar_wpp(telefone, response2Client['content'], header = HEADER)
                #     telefone = get_fake_number(telefone)
                # else:
                response = enviar_dm(id_contato, response2Client['content'], header = HEADER)

                if response == "False":
                    print(f"ERRO(lambda_function.lambda_handler) - {id_contato}: Erro na API Meta")
                    id_flag = osFunc('RESPONDER')
    
                    execute_query_db(f'''UPDATE OMNI_CONVERSA SET 
                                    ID_LAST_FLAG = {id_flag} 
                                    WHERE 
                                    DATETIME_CONVERSA = "{datetime_conversa}" AND
                                    ID_CAMPANHA = {id_campanha} AND
                                    ID_CONTATO = {id_contato} LIMIT 1;''') 
        
                    inputMessage = {
                                'ID_CONTATO': [id_contato], 
                                'ID_CAMPANHA': [id_campanha],
                                'DATETIME_CONVERSA': [datetime_conversa],
                                'DATETIME_FLAG': [formatted_datetime],
                                'ID_FLAG': [id_flag]
                    } 
                    insert_data_into_db(pd.DataFrame(inputMessage), 'OMNI_FLAG_HISTORICO')  
                elif response == "No authorization provided":
                    print(f"ERRO(lambda_function.lambda_handler) - {id_contato}: Erro na API Meta")
                    id_flag = osFunc('FINALIZADO')
    
                    execute_query_db(f'''UPDATE OMNI_CONVERSA SET 
                                    ID_LAST_FLAG = {id_flag} 
                                    WHERE 
                                    DATETIME_CONVERSA = "{datetime_conversa}" AND
                                    ID_CAMPANHA = {id_campanha} AND
                                    ID_CONTATO = {id_contato} LIMIT 1;''') 
        
                    inputMessage = {
                                'ID_CONTATO': [id_contato], 
                                'ID_CAMPANHA': [id_campanha],
                                'DATETIME_CONVERSA': [datetime_conversa],
                                'DATETIME_FLAG': [formatted_datetime],
                                'ID_FLAG': [id_flag]
                    } 
                    insert_data_into_db(pd.DataFrame(inputMessage), 'OMNI_FLAG_HISTORICO')
                else:
                    inputMessage = {
                                'ID_CONTATO': [id_contato], 
                                'ID_CAMPANHA': [id_campanha],
                                'DATETIME_CONVERSA': [datetime_conversa],
                                'MENSAGEM_DATETIME': [formatted_datetime_msg],
                                'CONTEUDO': [response2Client['content']],
                                'CUSTO': [0],
                                'ID_AUTOR': [0]
                    } 
                    insert_data_into_db(pd.DataFrame(inputMessage), 'OMNI_MENSAGEM')

        #EDITAR PARA POSTBACK
        elif response2Client['type'] == 'interactive':
            response2Client['content'] = json.loads(response2Client['content'])
            response = enviar_payload(response2Client['content'], header = HEADER)

            inputMessage = {
                        'ID_CONTATO': [id_contato], 
                        'ID_CAMPANHA': [id_campanha],
                        'DATETIME_CONVERSA': [datetime_conversa],
                        'MENSAGEM_DATETIME': [formatted_datetime_msg],
                        'CONTEUDO': [response2Client['content']['interactive']['body']['text']],
                        'CUSTO': [0],
                        'ID_AUTOR': [0]
            } 
            insert_data_into_db(pd.DataFrame(inputMessage), 'OMNI_MENSAGEM')
        
        if osFunc('Ambiente_Lambda'):
            return {'statusCode': 200}
        else:
            return {'resposta':response2Client['content'],'statusCode': 200} 
            
    # elif response2Client['content'] is not None and response2Client['content'] == '':
        
    #     msg = message_text
    #     partes = msg.split("-")  # Divide a string em partes usando o caractere "-"
    #     NPS = int(partes[0].strip())  # Remove espaços em branco e obtém o primeiro elemento
    #     print(f"NPS: {NPS} ")
    #     execute_query_db(f"UPDATE CONVERSA SET NPS = {NPS} WHERE TELEFONE = {telefone} AND DATETIME_CONVERSA = '{datetime_conversa}';")
    #     return {'statusCode': 200}
    ############################################################################
    # ENVIAR OS IDs para o SQS
    ############################################################################
    if osFunc('Ambiente_Lambda'):
        # URL da fila SQS
        if 'producao' in os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            queue_url = osFunc('urlSQS_mensagem_producao')
        else:
            queue_url = osFunc('urlSQS_mensagem_homolog')
        # Envia a mensagem para a fila SQS
        sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({'id_conversa': [str(id_contato), str(id_campanha), str(datetime_conversa)], 'timestamp': str(formatted_datetime_msg)}), 
            MessageGroupId='envio_direto')
        return {'statusCode': 200}
    else:
        event = {'id_conversa': [str(id_contato), str(id_campanha), str(datetime_conversa)], 'timestamp': str(formatted_datetime_msg)}
        bot = processa_mensagem(event, {})
        return bot