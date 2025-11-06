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
            telefone_wpp = body_json['telefone_wpp']
            body_json = body_json['post']
        else:
            timestamp = body_json['timestamp'] 
            telefone_wpp = body_json['telefone_wpp']
            body_json = json.loads(body_json['post'])
    else:
        body_json = event['body']['post']
        telefone_wpp = event['body']['telefone_wpp']
        timestamp = event['body']['timestamp']
        print(f"telefone_wpp: {telefone_wpp}")
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
    # EXTRAIR TELEFONE
    ############################################################################
    #print("INFO(lambda_function.lambda_handler): EVENT ",record['body'])
    # print(body_json)
    telefone = body_json['entry'][0]['changes'][0]['value']['contacts'][0]['wa_id']
    # Arruma os números com menos algarismos
    if len(telefone)==12 and telefone[:2] == '55':
        telefone = telefone[:4] + '9' + telefone[4:] 
    
    print(f"INFO(lambda_function.lambda_handler) - {telefone}: Antes de resgatar informações da conversa")
    
    
    ############################################################################
    #RESAGATE DAS INFORMAÇÕES DE CLIENTE (CONVERSA, NOME, AGENDAMENTO, AMIGO)
    ############################################################################
    # if osFunc('Ambiente_Lambda'): 
    #     if 'homolog' in os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
    #         fake_telefones = list(query_to_dataframe('SELECT * FROM FAKE_TELEFONE')["TELEFONE"].unique())
    #         if int(telefone) in fake_telefones:
    #             telefone = get_fake_number(telefone) 
    # else:
    #     fake_telefones = list(query_to_dataframe('SELECT * FROM FAKE_TELEFONE')["TELEFONE"].unique())
    #     if int(telefone) in fake_telefones:
    #         telefone = get_fake_number(telefone) 
    conversa = query_to_dataframe(f"""SELECT 
                                    CONVERSA.*,
                                    CONTATO.NOME_CLIENTE,
                                    CONTATO.GENERO,
                                    CAMP_AGENDAMENTO.UNIDADE_PSD,
                                    CAMP_AGENDAMENTO.DATA,
                                    CAMP_AGENDAMENTO.HORARIO,
                                    CAMP_INDICADOR.NOME_AMIGO,
                                    CAMPANHA.COMPLETAR_CADASTRO,
                                    CAMPANHA.AGENDAMENTO_CADASTRADO,
                                    TELEFONE_WPP.TOKEN
                                FROM
                                    CONVERSA
                                        LEFT JOIN
                                    CONTATO ON CONVERSA.TELEFONE = CONTATO.TELEFONE
                                        LEFT JOIN
                                    CAMP_AGENDAMENTO ON CONVERSA.TELEFONE = CAMP_AGENDAMENTO.TELEFONE
                                        LEFT JOIN
                                    CAMP_INDICADOR ON CONVERSA.TELEFONE = CAMP_INDICADOR.TELEFONE
                                        LEFT JOIN
                                    CAMPANHA ON CONVERSA.ID_CAMPANHA = CAMPANHA.ID_CAMPANHA
                                        LEFT JOIN
                                    TELEFONE_WPP ON TELEFONE_WPP.TELEFONE_WPP = CONVERSA.TELEFONE_WPP
                                WHERE
                                    CONVERSA.TELEFONE = {telefone}
                                    AND CONVERSA.ID_RESULTADO != {osFunc("ADICIONADO")}
                                    AND CONVERSA.DATETIME_CONVERSA >= DATE_SUB(CURDATE(), INTERVAL 14 DAY)
                                ORDER BY CONVERSA.DATETIME_CONVERSA DESC
                                LIMIT 1;  
                                    """)
    
                                 
    df_token = query_to_dataframe(f"SELECT TOKEN FROM TELEFONE_WPP WHERE TELEFONE_WPP = '{telefone_wpp}'")
    print(df_token)
    wpp_token = df_token["TOKEN"].iloc[0]
    HEADER = {'D360-Api-Key': wpp_token, 'Content-Type': "application/json",}

    ############################################################################
    #TRATANDO CONVERSAS RECEPTIVAS
    ############################################################################
    
    print(f"INFO(lambda_function.lambda_handler) - {telefone}: Depois de resgatar informações da conversa, {conversa.empty}")
    
    # CASO NÃO EXISTA UMA CONVERSA PARA O TELEFONE OU A CONVERSA SEJA ANTIGA, CRIA-SE UMA NOVA CONVERSA AQUI                    
    if conversa.empty or pytz.timezone('America/Sao_Paulo').localize(conversa['DATETIME_CONVERSA'][0]) < datetime.now(pytz.timezone('America/Sao_Paulo')) - timedelta(days=osFunc('ANTIGUIDADE_MAXIMA')):
        print(f"INFO(lambda_function.lambda_handler)-{telefone}: Começando cadastrado receptivo")
        response2Client = cadastrar_receptivo(telefone, body_json, telefone_wpp, header = HEADER)
        print(f"INFO(lambda_function.lambda_handler)-{telefone}: Cliente receptivo cadastrado")
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
    
    print(f"INFO(lambda_function.lambda_handler) - {telefone}: Antes de extrair mensagem")

    ############################################################################
    # VERIFICAR SE O NÚMERO DO WHATSAPP DE RECEPÇÃO É IGUAL 
    # AO QUE ESTÁ CADASTRADO NA CONVERSA
    ############################################################################ 
    if conversa['TELEFONE_WPP'][0] != telefone_wpp:
        execute_query_db(f"UPDATE CONVERSA SET TELEFONE_WPP = {telefone_wpp} WHERE TELEFONE = {telefone} AND DATETIME_CONVERSA = '{datetime_conversa}' LIMIT 1")
        df_token = query_to_dataframe(f"SELECT TOKEN FROM TELEFONE_WPP WHERE TELEFONE_WPP = {telefone_wpp}")
        wpp_token = df_token["TOKEN"].iloc[0]
        HEADER = {'D360-Api-Key': wpp_token,'Content-Type': "application/json",}
     
    ############################################################################
    #EXTRAIR MENSAGEM ENVIADO PELO CLIENTE DAS DIFERENTES MIDIAS
    ############################################################################
    response2Client = {'type': None, 'content': None}
    message_text, id_mensagem, response2Client, id_flag, id_resultado, id_campanha, id_media, media_type, media_extension = extract_message(body_json, conversa, current_datetime, http_session, wpp_token, telefone_wpp)
    conversa.loc[0, 'ID_CAMPANHA'] = id_campanha
    ############################################################################
    # RESET DA CONVERSA
    ############################################################################
    print(f"INFO(lambda_function.lambda_handler) - {telefone}: Mensagem recebida = {message_text}")
    
    reset_bool, nova_campanha_list  = check_reset_presence_and_extract_value(message_text)
    if reset_bool:
        id_campanha = conversa['ID_CAMPANHA'][0]
        if len(nova_campanha_list) == 0:
            nova_campanha = None
        else:
            nova_campanha = nova_campanha_list[0]
        response2Client = reset_conversa(telefone, id_campanha, telefone_wpp, header = HEADER, novo_id_campanha = nova_campanha, banco_dados=conversa["BANCO_DADOS"][0])
        print(f"INFO(lambda_function.lambda_handler)-{telefone}: Conversa Resetada")
        if osFunc('Ambiente_Lambda'):
            return {'statusCode': 200} 
        else:
            return {'resposta':response2Client['content']}

    ############################################################################
    # UPLOAD DO RESULTADO
    ############################################################################
    
    #Mudar resultado se for a primeira resposta do cliente (resultado anterior SEM_INTERACAO) 
    if response2Client['type'] == None and conversa['ID_RESULTADO'][0] == osFunc('SEM_INTERACAO'):
        verificacao_confirmacao = verificar_confirmacao(telefone, id_campanha = id_campanha)
        if id_campanha in [osFunc("CONFIRMACAO"), osFunc("CONFIRMACAO_2H")] and not verificacao_confirmacao['agendado']:
            print(f"INFO(lambda_function.lambda_handler) - {telefone}: Cliente foi cancelado pelo telemarketing")
            execute_query_db(f"UPDATE CONVERSA SET ID_RESULTADO = {osFunc('RESPONDIDO')} WHERE TELEFONE = {telefone} AND DATETIME_CONVERSA = '{datetime_conversa}';")
            response2Client['type'] = 'text'
            telefone, cliente, unidade_nome, date, concat_times = gerar_variaveis_mensagem_automatica(conversa, current_datetime, http_session)
            print((verificacao_confirmacao['horario']))
            print((verificacao_confirmacao['data']))
            response2Client['content'] = f"""Parece que o horário da sua avaliação já passou ou não está mais disponível, lembrando que oferecemos 15 minutos de tolerância para atrasos. Seria necessário reagendar para outro momento. Para a unidade {unidade_nome}, a próxima data disponível é {date} nos seguintes horá:
                
- {concat_times}
                
Por favor, me informe qual desses horários seria mais conveniente para você reagendar sua avaliação."""
            id_flag = osFunc('AGUARDAR')
        else: #Cliente ainda possui agendamento
            execute_query_db(f"UPDATE CONVERSA SET ID_RESULTADO = '{id_resultado}' WHERE TELEFONE = '{telefone}' AND DATETIME_CONVERSA = '{datetime_conversa}' LIMIT 1;")
     
    #Mudar resultado em qualquer mensagem da pessoa
    elif conversa['ID_RESULTADO'][0] not in [osFunc('SUCESSO'), osFunc('FALHA_AGENDAMENTO'), osFunc('INTERVENCAO'), osFunc('TELEMARKETING'), osFunc('SUCESSO_REMARKETING')]: #Se o resultado anterior era SEM_INTERACAO, ou seja, trata-se da primeira mensagem do usuário
        verificacao_confirmacao = verificar_confirmacao(telefone, id_campanha = id_campanha)
        if id_campanha in [osFunc("NOSHOW"), osFunc("CANCELADOS"), osFunc("INDIQUE_AMIGOS"), osFunc("LEADS_META"), osFunc("RECEPTIVO"), osFunc("SAPATOS_SITE"), osFunc("AGENDAMENTO_SITES"), osFunc("LEADS_WHATSAPP"),  osFunc("RECEPTIVO_GOOGLE"), osFunc("RECEPTIVO_LP_FASCITE")] and verificacao_confirmacao['agendado']: #Cliente já possui agendamento no futuro
            print(f"INFO(lambda_function.lambda_handler) - {telefone}: Cliente já foi agendado pelo telemarketing")
            execute_query_db(f"UPDATE CONVERSA SET ID_RESULTADO = {osFunc('TELEMARKETING')} WHERE TELEFONE = {telefone} AND DATETIME_CONVERSA = '{datetime_conversa}';")
            response2Client['type'] = 'text'
            response2Client['content'] = f"Verifiquei aqui no sistema que você já possui uma avaliação gratuita marcada para {formatar_data((verificacao_confirmacao['data']))}, às {(verificacao_confirmacao['horario'])}. Se quiser reagendar ou tiver alguma dúvida, estou à disposição para ajudar!"
            id_flag = osFunc('AGUARDAR')
        else: #Cliente não possui agendamento
            execute_query_db(f"UPDATE CONVERSA SET ID_RESULTADO = '{id_resultado}' WHERE TELEFONE = '{telefone}' AND DATETIME_CONVERSA = '{datetime_conversa}' LIMIT 1;")
    
    #Mudar resultado se fazer_acao retornar SUCESSO ou FALHA_AGENDAMENTO
    elif id_resultado in [osFunc('SUCESSO'),osFunc('FALHA_AGENDAMENTO')]:
        execute_query_db(f"UPDATE CONVERSA SET ID_RESULTADO = '{id_resultado}' WHERE TELEFONE = '{telefone}' AND DATETIME_CONVERSA = '{datetime_conversa}' LIMIT 1;")
    
    print(f"INFO(lambda_function.lambda_handler) - {telefone}: depois de upload resultado")
    ############################################################################
    # SALVAR A MENSAGEM RECEPTIVA NO DB
    ############################################################################

    if message_text != 'reset' and message_text != 'Reset':    
        inputMessage = {
                        'TELEFONE': [telefone], 
                        'ID_CAMPANHA': [id_campanha],
                        'DATETIME_CONVERSA': [datetime_conversa],
                        'MENSAGEM_DATETIME': [formatted_datetime_msg],
                        'CONTEUDO': [message_text],
                        'CUSTO': [0],
                        'AUTOR_ID_AUTOR': [1],
                        'ID_MENSAGEM' : id_mensagem,
                        'MEDIA' : [media_type]
        } 
        
        if media_type == '':

            insert_data_into_db(pd.DataFrame(inputMessage), 'MENSAGEM')

        else:
            last_id = insert_data_into_db(pd.DataFrame(inputMessage), 'MENSAGEM', return_last_id=True)
            metadados_json = {
                "id": f"{id_media}",
                "type": f"{media_type}",
                "extension": f"{media_extension}"
                }
            metadados = json.dumps(metadados_json)
            query = f"""
                INSERT INTO MENSAGEM_METADADOS(ID_MENSAGEM, METADADOS)
                VALUES('{last_id}', '{metadados}')
                """
            execute_query_db(query)
        print(f"INFO(lambda_function.lambda_handler) - {telefone}: Depois de salvar metadados de mídia")

    print(f"INFO(lambda_function.lambda_handler) - {telefone}: Depois de salvar mensagem receptiva")
    ############################################################################
    # ON HOLD - TERMINA A FUNÇÃO
    ############################################################################ 
    if conversa['ID_LAST_FLAG'][0] == int(osFunc("ON_HOLD")) or id_flag == int(osFunc("ON_HOLD")):
        print(f"INFO(lambda_function.lambda_handler) - {telefone}: Cliente ON-HOLD")
        return {'resposta':'ON_HOLD','statusCode': 200} 

    ############################################################################
    # UPLOAD DA FLAG
    ############################################################################

    execute_query_db(f'''UPDATE CONVERSA SET 
                                ID_LAST_FLAG = {id_flag} 
                                WHERE 
                                DATETIME_CONVERSA = "{datetime_conversa}" AND
                                ID_CAMPANHA = {id_campanha} AND
                                TELEFONE = {telefone} LIMIT 1;''') 
    
    # upload das flags
    inputMessage = {
                'TELEFONE': [telefone], 
                'ID_CAMPANHA': [id_campanha],
                'DATETIME_CONVERSA': [datetime_conversa],
                'DATETIME_FLAG': [formatted_datetime],
                'ID_FLAG': [id_flag]
    } 
    insert_data_into_db(pd.DataFrame(inputMessage), 'FLAG_HISTORICO')  
    
    print(f"INFO(lambda_function.lambda_handler) - {telefone}: Depois de upload flag")
    
    
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
            execute_query_db(f"UPDATE CONTATO SET GENERO = '{genero}' WHERE TELEFONE = {telefone} LIMIT 1")
    
    print(f"INFO(lambda_function.lambda_handler) - {telefone}: Depois de upload genero")
    
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
                response = enviar_wpp(telefone, response2Client['content'], header = HEADER)

        
                if response == "False":
                    print(f"ERRO(lambda_function.lambda_handler) - {telefone}: Erro na dialog")
                    id_flag = osFunc('RESPONDER')
    
                    execute_query_db(f'''UPDATE CONVERSA SET 
                                    ID_LAST_FLAG = {id_flag} 
                                    WHERE 
                                    DATETIME_CONVERSA = "{datetime_conversa}" AND
                                    ID_CAMPANHA = {id_campanha} AND
                                    TELEFONE = {telefone} LIMIT 1;''') 
        
                    inputMessage = {
                                'TELEFONE': [telefone], 
                                'ID_CAMPANHA': [id_campanha],
                                'DATETIME_CONVERSA': [datetime_conversa],
                                'DATETIME_FLAG': [formatted_datetime],
                                'ID_FLAG': [id_flag]
                    } 
                    insert_data_into_db(pd.DataFrame(inputMessage), 'FLAG_HISTORICO')  
                elif response == "No authorization provided":
                    print(f"ERRO(lambda_function.lambda_handler) - {telefone}: Erro na dialog")
                    id_flag = osFunc('FINALIZADO')
    
                    execute_query_db(f'''UPDATE CONVERSA SET 
                                    ID_LAST_FLAG = {id_flag} 
                                    WHERE 
                                    DATETIME_CONVERSA = "{datetime_conversa}" AND
                                    ID_CAMPANHA = {id_campanha} AND
                                    TELEFONE = {telefone} LIMIT 1;''') 
        
                    inputMessage = {
                                'TELEFONE': [telefone], 
                                'ID_CAMPANHA': [id_campanha],
                                'DATETIME_CONVERSA': [datetime_conversa],
                                'DATETIME_FLAG': [formatted_datetime],
                                'ID_FLAG': [id_flag]
                    } 
                    insert_data_into_db(pd.DataFrame(inputMessage), 'FLAG_HISTORICO')
                else:
                    inputMessage = {
                                'TELEFONE': [telefone], 
                                'ID_CAMPANHA': [id_campanha],
                                'DATETIME_CONVERSA': [datetime_conversa],
                                'MENSAGEM_DATETIME': [formatted_datetime_msg],
                                'CONTEUDO': [response2Client['content']],
                                'CUSTO': [0],
                                'AUTOR_ID_AUTOR': [0],
                                'ID_MENSAGEM' : id_mensagem_response
                    } 
                    insert_data_into_db(pd.DataFrame(inputMessage), 'MENSAGEM')

        elif response2Client['type'] == 'interactive':
            response2Client['content'] = json.loads(response2Client['content'])
            response = enviar_payload(response2Client['content'], header = HEADER)

            inputMessage = {
                        'TELEFONE': [telefone], 
                        'ID_CAMPANHA': [id_campanha],
                        'DATETIME_CONVERSA': [datetime_conversa],
                        'MENSAGEM_DATETIME': [formatted_datetime_msg],
                        'CONTEUDO': [response2Client['content']['interactive']['body']['text']],
                        'CUSTO': [0],
                        'AUTOR_ID_AUTOR': [0],
                        'ID_MENSAGEM' : id_mensagem_response
            } 
            insert_data_into_db(pd.DataFrame(inputMessage), 'MENSAGEM')
        
        if osFunc('Ambiente_Lambda'):
            return {'statusCode': 200}
        else:
            return {'resposta':response2Client['content'],'statusCode': 200} 
            
    elif response2Client['content'] is not None and response2Client['content'] == '':
        
        msg = message_text
        partes = msg.split("-")  # Divide a string em partes usando o caractere "-"
        NPS = int(partes[0].strip())  # Remove espaços em branco e obtém o primeiro elemento
        print(f"NPS: {NPS} ")
        execute_query_db(f"UPDATE CONVERSA SET NPS = {NPS} WHERE TELEFONE = {telefone} AND DATETIME_CONVERSA = '{datetime_conversa}';")
        return {'statusCode': 200}
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
            MessageBody=json.dumps({'id_conversa': [str(telefone), str(id_campanha), str(datetime_conversa)], 'timestamp': str(formatted_datetime_msg)}), 
            MessageGroupId='envio_direto')
        return {'statusCode': 200}
    else:
        event = {'id_conversa': [str(telefone), str(id_campanha), str(datetime_conversa)], 'timestamp': str(formatted_datetime_msg)}
        bot = processa_mensagem(event, {})
        return bot
    

   
 
















