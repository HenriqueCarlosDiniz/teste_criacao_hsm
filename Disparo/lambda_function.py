
from utils.config import *
import json
import os
import pytz
import requests
import time
from datetime import datetime, timedelta
from utils.locations import converter_nome
from utils.connect_db import query_to_dataframe, DatabaseConnection, execute_query_db
from utils.api_communication import GET_unidades_ativas, GET_vagas_disponiveis, verificar_confirmacao
from utils.text import format_data_feira, get_categoria_dor
import boto3
import traceback

if osFunc('Ambiente_Lambda'):
    from invoke_lambda import invokeFunction
else: 
    from enviaDisparo.lambda_function import lambda_handler as enviaDisparo


def get_all_units(http_session):
    data_all = GET_unidades_ativas(http_session)
    data_all = json.loads(data_all.text)
    retorno = []
    for value in data_all:
        retorno.append(value['grupoFranquia'])
    return retorno
    
def get_desconto_remarketing(telefone,datetime_conversa):
    ## REMARKETING ##
    #completar com a query que pega o desconto a partir do telefone
    query_desconto = f""" 
        SELECT r.STRING_DESCONTO
        FROM REMARKETING r
        INNER JOIN CONVERSA c ON r.ID_REMARKETING = c.ID_REMARKETING
        WHERE c.TELEFONE = '{telefone}' AND DATETIME_CONVERSA = '{datetime_conversa}';
    """
    df = query_to_dataframe(query_desconto)
    print(f"telefone: {telefone}")
    print(f"datetime_conversa: {datetime_conversa}")
    string_desconto = df['STRING_DESCONTO'][0]
    
    print(string_desconto)

    return string_desconto
    
def unidades_indisponiveis(http_session):
    # Pega a lista com todas as unidades disponiveis e fica so com as siglas 
    lista_siglas = get_all_units(http_session)
    # Pega a data atual e 15 dias pra frente 
    data_inicial = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d")
    data_final = (datetime.now(pytz.timezone('America/Sao_Paulo')) + timedelta(days=osFunc("BLOQUEIO_AGENDA"))).strftime("%Y-%m-%d")

    # Funcao que checa se tem algum horario com uma vaga ou mais 
    def checar_vagas_disponiveis(vagas_unidade):
        try:
            dicionario = json.loads(vagas_unidade.text)
            for data in dicionario:
                for horario in dicionario[data]:
                    if dicionario[data][horario]["total"] >= 1:
                        return True 
            return False
        except:
            return False
    
    # Itera a lista de siglas e adiciona em uma lista nova as que estao indisponiveis 
    unidades_indisponiveis = []
    for i in lista_siglas:
        vagas_unidade = GET_vagas_disponiveis(data_inicial, data_final, i, http_session)
        unidade_disponivel = checar_vagas_disponiveis(vagas_unidade)
        if not unidade_disponivel:
            unidades_indisponiveis.append(i) 
            
    unidades_disponiveis = list(set(lista_siglas) - set(unidades_indisponiveis))
            
    return unidades_indisponiveis, unidades_disponiveis
    
def query_composer(campanha, unidades_bool, antiguidade, id_flags, set_unidades_disponiveis, limit, banco_dados):
    query_clientes = f"""
        SELECT 
            FD.*, H.CONTEUDO, H.MESSAGE_PAYLOAD, C.BANCO_DADOS, C.ID_HSM, C.ID_HSM_SECUNDARIO
        FROM FILA_DISPARO FD
        JOIN CONVERSA C ON FD.TELEFONE = C.TELEFONE AND FD.ID_CAMPANHA = C.ID_CAMPANHA AND FD.DATETIME_CONVERSA = C.DATETIME_CONVERSA
        JOIN HSM H ON (C.ID_HSM_SECUNDARIO IS NULL    AND C.ID_HSM = H.ID_HSM)
                    OR (1 AND 10 IN ({id_flags})      AND C.ID_HSM_SECUNDARIO = H.ID_HSM)
                    OR (NOT (1 AND 10 IN ({id_flags})) AND C.ID_HSM = H.ID_HSM)
        WHERE 
            FD.DATETIME_CONVERSA > DATE_SUB(CURDATE(), INTERVAL {antiguidade} DAY) AND
            FD.ID_LAST_FLAG IN ({id_flags}) AND
            FD.ID_CAMPANHA = {campanha} AND
            FD.BANCO_DADOS = {banco_dados}
        """
    
    if unidades_bool:
        query_clientes += f"""AND FD.UNIDADE_PSD IN {set_unidades_disponiveis}"""
    if limit:
        query_clientes += f""" LIMIT {limit}"""
    return query_clientes
    

def lambda_handler(event, context): 
    if osFunc('Ambiente_Lambda'):
        if 'producao' in os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
          invokeFunction({}, "arn:aws:lambda:sa-east-1:191246610649:function:producao_checa_validade_HSM")
        else:
            #invokeFunction({}, "arn:aws:lambda:sa-east-1:191246610649:function:homolog_checa_validade_HSM")
            pass 
    else:
        pass
    
    ################## VERIFICAÇÃO DO HORÁRIO ATUAL ############################ 
    unidades_info = get_unidades()
    horario_atual = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%H:%M:%S')
    margem_min = 5
    horario_inf = (datetime.now(pytz.timezone('America/Sao_Paulo')) - timedelta(minutes=margem_min)).strftime('%H:%M:%S') ########DESCOMENTAR AQUI!
    horario_sup = (datetime.now(pytz.timezone('America/Sao_Paulo')) + timedelta(minutes=margem_min)).strftime('%H:%M:%S') ########DESCOMENTAR AQUI!
    print("Horário atual:", horario_atual)
    print("Time 5 minutes before:", horario_inf)
    print("Time 5 minutes after:", horario_sup)
    
    ################## VERIFICAÇÃO DO DISPARO ############################
    query_disparo = f"""SELECT 
                        	DISPARO.*,
                            CAMPANHA.AGENDAMENTO_CADASTRADO
                        FROM
                        	DISPARO
                        LEFT JOIN CAMPANHA ON CAMPANHA.ID_CAMPANHA = DISPARO.ID_CAMPANHA
                        WHERE
                        	EXISTS( SELECT 
                        			1
                        		FROM
                        			(SELECT DISTINCT
                        				TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(DISPARO.HORARIO, ',', n.digit + 1), ',', - 1)) AS horario_value
                        			FROM
                        				(SELECT 
                        				(a.digit + b.digit * 10) AS digit
                        			FROM
                        				(SELECT 0 AS digit UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a
                        			CROSS JOIN (SELECT 0 AS digit UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b) n
                        			WHERE
                        				TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(DISPARO.HORARIO, ',', n.digit + 1), ',', - 1)) BETWEEN '{horario_inf}' AND '{horario_sup}') AS horarios);
                    """
    df_disparo = query_to_dataframe(query_disparo)
    if df_disparo.empty:
        return {'status': 200}
    
    
    ################## VERIFICAÇÃO DA DISPONIBILIDADE DAS UNIDADES #############
    # Carrega a lista de unidades disponíveis e converte para uma tupla
    http_session = requests.Session() 
    _, lista_unidades_disponiveis = unidades_indisponiveis(http_session)
    set_unidades_disponiveis = tuple(lista_unidades_disponiveis)
    http_session.close()

    for index, row in df_disparo.iterrows():
        try: #em caso de erro no disparo de uma campanha, não afeta as outras
            t = time.time()
            campanha, agendamento_bool, unidades_bool, antiguidade, id_flags, limit, banco_dados = row['ID_CAMPANHA'],row['AGENDAMENTO_CADASTRADO'], row['UNIDADES_DISPONIVEIS'], row['ANTIGUIDADE'], row['ID_FLAGS'], row['LIMIT'], row['BANCO_DADOS']
            
            print("    ##################  DISPARO PARA CAMPANHA: ", campanha, "  ################## ")
            query_clientes = query_composer(campanha, unidades_bool, antiguidade, id_flags, set_unidades_disponiveis, limit, banco_dados)
            
            clientes = query_to_dataframe(query_clientes) 
            if clientes.empty: 
                print(f"INFO(lambda_handler:producao_disparo): Não há cliente para disparar na campanha {campanha}")
                continue
        
            # Resgatando o token do whatsapp
            token = clientes['TOKEN'].iloc[0]

 
            payloads, query_conversa, query_flag, query_mensagem = update_disparo(clientes, unidades_info)

            ################## DEFINIÇÃO DO NÚMERO DE BATCHES ######################
            # Número de clientes
            numero_clientes = len(payloads.keys())
            print(f"INFO(lambda_handler:producao_disparo): numero_clientes = {numero_clientes}")
            
            MAX_MENSAGEM_POR_SEGUNDO_DIALOG = 50
            MEDIA_DISPARO_POR_SEGUNDO = 2 # uma função lambda
            MARGEM_SEGURANCA_CONEXAO = 15
            n_batch = MAX_MENSAGEM_POR_SEGUNDO_DIALOG/MEDIA_DISPARO_POR_SEGUNDO - MARGEM_SEGURANCA_CONEXAO # 10
            
            count_max = min(numero_clientes,int(numero_clientes//n_batch + 1)) 
            print(f"INFO(lambda_handler:producao_disparo): numero clientes por batch = {count_max}")
            
            numero_clientes_disparo = min(numero_clientes, n_batch * count_max)
            print(f"INFO(lambda_handler:producao_disparo): numero clientes disparados = {numero_clientes_disparo}")
            ################## LOOP PARA DISPARO EM BATCHES ########################
            
            count = 0
            batch_hsm_payload = []
            batch_id = []
            batch_campanha = []
            batch_date_time = [] 
            
            for key, value in payloads.items():
                try: #se der problema em um contato, pula e continua gerando os batches
                    count+=1
                    # batch_hsm_payload.update({key:value})
                    batch_hsm_payload.append(value)
                    batch_date_time.append(key[0].strftime('%Y-%m-%d %H:%M:%S'))
                    batch_id.append(key[1])
                    batch_campanha.append(key[2]) 
                    if count == count_max:
                        input_envio = {
                                        "hsm_payload": batch_hsm_payload,
                                        'id': batch_id,
                                        'campanha': batch_campanha,
                                        'datetime': batch_date_time,
                                        'token_wpp': token
                                        }
                        print(f"INFO(lambda_handler:producao_disparo): input -> enviaDisparo: {batch_id}")
                        if osFunc('Ambiente_Lambda'): 
                            if 'producao' in os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
                                invokeFunction(input_envio, "arn:aws:lambda:sa-east-1:191246610649:function:producao_enviaDisparo")
                            else:
                                invokeFunction(input_envio, "arn:aws:lambda:sa-east-1:191246610649:function:homolog_enviaDisparo")
                        else: enviaDisparo(input_envio,{})
        
                        count = 0
                        batch_hsm_payload = []
                        batch_id = []
                        batch_campanha = []
                        batch_date_time = []  
                except Exception as e:
                    traceback_str = traceback.format_exc()
                    print(f"ERROR(lambda_handler:producao_disparo): {index}")
                    print(f"ERROR(lambda_handler:producao_disparo): {row}", traceback_str)
                    
            if len(batch_id)>0:
                input_envio = {
                                    "hsm_payload": batch_hsm_payload,
                                    'id': batch_id,
                                    'campanha': batch_campanha,
                                    'datetime': batch_date_time,
                                    'token_wpp': token
                                    }
                print(f"INFO(lambda_handler:disparo) fora do for: input -> enviaDisparo: {batch_id}")
                if osFunc('Ambiente_Lambda'): 
                    if 'producao' in os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
                        invokeFunction(input_envio, "arn:aws:lambda:sa-east-1:191246610649:function:producao_enviaDisparo")# Envio de mensagem ao client
                    else:
                        invokeFunction(input_envio, "arn:aws:lambda:sa-east-1:191246610649:function:homolog_enviaDisparo")
                else: enviaDisparo(input_envio,{})
                
            print(f"INFO(lambda_handler:producao_disparo): Tempo disparo campanha {campanha} = {time.time()-t}")
        except Exception as e:
            traceback_str = traceback.format_exc()
            print(f"ERROR(lambda_handler:producao_disparo): {index}")
            print(f"ERROR(lambda_handler:producao_disparo): {row}", traceback_str)
    

        execute_query_db(query_conversa)
        execute_query_db(query_flag)
        execute_query_db(query_mensagem)
    DatabaseConnection.dispose_engine()
    return {'status': 200}

from datetime import datetime

def update_disparo(df, unidades_info):
    
    # Data/hora atual para usar nas inserções
    now = datetime.now(pytz.timezone('America/Sao_Paulo'))
    datetime_flag_str = now.strftime('%Y-%m-%d %H:%M:%S')
    datetime_flag_msg = now.strftime('%Y-%m-%d %H:%M:%S.%f')
    
    payloads = {}
    neg_response= []
    neg_id = []
    neg_campanha = []
    neg_date_time = []

    conversa_datetime = []
    conversa_id_campanha = []
    conversa_telefone = []
    conversa_id_hsm = []
    conversa_id_hsm_secundario = []
    conversa_resultado = []
    conversa_banco_dados = []
    flag_historico_flag = []
    mensagem_datetime = []
    mensagem_conteudo = []
    mensagem_id_autor = []
    mensagem_custo = []
    flag_datetime = []

    for index, row in df.iterrows():
        try:#se der problema em um contato, pula e continua gerando os batches
            campanha = row["ID_CAMPANHA"]
            resp = verificar_confirmacao(row['TELEFONE'])
            if (campanha == osFunc("CONFIRMACAO") and resp['confirmado']==True) or (campanha not in [osFunc('CONFIRMACAO'),osFunc('CONFIRMACAO_2H')] and resp['agendado']==True):
                neg_response.append('Telemarketing')
                neg_date_time.append(row["DATETIME_CONVERSA"].strftime('%Y-%m-%d %H:%M:%S'))
                neg_id.append(row["TELEFONE"])
                neg_campanha.append(row["ID_CAMPANHA"])
            else:
                mensagem = row['CONTEUDO']
                mensagem = mensagem.replace('{cliente}', row['NOME_CLIENTE'])
                if campanha == osFunc("INDIQUE_AMIGOS"):
                    mensagem = mensagem.replace('{amigo}', row["NOME_AMIGO"])
                elif campanha == osFunc("LEADS_WHATSAPP") or campanha == osFunc("LEADS_META"):
                    pass
                elif campanha == osFunc("REMARKETING"): ### REMARKETING ###
                    mensagem = mensagem.replace('{nome_especialista}', row['NOME_ESPECIALISTA'])
                    dor_principal = row['DOR_PRINCIPAL']
                    dor_principal = get_categoria_dor(dor_principal)
                    mensagem = mensagem.replace('{dor_principal}', dor_principal)
                    mensagem = mensagem.replace('{string_desconto}', row['STRING_DESCONTO'])
                    
                else:
                    unidade_nome  =  f"{unidades_info[row['UNIDADE_PSD']]['Nome']}"  
                    unidade_endereco = unidades_info[row['UNIDADE_PSD']]["Endereço"].replace('<strong>', '').replace('</strong>', '').replace('\n', '').replace('\r', '').replace('"', '')
                    unidade_maps = f"https://pessemdor.link/{row['UNIDADE_PSD']}"
                    mensagem = mensagem.replace('{data_agendamento}', row['DATA'].strftime("%d/%m/%Y"))
                    mensagem = mensagem.replace('{data_agendamento2}', format_data_feira(row['DATA']))
                    mensagem = mensagem.replace('{horario_agendamento}', str(row["HORARIO"]).split(' ')[2][:5])
                    mensagem = mensagem.replace('{horario_agendamento2}', datetime.strptime(str(row["HORARIO"]).split(' ')[2][:5], "%H:%M").strftime("%Hh%M"))
                    mensagem = mensagem.replace('{unidade_agendamento}', unidade_nome)
                    mensagem = mensagem.replace('{endereco_agendamento}', unidade_maps)
                    mensagem = mensagem.replace('{endereco_agendamento2}', json.dumps(unidade_endereco, ensure_ascii=False).replace('\\r',''))
                    mensagem = mensagem.replace('{link_forms}', f"https://psdsaopaulo.net.br/psd2/app/questionario/responder-questionario/{str(row['URL_FORMS'])}")
                # Query de atualização para a tabela CONVERSA
                
                conversa_datetime.append(row['DATETIME_CONVERSA'].strftime('%Y-%m-%d %H:%M:%S'))
                conversa_id_campanha.append(row['ID_CAMPANHA'])
                conversa_telefone.append(row['TELEFONE'])
                conversa_resultado.append(osFunc("SEM_INTERACAO"))
                conversa_id_hsm.append(row['ID_HSM'])
                conversa_id_hsm_secundario.append(row['ID_HSM_SECUNDARIO'])
                conversa_banco_dados.append(row['BANCO_DADOS'].iloc[0])
                flag_historico_flag.append(osFunc("HSM_ENVIADO"))
                flag_datetime.append(datetime_flag_str)
                mensagem_datetime.append(datetime_flag_msg)
                mensagem_conteudo.append(mensagem.replace('\'', '\'\''))
                mensagem_id_autor.append(0)
                mensagem_custo.append(0)
 
                payload = row['MESSAGE_PAYLOAD']
                payload = payload.replace('{cliente}', str("\""+row["NOME_CLIENTE"]+"\""))
                payload = payload.replace('{telefone}', str("\""+str(row["TELEFONE"])+"\"")) 
               
                if campanha == osFunc("INDIQUE_AMIGOS"):
                    payload = payload.replace('{amigo}', str("\""+row["NOME_AMIGO"]+"\""))
                elif campanha == osFunc("LEADS_WHATSAPP") or campanha == osFunc("LEADS_META"):
                    pass
                
                elif campanha == osFunc("REMARKETING"): ### REMARKETING ###
                    string_desconto = row['STRING_DESCONTO']
                    payload = payload.replace('{nome_especialista}', str("\""+str(row["NOME_ESPECIALISTA"])+"\""))
                    dor_principal = row["DOR_PRINCIPAL"]
                    dor_principal = get_categoria_dor(dor_principal)
                    payload = payload.replace('{dor_principal}', str("\""+dor_principal+"\"")) 
                    payload = payload.replace('{string_desconto}', str("\""+string_desconto+"\""))
                    
                else:
                    data_agendamento = row['DATA'].strftime("%d/%m/%Y")
                    data_agendamento2 = format_data_feira(row['DATA'])
                    horario = str(row["HORARIO"]).split(' ')[2][:5]
                    horario_agendamento = f"{horario}"
                    horario_agendamento2 = datetime.strptime(horario, "%H:%M").strftime("%Hh%M")
                
                    payload = payload.replace('{data_agendamento}', str("\""+data_agendamento+"\"")) 
                    payload = payload.replace('{data_agendamento2}', str("\""+data_agendamento2+"\"")) 
                    payload = payload.replace('{horario_agendamento}', str("\""+horario_agendamento+"\"")) 
                    payload = payload.replace('{horario_agendamento2}', str("\""+horario_agendamento2+"\"")) 
                    payload = payload.replace('{unidade_agendamento}', str("\""+unidade_nome+"\"")) 
                    payload = payload.replace('{endereco_agendamento}', str("\""+unidade_maps+"\"")) 
                    payload = payload.replace('{endereco_agendamento2}', str("\""+unidade_endereco+"\""))
                    payload = payload.replace('{link_forms}', f"""\"https://psdsaopaulo.net.br/psd2/app/questionario/responder-questionario/{str(row['URL_FORMS'])}\"""")
                payloads.update({(row["DATETIME_CONVERSA"], row["TELEFONE"], row["ID_CAMPANHA"]):payload})
            
            
        except:
            traceback_str = traceback.format_exc()
            print(f"ERROR(lambda_handler:update_disparo): {index}")
            print(f"ERROR(lambda_handler:update_disparo): {row}", traceback_str)
    
    values_list = ", ".join([f"('{formatted_datetime}', '{telefone}', '{id_campanha}', '{id_resultado}', '{id_hsm}', '{id_flag}', '{preco}', '{banco_de_dados}', " +
    (f"'{id_hsm_secundario}'" if id_hsm_secundario is not None else 'NULL') +
    ")" for formatted_datetime, telefone, id_campanha, id_resultado, id_hsm, id_flag, preco, banco_de_dados, id_hsm_secundario in zip(conversa_datetime, conversa_telefone, conversa_id_campanha, conversa_resultado, conversa_id_hsm, flag_historico_flag, mensagem_custo, conversa_banco_dados, conversa_id_hsm_secundario)])
    print(values_list)
    query_conversa = f"""
        INSERT INTO CONVERSA (DATETIME_CONVERSA, TELEFONE, ID_CAMPANHA, ID_RESULTADO, ID_HSM, ID_LAST_FLAG, PRECO, BANCO_DADOS, ID_HSM_SECUNDARIO) 
        VALUES {values_list}
        ON DUPLICATE KEY UPDATE DATETIME_CONVERSA = VALUES(DATETIME_CONVERSA), TELEFONE = VALUES(TELEFONE), ID_CAMPANHA = VALUES(ID_CAMPANHA), ID_RESULTADO = VALUES(ID_RESULTADO), ID_HSM = VALUES(ID_HSM), ID_LAST_FLAG = VALUES(ID_LAST_FLAG), PRECO = VALUES(PRECO), BANCO_DADOS = VALUES(BANCO_DADOS), ID_HSM_SECUNDARIO = VALUES(ID_HSM_SECUNDARIO);
    """
    values_list = ", ".join([f"('{formatted_datetime}', '{telefone}', '{id_campanha}', '{formatted_datetime_flag}', '{id_flag}')" for formatted_datetime, telefone, id_campanha, formatted_datetime_flag, id_flag in zip(conversa_datetime, conversa_telefone, conversa_id_campanha, flag_datetime, flag_historico_flag)])
    query_flag = f"""
        INSERT INTO FLAG_HISTORICO (DATETIME_CONVERSA, TELEFONE, ID_CAMPANHA, DATETIME_FLAG, ID_FLAG) 
        VALUES {values_list}
        ON DUPLICATE KEY UPDATE DATETIME_CONVERSA = VALUES(DATETIME_CONVERSA), TELEFONE = VALUES(TELEFONE), ID_CAMPANHA = VALUES(ID_CAMPANHA), DATETIME_FLAG = VALUES(DATETIME_FLAG), ID_FLAG = VALUES(ID_FLAG);
    """  

    values_list = ", ".join([f"('{formatted_datetime}', '{telefone}', '{id_campanha}', '{formatted_datetime_mensagem}', '{conteudo}', {custo}, {id_autor})" for formatted_datetime, telefone, id_campanha, formatted_datetime_mensagem, conteudo, custo, id_autor in zip(conversa_datetime, conversa_telefone, conversa_id_campanha, mensagem_datetime, mensagem_conteudo, mensagem_custo, mensagem_id_autor)])
    query_mensagem = f"""
        INSERT INTO MENSAGEM (DATETIME_CONVERSA, TELEFONE, ID_CAMPANHA, MENSAGEM_DATETIME, CONTEUDO, CUSTO, AUTOR_ID_AUTOR) 
        VALUES {values_list}
        ON DUPLICATE KEY UPDATE DATETIME_CONVERSA = VALUES(DATETIME_CONVERSA), TELEFONE = VALUES(TELEFONE), ID_CAMPANHA = VALUES(ID_CAMPANHA), MENSAGEM_DATETIME = VALUES(MENSAGEM_DATETIME), CONTEUDO = VALUES(CONTEUDO), CUSTO = VALUES(CUSTO), AUTOR_ID_AUTOR = VALUES(AUTOR_ID_AUTOR));
    """
    
    ### WRITE NA FILA POS
    # Configura o cliente do SQS
    sqs_client = boto3.client('sqs')
    # URL da fila SQS
    if 'producao' in os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
        queue_url = "https://sqs.sa-east-1.amazonaws.com/191246610649/producao_posProcessamento.fifo"
    else:
        queue_url = "https://sqs.sa-east-1.amazonaws.com/191246610649/homolog_posProcessamento.fifo"
    # Envia a mensagem para a fila SQS
    # current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')) 
    # formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    if len(neg_response) > 0:
        print("NEG:", neg_id)
        sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({'responses': neg_response, 'id':neg_id, 'campanha':neg_campanha, 'datetime':neg_date_time}), 
            MessageGroupId='envio_direto')
    return payloads, query_conversa, query_flag, query_mensagem

def get_unidades():
        """
        Obtém informações formatadas sobre todas as unidades ativas da franquia PSD2.

        :return: Um dicionário contendo informações formatadas de todas as unidades ativas.
                Cada unidade é representada por uma chave (sigla) e contém um dicionário com detalhes, incluindo endereço, latitude, longitude e nome.
                Exemplo:
                {
                    'PSD': {'Endereço': 'Rua XYZ, 123, CidadeA', 'Latitude': -23.456, 'Longitude': -46.789, 'Nome': 'Unidade A'},
                    'ABC': {'Endereço': 'Rua ABC, 456, CidadeB', 'Latitude': -12.345, 'Longitude': -56.789, 'Nome': 'Unidade B'},
                    ...
                }
        :rtype: dict
        """
        # Obtendo informações sobre todas as unidades ativas.
        data_all = GET_unidades_ativas()
         
        # Convertendo a resposta da API (JSON) para um objeto Python.
        data_all = json.loads(data_all.text)
 
        # Criando um dicionário vazio para armazenar as informações formatadas das unidades.
        retorno = {}

        # Iterando sobre cada unidade nos dados recebidos da API.
        for value in data_all: 
            # Adicionando informações formatadas da unidade ao dicionário 'retorno'.
            retorno[value['grupoFranquia']] = {
                "Endereço": (
                    value.get('enderecoFranquia', '') + ', ' +
                    value.get('numeroFranquia', '') + ', ' +
                    value.get('bairroFranquia', '') + ', ' +
                    value.get('cidadeFranquia', '') + ', ' +
                    value.get('complementoFranquia', '')
                ),
                "Latitude": value.get('latitude', 0.0),
                "Longitude": value.get('longitude', 0.0),
                "Nome": converter_nome(value.get('nomeFranquia', ''))
            }

        # Retornando o dicionário com as informações das unidades.
        return retorno

 
 