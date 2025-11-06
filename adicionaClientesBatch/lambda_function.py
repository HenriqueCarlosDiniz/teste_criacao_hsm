from utils.config import * 

import json
import time
import pytz
from datetime import datetime 

from utils.connect_db import execute_query_db, execute_queries_db


def lambda_handler(event, context): 
    start_time = time.time()
    print('EVENT ', json.dumps(event))
    formatted_datetime_flag = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S")

    telefone_vec = event['telefone_vec']
    nome_vec = event['nome_vec']
    horario_consulta_vec = event['horario_consulta_vec']
    data_consulta_vec = event['data_consulta_vec']
    unidade_consulta_vec = event['unidade_consulta_vec']
    key_vec = event['key_vec']
    formatted_datetime_vec = event['formatted_datetime_vec']
    id_campanha_vec = event['id_campanha_vec']
    banco_de_dados_vec = event['banco_de_dados_vec']
    id_resultado_vec = event['id_resultado_vec']
    preco_vec = event['preco_vec']
    hsm_vec = event['hsm_vec']
    id_flag_vec = event['id_flag_vec']
    genero_vec = event['genero_vec']
    indicado_vec = event['indicado_vec']
    hsm_secundario_vec = [None if x == 'None' else x for x in event['hsm_secundario_vec']]
    telefone_wpp = event['telefone_wpp']
    forms_link_vec = event["forms_link_vec"]
    id_remarketing_vec = [None if x == 'None' else x for x in event['id_remarketing_vec']] ### REMARKETING ###
    nome_especialista_vec = event["nome_especialista_vec"]
    dor_principal_vec = event["dor_principal_vec"]
    token_cancelamento_vec = event['token_cancelamento_vec']
    token_confirmacao_vec = event['token_confirmacao_vec']
    unidade_federativa_vec = event["unidade_federativa_vec"]
    cidade_vec = event['cidade_vec']
    ativo_vec = event['ativo_vec']
    
    queries = []
    print(str(osFunc("LEMBRETE_1H")), id_flag_vec, str(osFunc("LEMBRETE_1H")) in id_flag_vec) 
    if str(osFunc("LEMBRETE_1H")) in id_flag_vec or osFunc("LEMBRETE_1H") in id_flag_vec:
        print("LEMBRETE_1H")
        # Inicializando vetores de verificação
        check_id_flag_vec = []
        check_formatted_datetime_vec = []
        check_telefone_vec = [] 
        check_id_campanha_vec = []
        check_datetime_flag = []
        i = 0
        len_init = len(telefone_vec)
        for index in range(len_init):
            resp, query, datetime_conv = direto_lembrete_1h(telefone_vec[i], forms_link_vec[i])  
            print("RESP", resp, "QUERY", query) 
            if resp: 
                if query is not None:
                    queries.append(query)  
                    check_id_flag_vec.append(id_flag_vec[i])
                    check_formatted_datetime_vec.append(datetime_conv)
                    check_telefone_vec.append(telefone_vec[i])
                    check_id_campanha_vec.append(id_campanha_vec[i])
                    check_datetime_flag.append(formatted_datetime_flag)
                telefone_vec.pop(i)
                nome_vec.pop(i)
                horario_consulta_vec.pop(i)
                data_consulta_vec.pop(i)
                unidade_consulta_vec.pop(i)
                key_vec.pop(i)
                formatted_datetime_vec.pop(i)
                id_campanha_vec.pop(i)
                banco_de_dados_vec.pop(i)
                preco_vec.pop(i)
                id_flag_vec.pop(i)
                genero_vec.pop(i)
                indicado_vec.pop(i)
                forms_link_vec.pop(i)
                hsm_secundario_vec.pop(i)
                hsm_vec.pop(i)
                ativo_vec.pop(i)
                token_cancelamento_vec.pop(i)
                token_confirmacao_vec.pop(i)
            else:
                i = i + 1
        ############## Insert HIST FLAG ####################
        if len(check_telefone_vec)>0:
            values_list = ", ".join([f"('{formatted_datetime}', '{telefone}', '{id_campanha}', '{datetime_flag}', '{id_flag}')" for formatted_datetime, telefone, id_campanha, id_flag, datetime_flag in zip(check_formatted_datetime_vec, check_telefone_vec, check_id_campanha_vec, check_id_flag_vec, check_datetime_flag)])
            query_flag = f"""
                INSERT INTO FLAG_HISTORICO (DATETIME_CONVERSA, TELEFONE, ID_CAMPANHA, DATETIME_FLAG, ID_FLAG) 
                VALUES {values_list}
                ON DUPLICATE KEY UPDATE DATETIME_CONVERSA = VALUES(DATETIME_CONVERSA), TELEFONE = VALUES(TELEFONE), ID_CAMPANHA = VALUES(ID_CAMPANHA), DATETIME_FLAG = VALUES(DATETIME_FLAG), ID_FLAG = VALUES(ID_FLAG);
            """    
            queries.append(query_flag)
    
    if len(telefone_vec)>0:
        ############## Insert CONTATO ####################
        data_contato = {
            "TELEFONE" : telefone_vec,
            "NOME_CLIENTE": nome_vec,
            "GENERO": genero_vec
        }
        print("CONTATO:", data_contato)

        values_list = ", ".join([f"('{telefone}', '{nome}', '{genero}')" for telefone, nome, genero in zip(telefone_vec, nome_vec, genero_vec)])
        if int(id_campanha_vec[0]) == osFunc("LEADS_WHATSAPP"):
            query_resultado = f"""
                INSERT IGNORE INTO CONTATO (TELEFONE, NOME_CLIENTE, GENERO) 
                VALUES {values_list}
               ;
                """
        else:
            query_resultado = f"""
                INSERT INTO CONTATO (TELEFONE, NOME_CLIENTE, GENERO) 
                VALUES {values_list}
                ON DUPLICATE KEY UPDATE TELEFONE=VALUES(TELEFONE), NOME_CLIENTE=VALUES(NOME_CLIENTE), GENERO=VALUES(GENERO);
                """
        execute_query_db(query_resultado)
        
        ############## Insert CONVERSA #####################
        
        data_conversa = {
            "DATETIME_CONVERSA":formatted_datetime_vec,
            "TELEFONE":telefone_vec,
            "ID_CAMPANHA": id_campanha_vec,
            "ID_RESULTADO":id_resultado_vec,
            "ID_HSM": hsm_vec,
            "ID_LAST_FLAG": id_flag_vec, 
            "PRECO": 0,
            "BANCO_DE_DADOS": banco_de_dados_vec,
            "ID_HSM_SECUNDARIO": hsm_secundario_vec,
            "ID_REMARKETING": id_remarketing_vec,
            "ATIVO": 0
        }
        print("CONVERSA:",data_conversa)
    
        values_list = ", ".join([
        f"('{formatted_datetime}', '{telefone}', '{id_campanha}', '{id_resultado}', '{hsm}', '{id_flag}', '{preco}', '{banco_de_dados}', '{ativo}', {telefone_wpp}, " +
        (f"'{hsm_secundario}'," if hsm_secundario is not None else 'NULL,') +
        (f"'{link_forms}'," if link_forms is not None else 'NULL,') +
        (f"'{id_remarketing}'" if id_remarketing is not None else 'NULL') +
        ")"
        
        for formatted_datetime, telefone, id_campanha, id_resultado, hsm, id_flag, preco, banco_de_dados, ativo, hsm_secundario, link_forms, id_remarketing
        in zip(formatted_datetime_vec, telefone_vec, id_campanha_vec, id_resultado_vec, hsm_vec, id_flag_vec, preco_vec, banco_de_dados_vec, ativo_vec, hsm_secundario_vec, forms_link_vec, id_remarketing_vec)
        ])
        
        query_resultado = f"""
            INSERT INTO CONVERSA (DATETIME_CONVERSA, TELEFONE, ID_CAMPANHA, ID_RESULTADO, ID_HSM, ID_LAST_FLAG, PRECO, BANCO_DADOS, TELEFONE_WPP, ID_HSM_SECUNDARIO, URL_FORMS, ID_REMARKETING, ATIVO) 
            VALUES {values_list}
            ON DUPLICATE KEY UPDATE DATETIME_CONVERSA = VALUES(DATETIME_CONVERSA), TELEFONE = VALUES(TELEFONE), ID_CAMPANHA = VALUES(ID_CAMPANHA), ID_RESULTADO = VALUES(ID_RESULTADO), ID_HSM = VALUES(ID_HSM), ID_LAST_FLAG = VALUES(ID_LAST_FLAG), PRECO = VALUES(PRECO), BANCO_DADOS = VALUES(BANCO_DADOS), TELEFONE_WPP = VALUES(TELEFONE_WPP), ID_HSM_SECUNDARIO = VALUES(ID_HSM_SECUNDARIO), URL_FORMS = VALUES(URL_FORMS), ID_REMARKETING = VALUES(ID_REMARKETING), ATIVO = VALUES(ATIVO) ; 
        """

        execute_query_db(query_resultado)

        if int(id_campanha_vec[0]) == osFunc("INDIQUE_AMIGOS"):
            ############# Insert AMIGO ####################
            data_contato = {
                "TELEFONE" : [telefone_vec],
                "NOME_AMIGO": [indicado_vec]
            }
            print("NOME_AMIGO:", data_contato)

            values_list = ", ".join([f"('{telefone}', '{indicado}')" for telefone, indicado in zip(telefone_vec, indicado_vec)])
            query_resultado = f"""
            INSERT INTO CAMP_INDICADOR (TELEFONE, NOME_AMIGO) 
            VALUES {values_list}
            ON DUPLICATE KEY UPDATE TELEFONE=VALUES(TELEFONE), NOME_AMIGO=VALUES(NOME_AMIGO);
            """
            execute_query_db(query_resultado)    
        
        elif int(id_campanha_vec[0]) == osFunc("LEADS_WHATSAPP"):
            pass
        
        elif int(id_campanha_vec[0]) == osFunc("REMARKETING"):
            data_remarketing = {
                "TELEFONE" : [telefone_vec],
                "NOME_ESPECIALITA": [nome_especialista_vec],
                "DOR_PRINCIPAL": [dor_principal_vec]

            }
            print("REMARKETING:", data_remarketing)

            values_list = ", ".join([f"('{telefone}', '{nome_especialista}', '{dor_principal}')" for telefone, nome_especialista, dor_principal in zip(telefone_vec, nome_especialista_vec, dor_principal_vec)])
            query_resultado = f"""
            INSERT INTO CAMP_REMARKETING (TELEFONE, NOME_ESPECIALISTA, DOR_PRINCIPAL) 
            VALUES {values_list}
            ON DUPLICATE KEY UPDATE TELEFONE=VALUES(TELEFONE), NOME_ESPECIALISTA=VALUES(NOME_ESPECIALISTA), DOR_PRINCIPAL=VALUES(DOR_PRINCIPAL);
            """
            execute_query_db(query_resultado)
            
            ############## Insert AGENDAMENTO ####################
            data_agendamento = {
                "TELEFONE":telefone_vec,
                "ID_AGENDAMENTO":key_vec,
                "DATA": data_consulta_vec,
                "UNIDADE_PSD":unidade_consulta_vec,
                "HORARIO":horario_consulta_vec
            }  
            print("AGENDAMENTO:",data_agendamento)

            values_list = ", ".join([f"('{telefone}', '{key}', '{data_consulta}', '{unidade_consulta}', '{horario_consulta}')" for telefone, key, data_consulta, unidade_consulta, horario_consulta in zip(telefone_vec, key_vec, data_consulta_vec, unidade_consulta_vec, horario_consulta_vec)])
            query_resultado = f"""
                INSERT INTO CAMP_AGENDAMENTO (TELEFONE, ID_AGENDAMENTO, DATA, UNIDADE_PSD, HORARIO) 
                VALUES {values_list}
                ON DUPLICATE KEY UPDATE TELEFONE = VALUES(TELEFONE), ID_AGENDAMENTO = VALUES(ID_AGENDAMENTO), DATA = VALUES(DATA), UNIDADE_PSD = VALUES(UNIDADE_PSD), HORARIO = VALUES(HORARIO);
            """
            execute_query_db(query_resultado)
            
        elif int(id_campanha_vec[0]) == osFunc("LEADS_META"):
            data_leads_meta = {
                "TELEFONE" : [telefone_vec],
                "ESTADO": [unidade_federativa_vec],
                "CIDADE": [cidade_vec]
            }
            print("LEADS_META:", data_leads_meta)

            values_list = ", ".join([f"('{telefone}', '{unidade_federativa}', '{cidade}')" for telefone, unidade_federativa, cidade  in zip(telefone_vec, unidade_federativa_vec, cidade_vec)])
            query_resultado = f"""
            INSERT INTO CAMP_LEADS_META (TELEFONE, ESTADO, CIDADE) 
            VALUES {values_list}
            ON DUPLICATE KEY UPDATE TELEFONE=VALUES(TELEFONE), ESTADO=VALUES(ESTADO), CIDADE=VALUES(CIDADE);
            """
            execute_query_db(query_resultado)
        
        else:
            ############## Insert AGENDAMENTO ####################

            if int(id_campanha_vec[0]) in [osFunc("CONFIRMACAO_2H"), osFunc("CONFIRMACAO")]:
                data_agendamento = {
                    "ID_AGENDAMENTO":key_vec,
                    "TOKEN_CONFIRMACAO":token_confirmacao_vec,
                    "TOKEN_CANCELAMENTO":token_cancelamento_vec,
                    "TELEFONE":telefone_vec,
                    "NOME":nome_vec,
                    "AGENDAMENTO_BOT":0,
                    "DATA": data_consulta_vec,
                    "UNIDADE_PSD":unidade_consulta_vec,
                    "HORARIO":horario_consulta_vec 
                }  
                print("AGENDAMENTO_REALIZADO:",data_agendamento)

                values_list = ", ".join([f"('{telefone}', '{key}', '{data_consulta}', '{unidade_consulta}', '{horario_consulta}', 0 ,'{token_confirmacao}', '{token_cancelamento}', '{nome}')" for telefone, key, data_consulta, unidade_consulta, horario_consulta, token_confirmacao, token_cancelamento, nome in zip(telefone_vec, key_vec, data_consulta_vec, unidade_consulta_vec, horario_consulta_vec, token_confirmacao_vec, token_cancelamento_vec, nome_vec)])
                query_resultado = f"""
                    INSERT INTO AGENDAMENTO_REALIZADO (TELEFONE, ID_AGENDAMENTO, DATA, UNIDADE_PSD, HORARIO, AGENDAMENTO_BOT, TOKEN_CONFIRMACAO,TOKEN_CANCELAMENTO, NOME) 
                    VALUES {values_list}
                    ON DUPLICATE KEY UPDATE TELEFONE = VALUES(TELEFONE), ID_AGENDAMENTO = VALUES(ID_AGENDAMENTO), DATA = VALUES(DATA), UNIDADE_PSD = VALUES(UNIDADE_PSD), HORARIO = VALUES(HORARIO), TOKEN_CONFIRMACAO = VALUES(TOKEN_CONFIRMACAO), TOKEN_CANCELAMENTO =  VALUES(TOKEN_CANCELAMENTO), NOME = VALUES(NOME);
                """
                execute_query_db(query_resultado)
 
            data_agendamento = {
                "TELEFONE":telefone_vec,
                "ID_AGENDAMENTO":key_vec,
                "DATA": data_consulta_vec,
                "UNIDADE_PSD":unidade_consulta_vec,
                "HORARIO":horario_consulta_vec
            }  
            print("CAMP_AGENDAMENTO:",data_agendamento) 

            values_list = ", ".join([f"('{telefone}', '{key}', '{data_consulta}', '{unidade_consulta}', '{horario_consulta}')" for telefone, key, data_consulta, unidade_consulta, horario_consulta in zip(telefone_vec, key_vec, data_consulta_vec, unidade_consulta_vec, horario_consulta_vec)])
            query_resultado = f"""
                INSERT INTO CAMP_AGENDAMENTO (TELEFONE, ID_AGENDAMENTO, DATA, UNIDADE_PSD, HORARIO) 
                VALUES {values_list}
                ON DUPLICATE KEY UPDATE TELEFONE = VALUES(TELEFONE), ID_AGENDAMENTO = VALUES(ID_AGENDAMENTO), DATA = VALUES(DATA), UNIDADE_PSD = VALUES(UNIDADE_PSD), HORARIO = VALUES(HORARIO);
            """
            execute_query_db(query_resultado)

        ############## Insert HIST FLAG ####################
        data_flag_historico = {
            "DATETIME_CONVERSA":formatted_datetime_vec,
            "DATETIME_FLAG":formatted_datetime_vec,
            "TELEFONE": telefone_vec,
            "ID_CAMPANHA": id_campanha_vec,
            "ID_FLAG":id_flag_vec,
        }
        print("FLAG_HISTORICO:",data_flag_historico)

        values_list = ", ".join([f"('{formatted_datetime}', '{telefone}', '{id_campanha}', '{formatted_datetime}', '{id_flag}')" for formatted_datetime, telefone, id_campanha, id_flag in zip(formatted_datetime_vec, telefone_vec, id_campanha_vec, id_flag_vec)])
        query_resultado = f"""
            INSERT INTO FLAG_HISTORICO (DATETIME_CONVERSA, TELEFONE, ID_CAMPANHA, DATETIME_FLAG, ID_FLAG) 
            VALUES {values_list}
            ON DUPLICATE KEY UPDATE DATETIME_CONVERSA = VALUES(DATETIME_CONVERSA), TELEFONE = VALUES(TELEFONE), ID_CAMPANHA = VALUES(ID_CAMPANHA), DATETIME_FLAG = VALUES(DATETIME_FLAG), ID_FLAG = VALUES(ID_FLAG);
        """    
        execute_query_db(query_resultado)

    if len(queries)>0:
        print("EXECUTANDO QUERIES")
        print(queries)
        execute_queries_db(queries)
    end_time = time.time()
    print(f"Tempo total para adicionar {len(telefone_vec)} clientes: {end_time - start_time} s")

    return {
        'statusCode': 200,
        'body': json.dumps('adicionaClientesBatch finalizada')
    } 

def direto_lembrete_1h(telefone, url_forms):
    datetime_conv = None 
    query = None
    # Função para checar a última conversa de um cliente
    def check_ultima_conversa_cliente(telefone):
        # Query para buscar a última conversa do cliente
        query = f"""
        SELECT *
        FROM CONVERSA
        WHERE TELEFONE = {telefone}
        ORDER BY DATETIME_CONVERSA DESC
        LIMIT 1
        """
        # Retorna o DataFrame com o resultado da consulta
        return query_to_dataframe(query)

    # Função para atualizar a flag do cliente
    def atualizar_flag_cliente(telefone, datetime_conv, id_campanha, nova_flag, url_forms):
        # Query para atualizar a flag
        query = f"""
        UPDATE CONVERSA 
        SET ID_LAST_FLAG = {nova_flag}, URL_FORMS = '{url_forms}'
        WHERE TELEFONE = {telefone} AND DATETIME_CONVERSA = '{datetime_conv}' AND ID_CAMPANHA = {id_campanha} 
        LIMIT 1
        """
        return query

    # Definição das constantes das flags
    FLAG_LEMBRETE_1H = osFunc('LEMBRETE_1H')
    ULTIMA_ID_RESULTADO_CHECK = osFunc('SEM_INTERACAO')
    CONFIRMACAO_2H = osFunc('CONFIRMACAO_2H')

    # Checa a última conversa do cliente
    ultima_conversa_df = check_ultima_conversa_cliente(telefone)
    print('check_info:', ultima_conversa_df is not None and not ultima_conversa_df.empty, ultima_conversa_df)

    # Verifica se o resultado foi obtido e se a última flag é de confirmação de 2 horas
    if ultima_conversa_df is not None and not ultima_conversa_df.empty:
        ultima_flag = ultima_conversa_df.iloc[0]['ID_LAST_FLAG']
        id_resultado =  ultima_conversa_df.iloc[0]['ID_RESULTADO']
        datetime_conv =  ultima_conversa_df.iloc[0]['DATETIME_CONVERSA']
        telefone =  ultima_conversa_df.iloc[0]['TELEFONE']
        id_campanha =  ultima_conversa_df.iloc[0]['ID_CAMPANHA']
        print('check_info:',id_resultado == ULTIMA_ID_RESULTADO_CHECK, id_resultado)
        if id_resultado == ULTIMA_ID_RESULTADO_CHECK and id_campanha == CONFIRMACAO_2H:
            # Se a última flag é de confirmação de 2 horas, atualiza para lembrete de 1 hora
            print("TCampanha = 14 e resultado = 1:", telefone)
            query = atualizar_flag_cliente(telefone, datetime_conv, id_campanha, FLAG_LEMBRETE_1H, url_forms)
            return True, query, datetime_conv
        elif id_resultado != ULTIMA_ID_RESULTADO_CHECK and id_campanha == CONFIRMACAO_2H:
            print("Tem conversa com resultado diferente de 1 :", telefone)
            return True, query, datetime_conv
        elif id_campanha != CONFIRMACAO_2H:
            print("Tem conversa em outra campanha :", telefone)
            return False, query, datetime_conv
        else:
             return False, query, datetime_conv
            
    else:
        print("Nenhuma conversa encontrada ou falha na consulta para o telefone:", telefone)
        return False, query, datetime_conv

 