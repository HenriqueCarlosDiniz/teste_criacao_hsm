from utils.connect_db import insert_data_into_db, query_to_dataframe, execute_query_db
import pandas as pd
from utils.config import osFunc
from utils.send_wpp import enviar_payload, enviar_wpp
import pytz
from datetime import datetime
from Models.contato import Contato
from Models.conversa import Conversa
if osFunc('Ambiente_Lambda'):
    from message import get_user_message, is_user_message
else:
    from recebeMensagem.message import get_user_message, is_user_message 
import json
 

def cadastrar_receptivo(id_contato, body_json, id_pagina, header):
    # primeira_msg = "Olá! Bem vindo ao atendimento da Pés sem Dor! Poderia me passar seu nome completo e a cidade de onde está falando?"
    # if campanha_hard != 'RECEPTIVO':
    #     primeira_msg = "Olá! Bem vindo ao atendimento da Pés sem Dor! Nós Atendemos em todos os estados do Brasil! Qual a sua duvida referente aos calçados?"
    current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')) 
    formatted_datetime_conversa = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    response2Client = {'type': None, 'content': None}
    id_flag = osFunc("RESPONDER")
    id_resultado = osFunc("RESPONDIDO")
    message_text = "MÍDIA"
    
    if is_user_message(body_json):
        message_text, id_mensagem = get_user_message(body_json)
        if message_text == False:
            message_text = "Emoji"
    
    id_campanha = get_campanha_receptivo(message_text) ### DEFINIR A CAMPANHA RECEPTIVO IG OU FB
    
    datetime_conversa_check = checar_cadastra_anterior(id_contato, id_campanha)
    if datetime_conversa_check:
        current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')) 
        formatted_datetime_conversa = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        formatted_datetime_send = current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
        
        # insert_data_into_db(pd.DataFrame({
        #                                     'DATETIME_CONVERSA' : [formatted_datetime_conversa],
        #                                     'TELEFONE' : [telefone],
        #                                     'ID_CAMPANHA' : [id_campanha],
        #                                     'ID_RESULTADO' : [id_resultado],
        #                                     "ID_LAST_FLAG": [id_flag],
        #                                     'TELEFONE_WPP': [telefone_wpp]
        #                                     }), 'CONVERSA')
                                            
        query_conversa = f"""INSERT IGNORE INTO OMNI_CONVERSA (ID_CONTATO, DATETIME_CONVERSA, ID_CAMPANHA, ID_RESULTADO, ID_LAST_FLAG) 
                             VALUES ('{id_contato}', '{formatted_datetime_conversa}', '{id_campanha}', '{id_resultado}', '{id_flag}');"""

        execute_query_db(query_conversa)
        
        inputMessage = {
                            'ID_CONTATO': [id_contato],
                            'DATETIME_CONVERSA': [formatted_datetime_conversa],
                            'ID_CAMPANHA': [id_campanha],
                            'MENSAGEM_DATETIME': [formatted_datetime_send],
                            'CONTEUDO': [message_text],
                            'AUTOR_ID_AUTOR': [1],
                            'CUSTO': [0],
                            # 'ID_MENSAGEM' : id_mensagem
            } 
        
        insert_data_into_db(pd.DataFrame(inputMessage), 'OMNI_MENSAGEM')
    else:
        nome_cliente = "receptivo"
        
        query_contato = f"""INSERT INTO OMNI_CONTATO(ID_CONTATO, NOME_CLIENTE)
                            VALUES ({id_contato}, '{nome_cliente}')
                            AS new_contato
                            ON DUPLICATE KEY UPDATE ID_CONTATO=new_contato.ID_CONTATO;"""

        execute_query_db(query_contato)
        
        query_conversa = f"""INSERT IGNORE INTO OMNI_CONVERSA (ID_CONTATO, DATETIME_CONVERSA, ID_CAMPANHA, ID_RESULTADO, ID_LAST_FLAG) 
                             VALUES ('{id_contato}', '{formatted_datetime_conversa}', '{id_campanha}', '{id_resultado}', '{id_flag}');"""

        execute_query_db(query_conversa)
        
        current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')) 
        formatted_datetime_send = current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
        
        inputMessage = {
                            'ID_CONTATO': [id_contato],
                            'DATETIME_CONVERSA': [formatted_datetime_conversa],
                            'ID_CAMPANHA': [id_campanha],
                            'MENSAGEM_DATETIME': [formatted_datetime_send],
                            'CONTEUDO': [message_text],
                            'AUTOR_ID_AUTOR': [1],
                            'CUSTO': [0],
                            # 'ID_MENSAGEM' : id_mensagem
            } 
        
        insert_data_into_db(pd.DataFrame(inputMessage), 'OMNI_MENSAGEM')
    
    current_datetime_send = datetime.now(pytz.timezone('America/Sao_Paulo')) 
    formatted_datetime_send = current_datetime_send.strftime("%Y-%m-%d %H:%M:%S.%f")
    df_interactive_receptivo = query_to_dataframe(f"SELECT CONTEUDO_RESPOSTA, CONTEUDO_DB, TIPO_SAIDA FROM MENSAGENS_ESTATICAS WHERE ID_CAMPANHA = {id_campanha} AND TIPO_ENTRADA = \'DIRECT\'")
    print(f"query receptivo: SELECT CONTEUDO_RESPOSTA, CONTEUDO_DB, TIPO_SAIDA FROM MENSAGENS_ESTATICAS WHERE ID_CAMPANHA = {id_campanha} AND TIPO_ENTRADA = \'DIRECT\'\n", df_interactive_receptivo)
    primeira_mensagem = df_interactive_receptivo['CONTEUDO_DB'].iloc[0]
    # if int(telefone)<1000000:
    #     true_telefone = query_to_dataframe(f'SELECT * FROM FAKE_TELEFONE WHERE FAKE_TELEFONE = {telefone}')['TELEFONE'].iloc[0]
    #     response2Client = {'type': df_interactive_receptivo['TIPO_SAIDA'].iloc[0], 'content': df_interactive_receptivo['CONTEUDO_RESPOSTA'].iloc[0].replace("{telefone}", str(true_telefone))}
    # else:
    response2Client = {'type': df_interactive_receptivo['TIPO_SAIDA'].iloc[0], 'content': df_interactive_receptivo['CONTEUDO_RESPOSTA'].iloc[0].replace("{telefone}", str(id_contato))}
        
    if osFunc('Ambiente_Lambda'): 
        ret_envio = enviar_dm_receptivo(id_contato, response2Client, header) ### TRATAR RETORNO  
        print(f'RETORNO_WPP_RECEPTIVO: ',ret_envio)
    
    data_resposta_bot = {
                'AUTOR_ID_AUTOR': [0],
                'CONTEUDO': [primeira_mensagem],
                'CUSTO': [0],
                'ID_CAMPANHA': [id_campanha],
                "DATETIME_CONVERSA": [formatted_datetime_conversa],
                "MENSAGEM_DATETIME": [formatted_datetime_send],
                "ID_CONTATO": [id_contato]
    }
    
    insert_data_into_db(pd.DataFrame(data_resposta_bot), "OMNI_MENSAGEM" )
    
    contato_receptivo = Contato(id_contato)
    conversa_receptivo = Conversa(contato_receptivo, id_campanha, datetime_conversa = formatted_datetime_conversa)
    current_datetime_flag = datetime.now(pytz.timezone('America/Sao_Paulo')) 
    formatted_datetime_flag = current_datetime_flag.strftime("%Y-%m-%d %H:%M:%S")
    conversa_receptivo.set_flag("AGUARDAR", formatted_datetime_flag)
    conversa_receptivo.set_resultado("RESPONDIDO")
 
    return response2Client
    

def get_campanha_receptivo(message):
    # Carregar as tabelas inteiras em DataFrames
    query_mensagem = "SELECT * FROM RECEPTIVO_MENSAGEM"
    df_mensagem = query_to_dataframe(query_mensagem)
    print(df_mensagem)
    
    # Procurar a mensagem na tabela RECEPTIVO_MENSAGEM
    df_msg = df_mensagem[df_mensagem['PRIMEIRA_MENSAGEM'] == message]
    if not df_msg.empty:
        id_campanha = df_msg['ID_CAMPANHA'].iloc[0]
        print("## TESTE ##", id_campanha)
        return id_campanha
    # mudar para campanha padrão do facebook/insta
    return osFunc("RECEPTIVO")

def enviar_dm_receptivo(id_contato, response2Client, header):
    id_mensagem_response = 'None'
    # if int(telefone)<1000000:
    #     telefone = query_to_dataframe(f'SELECT * FROM FAKE_TELEFONE WHERE FAKE_TELEFONE = {telefone}')['TELEFONE'].iloc[0]
        
    if response2Client['type'] == 'text':
        response = enviar_wpp(id_contato, response2Client['content'], header = header)

    elif response2Client['type'] == 'interactive':
        response = enviar_payload(json.loads(response2Client['content']), header = header)
    
    

    return id_mensagem_response

    
def checar_cadastra_anterior(id_contato, id_campanha):
    query_contato = f"SELECT * FROM OMNI_CONTATO WHERE ID_CONTATO = {id_contato};"
    query_conversa = f"SELECT * FROM OMNI_CONVERSA WHERE ID_CONTATO = {id_contato} AND ID_CAMPANHA = {id_campanha} AND ID_RESULTADO != {osFunc('ADICIONADO')} ORDER BY DATETIME_CONVERSA DESC LIMIT 1;"
    
    df_contato = query_to_dataframe(query_contato)
    
    if not df_contato.empty:
        df_conversa = query_to_dataframe(query_conversa)
        if df_conversa.empty:
            datetime_conversa = False
        else:
            datetime_conversa = df_conversa["DATETIME_CONVERSA"][0]
        return datetime_conversa
    else:
        return False
    
    
        
        
    
    