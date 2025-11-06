from utils.config import *
from utils.connect_db import query_to_dataframe, insert_data_into_db
from utils.api_communication import POST_cancelar_agendamento
from datetime import datetime, timedelta
import pytz
import pandas as pd
from Models.contato import Contato
from Models.conversa import Conversa
import json, re, requests

from utils.send_wpp import enviar_payload, enviar_wpp, enviar_HSM
from utils.text import format_data_feira


def create_fake_number(telefone):
    initial_val = query_to_dataframe(f"SELECT * FROM FAKE_TELEFONE ORDER BY FAKE_TELEFONE DESC LIMIT 1;")['FAKE_TELEFONE'][0]
    current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')) 
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    fake_telefone = int(initial_val)+1
    insert_data_into_db(pd.DataFrame({
                                        'TELEFONE' : [telefone],
                                        'FAKE_TELEFONE' : [fake_telefone],
                                        'DATETIME_CREATED' : [formatted_datetime]
                                        }), 'FAKE_TELEFONE')

def get_true_number(fake_telefone):
    df_fake = query_to_dataframe(f"SELECT * FROM FAKE_TELEFONE WHERE FAKE_TELEFONE = {fake_telefone};")
    return df_fake['TELEFONE'].iloc[0]

def get_fake_number(telefone):
    df_fake = query_to_dataframe(f"SELECT * FROM FAKE_TELEFONE WHERE TELEFONE = {telefone} ORDER BY DATETIME_CREATED DESC LIMIT 1;")
    return df_fake['FAKE_TELEFONE'].iloc[0]

def reset_comum(telefone, id_campanha, telefone_wpp, header, novo_id_campanha = None, banco_dados=0):
    response2Client = {'type': None, 'content': None}
    contato_reset = Contato(telefone)
    print('contato_reset.agendamento.data_agendamento:', contato_reset.agendamento.data_agendamento, telefone) 
    conversa_reset = Conversa(contato_reset, id_campanha) 
    current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')) 
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

    
    if not campanha_tem_agendamento(id_campanha) and contato_reset.agendamento.get_dados_agendamentoAPI() and datetime.strptime(contato_reset.agendamento.data_agendamento, '%Y-%m-%d').date() >= datetime.now(pytz.timezone('America/Sao_Paulo')).date(): #Cliente já possui agendamento no futuro
        POST_cancelar_agendamento(contato_reset.agendamento.token_cancelamento, id_campanha=id_campanha, banco_dados=banco_dados)

    id_flag = osFunc("HSM_ENVIADO")
    id_resultado = osFunc("SEM_INTERACAO")
    if novo_id_campanha is not None:
        id_campanha = novo_id_campanha
    

    insert_data_into_db(pd.DataFrame({
                                'DATETIME_CONVERSA' : [formatted_datetime],
                                'TELEFONE' : [telefone],
                                'ID_CAMPANHA' : [id_campanha],
                                'ID_RESULTADO' : [id_resultado],
                                'ID_LAST_FLAG':[id_flag],
                                'ID_HSM': [conversa_reset.HSM],
                                'TELEFONE_WPP': [telefone_wpp]
                                }), 'CONVERSA')
    datetime_conversa = formatted_datetime 

    df_HSM = query_to_dataframe(f'''SELECT * FROM HSM WHERE ID_HSM = "{conversa_reset.HSM}"''')

    hsm, payload = get_hsm_payload(df_HSM, contato_reset, telefone, id_campanha)
    response2Client['content'] = hsm 
    response2Client['type'] = 'text'
    
    if osFunc('Ambiente_Lambda'):
        ret_hsm = NEW_enviar_HSM(payload = payload, header = header)   
    inputMessage = {
                    'TELEFONE': [telefone], 
                    'ID_CAMPANHA': [id_campanha],
                    'DATETIME_CONVERSA': [datetime_conversa],
                    'MENSAGEM_DATETIME': [formatted_datetime],
                    'CONTEUDO': [hsm],
                    'CUSTO': [0],
                    'AUTOR_ID_AUTOR': [0],
    } 
    
    insert_data_into_db(pd.DataFrame(inputMessage), 'MENSAGEM')  
    insert_data_into_db(pd.DataFrame({
                                'DATETIME_CONVERSA' : [formatted_datetime],
                                'TELEFONE' : [telefone],
                                'ID_CAMPANHA' : [id_campanha],
                                'DATETIME_FLAG' : [formatted_datetime],
                                'ID_FLAG':[id_flag],
                                }), 'FLAG_HISTORICO')
    return response2Client
    
def reset_comum_sem_hsm(telefone, id_campanha, telefone_wpp, header, banco_dados=0):
    response2Client = {'type': None, 'content': None}
    contato_reset = Contato(telefone)
    # conversa_reset = Conversa(contato_reset, id_campanha)
    current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')) 
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

    
    if not campanha_tem_agendamento(id_campanha) and contato_reset.agendamento.get_dados_agendamentoAPI() and datetime.strptime(contato_reset.agendamento.data_agendamento, '%Y-%m-%d').date() >= datetime.now(pytz.timezone('America/Sao_Paulo')).date(): #Cliente já possui agendamento no futuro
        POST_cancelar_agendamento(contato_reset.agendamento.token_cancelamento, id_campanha=id_campanha, banco_dados=banco_dados)

    id_flag = osFunc("HSM_ENVIADO")
    id_resultado = osFunc("SEM_INTERACAO")
    
    df_HSM = query_to_dataframe(f'''SELECT ID_HSM, CONTEUDO, MESSAGE_PAYLOAD FROM HSM WHERE ID_CAMPANHA = {id_campanha} AND ATIVO = 1 LIMIT 1;''')
    
    insert_data_into_db(pd.DataFrame({
                                'DATETIME_CONVERSA' : [formatted_datetime],
                                'TELEFONE' : [telefone],
                                'ID_CAMPANHA' : [id_campanha],
                                'ID_RESULTADO' : [id_resultado],
                                'ID_LAST_FLAG':[id_flag],
                                'ID_HSM': [df_HSM['ID_HSM'].iloc[0]],
                                'TELEFONE_WPP': [telefone_wpp]
                                }), 'CONVERSA')
    
    datetime_conversa = formatted_datetime
    hsm, payload = get_hsm_payload(df_HSM, contato_reset, telefone, id_campanha)
    
    response2Client['content'] = hsm 
    response2Client['type'] = 'text'
    
    if osFunc('Ambiente_Lambda'):
        ret_hsm = NEW_enviar_HSM(payload = payload, header = header)
    
    inputMessage = {
                    'TELEFONE': [telefone], 
                    'ID_CAMPANHA': [id_campanha],
                    'DATETIME_CONVERSA': [datetime_conversa],
                    'MENSAGEM_DATETIME': [formatted_datetime],
                    'CONTEUDO': [hsm],
                    'CUSTO': [0],
                    'AUTOR_ID_AUTOR': [0],
    } 
    
    insert_data_into_db(pd.DataFrame(inputMessage), 'MENSAGEM')  
    insert_data_into_db(pd.DataFrame({
                                'DATETIME_CONVERSA' : [formatted_datetime],
                                'TELEFONE' : [telefone],
                                'ID_CAMPANHA' : [id_campanha],
                                'DATETIME_FLAG' : [formatted_datetime],
                                'ID_FLAG':[id_flag],
                                }), 'FLAG_HISTORICO')
    return response2Client

def reset_receptivo(conversa, id_campanha, telefone_wpp, header):
    # if int(telefone)<1000000:
    #     telefone = query_to_dataframe(f'SELECT * FROM FAKE_TELEFONE WHERE FAKE_TELEFONE = {telefone}')['TELEFONE'].iloc[0]
    # create_fake_number(telefone)
    current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')) 
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    insert_data_into_db(pd.DataFrame({
                                'DATETIME_CONVERSA' : [formatted_datetime],
                                'TELEFONE' : [conversa.contato.id_contato], 
                                'ID_CAMPANHA' : [id_campanha],
                                'ID_RESULTADO' : [1],
                                'ID_LAST_FLAG':[0],
                                'ID_HSM': [None],
                                'TELEFONE_WPP': [telefone_wpp]
                                }), 'CONVERSA')  
    conversa.contato.update_nome_contato('receptivo')  
    response = enviar_wpp(conversa.contato.id_contato, 'Teste Receptivo pode ser reiniciado, envie alguma mensagem', header = header)
    return {'type': None, 'content': 'Teste Receptivo pode ser reiniciado, envie alguma mensagem'}
   
    ### Enviar buttons

def reset_conversa(telefone, id_campanha, telefone_wpp, header, novo_id_campanha = None, banco_dados=0):
    contato = Contato(telefone)
    conversa = Conversa(contato, id_campanha)
    print(f'ID_CAMPANHA: {id_campanha}; NOVO_ID_CAMPANHA: {novo_id_campanha}')
    
    if campanha_is_receptivo(id_campanha):   
        if novo_id_campanha is None:
            print("RESET RECEPTIVO 1")
            response2Client = reset_receptivo(conversa, id_campanha, telefone_wpp, header)
        else:
            if campanha_is_receptivo(novo_id_campanha):
                print("RESET RECEPTIVO 2")
                response2Client = reset_receptivo(conversa, novo_id_campanha, telefone_wpp, header)
            else:
                print("RESET COMUM SEM HSM")
                # if int(telefone)<1000000: 
                #     telefone = query_to_dataframe(f'SELECT * FROM FAKE_TELEFONE WHERE FAKE_TELEFONE = {telefone}')['TELEFONE'].iloc[0]
                response2Client = reset_comum_sem_hsm(telefone, novo_id_campanha, telefone_wpp, header, banco_dados=banco_dados)    

    else:
        if novo_id_campanha is not None:
            if campanha_is_receptivo(novo_id_campanha):
                print("RESET RECEPTIVO 3")
                response2Client = reset_receptivo(conversa, novo_id_campanha, telefone_wpp, header)
            else: 
                if novo_id_campanha == id_campanha:
                    print("RESET COMUM 1")
                    response2Client = reset_comum(telefone, id_campanha, telefone_wpp, header, novo_id_campanha, banco_dados=banco_dados)
                else:
                    print("RESET COMUM 2")
                    response2Client = reset_comum_sem_hsm(telefone, novo_id_campanha, telefone_wpp, header, banco_dados=banco_dados)
        else:
            print("RESET COMUM 3")
            response2Client = reset_comum(telefone, id_campanha, telefone_wpp, header, banco_dados=banco_dados)
    return response2Client

def campanha_is_receptivo(id_campanha): ##### FAZER COM DB
    if int(id_campanha) in [10,12,13]:
        return True
    else:
        return False
    
def campanha_tem_agendamento(id_campanha): ##### FAZER COM DB
    if id_campanha in [0,1,2,3,14]:
        return True
    else:
        return False
        
def campanha_confirmacao(id_campanha): ##### FAZER COM DB
    if id_campanha in [1,14]:
        return True
    else:
        return False
    
def enviar_wpp_reset(telefone, response2Client, header):
    id_mensagem_response = 'None'
    # if int(telefone)<1000000:
    #     telefone = query_to_dataframe(f'SELECT * FROM FAKE_TELEFONE WHERE FAKE_TELEFONE = {telefone}')['TELEFONE'].iloc[0]

    if response2Client['type'] == 'text':
        response = enviar_wpp(telefone, response2Client['content'], header = header)

    elif response2Client['type'] == 'interactive':
        response = enviar_payload(json.loads(response2Client['content']), header = header)

    return id_mensagem_response
    

def check_reset_presence_and_extract_value(s):
    # Checking for the presence of 'reset', 'Reset', 'reset(X)', and 'Reset(Y)' where X and Y are integers
    pattern = r'\breset\(\d+\)|\bReset\(\d+\)|\breset\b|\bReset\b'
    matches = re.findall(pattern, s)

    # If any form of reset is found, process further to extract values if present
    if matches:
        # Extracting the numeric values from 'reset(X)' and 'Reset(Y)'
        values = [int(x) for x in re.findall(r'\d+', str(matches))]
        return True, values
    else:
        return False, []

def criar_agendamento(telefone, id_campanha):
    print("TELEFONE CRIA AGENDAMENTO:", telefone)
    if campanha_tem_agendamento(id_campanha):
        if campanha_confirmacao(id_campanha):
            current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')) 
            new_datetime = current_datetime + timedelta(days=1)
            formatted_datetime = new_datetime.strftime("%Y-%m-%d %H:%M:%S")
            
        else:
            current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')) 
            new_datetime = current_datetime - timedelta(days=1)
            formatted_datetime = new_datetime.strftime("%Y-%m-%d %H:%M:%S")
        data = formatted_datetime
        horario = '12:00:00'
        unidade_psd = 'PSD'
            
        insert_data_into_db(pd.DataFrame({
                            'TELEFONE' : [telefone],
                            'ID_AGENDAMENTO' : [None],
                            'DATA' : [data],
                            'UNIDADE_PSD' : [unidade_psd],
                            'HORARIO':[horario],
                            }), 'CAMP_AGENDAMENTO')
        return True
    else:
        return False

def NEW_enviar_HSM(http_session = None, payload = None, header = None):
    # Preparando a mensagem para ser enviada
    
    URL_DIALOG_MESSAGE = osFunc("URL_DIALOG_MESSAGE")
    print("payload:",payload)
    try:
        payload = json.loads(payload)
        payload = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    except Exception as e:
        print(f"ERROR(lambda_function.NEW_enviar_HSM): Erro no payload: {e}")
        return 'False'

    # Making the POST request to send the response using the provided HTTP session
    try:
        if http_session:
            response = http_session.post(URL_DIALOG_MESSAGE, data=payload, headers=header)  # Use headers variable
        else:
            response = requests.post(URL_DIALOG_MESSAGE, data=payload, headers=header)
        response_json = json.loads(response.text)
        print("Response:",response.text)

        if 'errors' in response_json.keys():  # Se tiver algum erro, não retornar true
            if response_json['errors'][0]['details'] == "Recipient is not a valid WhatsApp user":
                # Se o número não tiver um WhatsApp vinculado, retornar "Sem Whatsapp"
                return 'Sem Whatsapp'
            return 'False'
        if 'meta' in response_json.keys():  # Se tiver algum erro, não retornar true
            if response_json['meta']['api_status'] == "stable":
                return 'True'

        return 'False'

    except Exception as e:
        print(f"ERROR(lambda_function.NEW_enviar_HSM): Erro no pos na dialog: {e}")
        # Se houver alguma exceção, retornar False
        if 'response' in locals() or 'response' in globals():
            print("Response:",response)
        return 'False'
    
def get_hsm_payload(df_HSM, contato_reset, telefone, id_campanha):
    hsm = df_HSM['CONTEUDO'][0].replace('{cliente}', contato_reset.nome)
    hsm = hsm.replace('{data_agendamento}', datetime.strptime(contato_reset.agendamento.data_agendamento, "%Y-%m-%d").strftime("%d/%m"))
    hsm = hsm.replace('{data_agendamento2}', datetime.strptime(contato_reset.agendamento.data_agendamento, "%Y-%m-%d").strftime("%d/%b/%Y"))
    hsm = hsm.replace('{horario_agendamento}', contato_reset.agendamento.horario_agendamento)
    hsm = hsm.replace('{horario_agendamento2}', datetime.strptime(contato_reset.agendamento.horario_agendamento, "%H:%M").strftime("%Hh%M"))
    hsm = hsm.replace('{unidade_agendamento}', f"Unidade {contato_reset.agendamento.unidade_agendamento.nome}")
    hsm = hsm.replace('{endereco_agendamento}', f"https://pessemdor.link/{contato_reset.agendamento.unidade_agendamento.sigla}")
    hsm = hsm.replace('{endereco_agendamento2}', json.dumps(contato_reset.agendamento.unidade_agendamento.endereco, ensure_ascii=False).replace('\\r',''))
    hsm = hsm.replace('{link_forms}', f"link_forms")
    
    payload = df_HSM['MESSAGE_PAYLOAD'][0]
    payload = payload.replace('{cliente}', str("\""+contato_reset.nome+"\"")) 
    # if int(telefone)<1000000:
    #         telefone_envio = query_to_dataframe(f'SELECT * FROM FAKE_TELEFONE WHERE FAKE_TELEFONE = {telefone}')['TELEFONE'].iloc[0]
    # else:
    telefone_envio = telefone
    payload = payload.replace('{telefone}', str("\""+str(telefone_envio)+"\"")) 
   
    if id_campanha == osFunc("INDIQUE_AMIGOS"):
        nome_amigo = None ################# 
        payload = payload.replace('{amigo}', str("\""+nome_amigo+"\""))
    else:
        unidade_nome = contato_reset.agendamento.unidade_agendamento.nome
        unidade_endereco2 = contato_reset.agendamento.unidade_agendamento.endereco
        unidade_endereco = f"https://pessemdor.link/{contato_reset.agendamento.unidade_agendamento.sigla}"
        data_agendamento = datetime.strptime(contato_reset.agendamento.data_agendamento, "%Y-%m-%d").strftime("%d/%m")
        data_agendamento2 = format_data_feira(datetime.strptime(contato_reset.agendamento.data_agendamento, "%Y-%m-%d"))
        horario = contato_reset.agendamento.horario_agendamento
        horario_agendamento = f"{horario}h"
        horario_agendamento2 = datetime.strptime(horario, "%H:%M").strftime("%Hh%M")
    
        payload = payload.replace('{data_agendamento}', str("\""+data_agendamento+"\"")) 
        payload = payload.replace('{data_agendamento2}', str("\""+data_agendamento2+"\"")) 
        payload = payload.replace('{horario_agendamento}', str("\""+horario_agendamento+"\"")) 
        payload = payload.replace('{horario_agendamento2}', str("\""+horario_agendamento2+"\"")) 
        payload = payload.replace('{unidade_agendamento}', str("\""+unidade_nome+"\"")) 
        payload = payload.replace('{endereco_agendamento}', str("\""+unidade_endereco+"\"")) 
        payload = payload.replace('{endereco_agendamento2}', str("\""+unidade_endereco2+"\""))
        payload = payload.replace('{link_forms}', "\""+ f"link_forms"+"\"")
    return hsm, payload