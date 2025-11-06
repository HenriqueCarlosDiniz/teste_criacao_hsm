import json
from datetime import datetime, timedelta
import requests
import os
import pymysql
import traceback

URL_DIALOG_MESSAGE = os.environ["URL_DIALOG_MESSAGE"]
DB_CONFIG = {
    "user": os.environ.get('DB_USERNAME'),
    "password": os.environ.get('DB_PASSWORD'),
    "host": os.environ.get('DB_HOST'),
    "database": os.environ.get('DB_DATABASE')
}
ID_FLAG = os.environ.get('ON_HOLD', 8)
ID_RESULTADO = os.environ.get('INTERVENCAO', 9)
ID_HSM = os.environ.get('ID_HSM', 'ia_reiniciar_conversa')
ID_HSM_REMARKETING = os.environ.get('ID_HSM_REMARKETING', 'ia_reiniciar_conversa_v2')
REMARKETING = os.environ.get('REMARKETING', 16)
ERROR_UNMAPPED = 'Erro nao mapeado'
ERROR_NO_AUTH = 'No authorization provided'
ERROR_NON_JSON_RESPONSE = 'Resposta da 360dialog veio em formato sem ser json (erro no servidor deles)'
ERROR_BODY = 'Sem body no request'
ERROR_TELEFONE_MENSAGEM = 'Sem telefone ou mensagem no body'

class BadRequestError(Exception):
    def __init__(self, response):
        response = f"Request invalido: {response}"
        super().__init__(response)

class SendMessageError(Exception):
    def __init__(self, response):
        response = f"Erro em enviar mensagem pra dialog: {response}"
        super().__init__(response)

def lambda_handler(event, context):
    print("EVENT:", json.dumps(event))          
    try:
        telefone, mensagem, hsm_bool, metadados = extrair_body(event)
        print('extraiu body')
        cnx = pymysql.connect(**DB_CONFIG)
        cursor = cnx.cursor()
        info_conversa = update_conversa(cursor, telefone)
        print('atualizou conversa')
        id_campanha = info_conversa["ID_CAMPANHA"]
        hsm_info = get_hsm_info(cursor, hsm_bool, id_campanha)
        print('pegou info hsm')
        print('info conv: ', info_conversa)
        print('msg: ', mensagem)
        print('hsm info: ', hsm_info)
        enviar_mensagem(info_conversa, mensagem, hsm_info)
        print('enviou msg')
        salvar_mensagem(cursor, info_conversa, mensagem, metadados, hsm_info)
        print('salvou msg')
        cnx.commit() 
        cursor.close()
        cnx.close()
    except BadRequestError as e:
        traceback.print_exc()
        print(f"BadRequestError: {e}")
        return response(str(e), 400)
    except SendMessageError as e:
        traceback.print_exc()
        print(f"SendMessageError: {e}")
        return response(str(e), 500)
    except Exception as e:
        traceback.print_exc()
        print(f"ExceptionError: {e}")
        return response("Erro interno de servidor", 500)

    return response("Mensagem enviada com sucesso", 200)

def response(message, status_code):
    response = {
        'headers': {
            'Content-Type': 'application/json'
        },
        'statusCode': status_code,
        'body': json.dumps(message)
    }
    print(f"response: {response}")
    return response
    
def extrair_body(event):
    body = event.get('body')
    if not body:
        raise BadRequestError(ERROR_BODY)
    telefone = body.get('telefone')
    hsm_bool = body.get('hsm_bool', False)
    if hsm_bool:
        mensagem = "placeholder"
    else:
        mensagem = body.get('mensagem')
    metadados = body.get('metadados', None)
    if not telefone or not mensagem:
        raise BadRequestError(ERROR_TELEFONE_MENSAGEM)
    return telefone, mensagem, hsm_bool, metadados

def update_conversa(cursor, telefone):
    query_conversa = f'''
        SELECT 
            CONVERSA.DATETIME_CONVERSA,
            CONVERSA.TELEFONE,
            CONVERSA.ID_CAMPANHA,
            CONVERSA.ID_RESULTADO,
            CONVERSA.ID_LAST_FLAG,
            TELEFONE_WPP.TOKEN
        FROM CONVERSA 
        LEFT JOIN TELEFONE_WPP ON TELEFONE_WPP.TELEFONE_WPP = CONVERSA.TELEFONE_WPP
        WHERE TELEFONE = {telefone} AND ID_RESULTADO != 0
        ORDER BY DATETIME_CONVERSA DESC 
        LIMIT 1'''
    cursor.execute(query_conversa)
    result = cursor.fetchone()
    info_conversa = {
        "DATETIME_CONVERSA": result[0],
        "TELEFONE": result[1],
        "ID_CAMPANHA": result[2],
        "ID_RESULTADO": result[3],
        "ID_LAST_FLAG": result[4],
        "TOKEN": result[5]
    }

    set_flag_resultado(cursor, info_conversa)

    return info_conversa

def set_flag_resultado(cursor, info_conversa):
    telefone = info_conversa["TELEFONE"]
    datetime_conversa = info_conversa["DATETIME_CONVERSA"]
    conversa_id_resultado = str(info_conversa["ID_RESULTADO"])
    conversa_id_flag = str(info_conversa["ID_LAST_FLAG"])
    id_campanha = info_conversa["ID_CAMPANHA"]
  
    if conversa_id_flag != ID_FLAG:
        flag_datetime = (datetime.now()- timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")
        

        query_insert_flag = f"""
        INSERT INTO FLAG_HISTORICO(DATETIME_CONVERSA, DATETIME_FLAG, TELEFONE, ID_CAMPANHA, ID_FLAG)
        VALUES('{datetime_conversa}', '{flag_datetime}', '{telefone}', {id_campanha}, {ID_FLAG})"""
        cursor.execute(query_insert_flag)

        query_update_flag = f'''
            UPDATE CONVERSA SET 
            ID_LAST_FLAG = {ID_FLAG} 
            WHERE TELEFONE = '{telefone}' AND DATETIME_CONVERSA = '{datetime_conversa}'
            LIMIT 1;'''
        cursor.execute(query_update_flag)
        
    if conversa_id_resultado != ID_RESULTADO and int(id_campanha) != int(REMARKETING):
        query_update_resultado = f'''
            UPDATE CONVERSA SET 
            ID_RESULTADO = {ID_RESULTADO}
            WHERE TELEFONE = '{telefone}' AND DATETIME_CONVERSA = '{datetime_conversa}'
            LIMIT 1;'''
        cursor.execute(query_update_resultado)

def get_hsm_info(cursor, hsm_bool, id_campanha):
    info_hsm = None
    if hsm_bool:
        if int(id_campanha) == int(REMARKETING):
            query_hsm = f"SELECT CONTEUDO, MESSAGE_PAYLOAD FROM HSM WHERE ID_HSM = '{ID_HSM_REMARKETING}'"
        else:
            query_hsm = f"SELECT CONTEUDO, MESSAGE_PAYLOAD FROM HSM WHERE ID_HSM = '{ID_HSM}'"
        cursor.execute(query_hsm)
        result = cursor.fetchone()
        info_hsm = {
            "CONTEUDO": result[0],
            "PAYLOAD": result[1]
        }
    return info_hsm

def enviar_mensagem(info_conversa, mensagem, hsm_info):
    telefone =  info_conversa["TELEFONE"]
    conversa_token =  info_conversa["TOKEN"]
    header = {
        'D360-Api-Key': conversa_token,
        'Content-Type': "application/json",
    }
    payload = gerar_payload(telefone, mensagem, hsm_info)
    response = enviar_wpp(header, payload)

    if response in [ERROR_UNMAPPED, ERROR_NO_AUTH, ERROR_NON_JSON_RESPONSE]:
        raise SendMessageError(response)
    
def gerar_payload(telefone, mensagem, hsm_info):
    if hsm_info:
        payload = hsm_info["PAYLOAD"]
        payload = payload.replace('{telefone}', str("\""+str(telefone)+"\""))
        payload = json.loads(payload)
    else:
        payload = {
            "to": str(telefone),
            "messaging_product": "whatsapp",
            "type": "text",
            "text": {"body": mensagem}
        }
        
    payload = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    print('payload da gerar_payload: ', payload)
    return payload

def enviar_wpp(header, payload):
    print('header: ', header)
    print('payload da enviar_wpp: ', payload)
    api_response = requests.post(URL_DIALOG_MESSAGE, data=payload, headers=header)
    print(f"INFO(send_wpp.enviar_wpp): payload = {payload}, response = {api_response.text}")
    print('api_response: ', api_response)
    return processar_api_response(api_response)

def processar_api_response(api_response):
    # print('processar_api_response: ', api_response.headers.get('Content-Type'))
    # if api_response.headers.get('Content-Type') == 'application/json':
    # if 'application/json' in api_response.headers.get('Content-Type'):
    #     data = json.loads(api_response.text)
    #     print('data', data)
    #     if 'meta' in data:
    #         if data['meta'].get('api_status') == "stable":
    #             return 'True'
    #         if data['meta'].get('developer_message') == ERROR_NO_AUTH:
    #             return ERROR_NO_AUTH
    response = json.loads(api_response.text)
    if 'messages' in response and 'id' in response['messages'][0]:
        return 'True' 
    else:
        print(response)
        return ERROR_UNMAPPED
    
    # else:
    #     print(f"WARNING(send_message) - Received non-JSON response. Status code: {api_response.status_code}, Content-Type: {api_response.headers.get('Content-Type')}")
    #     return ERROR_NON_JSON_RESPONSE
    # return ERROR_UNMAPPED

def salvar_mensagem(cursor, info_conversa, mensagem, metadados, hsm_info):
    hsm = 0
    if hsm_info: 
        mensagem = hsm_info["CONTEUDO"]
        hsm = 1
    mensagem_datetime = (datetime.now()- timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")
    telefone = info_conversa["TELEFONE"]
    id_campanha = info_conversa["ID_CAMPANHA"]
    datetime_conversa = info_conversa["DATETIME_CONVERSA"]

    query_insert_flag = f"""
        INSERT INTO MENSAGEM(TELEFONE, ID_CAMPANHA, DATETIME_CONVERSA, MENSAGEM_DATETIME, CONTEUDO, CUSTO, AUTOR_ID_AUTOR, ID_MENSAGEM, HSM)
        VALUES('{telefone}', {id_campanha}, '{datetime_conversa}', '{mensagem_datetime}', '{mensagem}', 0, 0, '', {hsm})"""
    cursor.execute(query_insert_flag)

    id = cursor.lastrowid
    query_insert_mm = f"""
        INSERT INTO MENSAGEM_METADADOS(ID_MENSAGEM, METADADOS)
        VALUES('{id}', "{metadados}")
        """
    cursor.execute(query_insert_mm)