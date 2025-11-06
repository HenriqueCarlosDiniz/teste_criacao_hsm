from utils.config import *

import json
from datetime import datetime
import pytz
import time

from Models.contato import Contato
from Models.conversa import Conversa
from utils.send_wpp import enviar_wpp

class BadRequestError(Exception):
    def __init__(self, response="Bad Request"):
        super().__init__(response)

class SendMessageError(Exception):
    def __init__(self, response):
        message = f"erro em enviar mensagem pra dialog: {response}"
        super().__init__(message)

def lambda_handler(event, context):
    start_time = time.time()
    print("EVENT:", json.dumps(event))
    print(f": pós print event: {time.time() - start_time:.3f} ")
          
    try:
        telefone, mensagem = extrair_headers(event)
        print(f": pós extrair_headers: {time.time() - start_time:.3f} ")
        contato = Contato(telefone)
        print(f": pós contato: {time.time() - start_time:.3f} ")
        conversa = Conversa(contato)
        print(f": pós conversa: {time.time() - start_time:.3f} ")
        update_conversa(conversa, mensagem)
        print(f": pós update_conversa: {time.time() - start_time:.3f} ")
        enviar_mensagem(conversa, telefone, mensagem)
        print(f": pós enviar_mensagem: {time.time() - start_time:.3f} ")
        salvar_mensagem(conversa, mensagem)
        print(f": pós salvar_mensagem: {time.time() - start_time:.3f} ")
    except BadRequestError as e:
        print(f"BadRequestError: {e}")
        return response(str(e), 400)
    except SendMessageError as e:
        print(f"SendMessageError: {e}")
        return response(str(e), 500)
    except Exception as e:
        print(f"ExceptionError: {e}")
        return response("Internal server error", 500)

    return response("mensagem enviada com sucesso", 200)

def response(message, status_code):
    return {
        'statusCode': status_code,
        'body': json.dumps(message)
    }

def extrair_headers(event):
    header = event.get('headers')
    if not header:
        raise BadRequestError("Sem header no request")
    telefone = header.get('telefone')
    mensagem = header.get('mensagem')
    if not telefone or not mensagem:
        raise BadRequestError("Sem telefone ou mensagem no header")
    return telefone, mensagem

def update_conversa(conversa, mensagem):
    flag_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S")
    if conversa.flag != osFunc("ON_HOLD"):
        conversa.set_flag("ON_HOLD", flag_datetime)
    if conversa.resultado != osFunc("INTERVENCAO"):
        conversa.set_resultado("INTERVENCAO")

def enviar_mensagem(conversa, telefone, mensagem):
    response = enviar_wpp(telefone, mensagem, quebrar_mensagem_condition=False, header=conversa.wpp_header)
    if response in ['False', "No authorization provided"]:
        raise SendMessageError(response)

def salvar_mensagem(conversa, mensagem):
    mensagem_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S")
    conversa.add_mensagem(mensagem, mensagem_datetime)