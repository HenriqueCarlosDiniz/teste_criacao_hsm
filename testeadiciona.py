import json, requests, os, time, pytz
import pandas as pd
from datetime import datetime, timedelta
import openai
from functools import lru_cache
import traceback
from utils.config import *

base_url = "https://homolog.psdsaopaulo.net.br/psd2/app/api/agenda-parceiro/"

chaves_api = {
    0: {  # banco_dados = 0
        0: osFunc('API_AGENDAMENTO_NO_SHOW'), 
        1: osFunc('API_AGENDAMENTO_CONFIRMACAO'),
        2: osFunc('API_AGENDAMENTO_CANCELADOS'), 
        3: osFunc('API_AGENDAMENTO_INDIQUE_AMIGOS'), 
        4: osFunc('API_AGENDAMENTO_LEADS_META'), 
        9: osFunc('API_AGENDAMENTO_RECEPTIVO_LP_FASCITE'),
        10: osFunc('API_AGENDAMENTO_RECEPTIVO'),
        11: osFunc('API_AGENDAMENTO_RECEPTIVO_GOOGLE'),
        12: osFunc('API_AGENDAMENTO_SAPATOS_SITE'),
        13: osFunc('API_AGENDAMENTO_AGENDAMENTO_SITES'),
        14: osFunc('API_AGENDAMENTO_CONFIRMACAO_2H'),
        15: osFunc('API_AGENDAMENTO_LEADS_WHATSAPP')
    },
    1: {  # banco_dados = 1
        0: osFunc('API_AGENDAMENTO_NO_SHOW_BD'), 
        2: osFunc('API_AGENDAMENTO_CANCELADOS_BD'), 
    },
}
valor_padrao = osFunc('API_AGENDAMENTO_NO_SHOW')

def retry_api_call(func, args=(), kwargs={}, max_attempts=10, delay=.2, http_session=None):
    """
    Tenta executar a função de chamada API especificada até um número máximo de tentativas.
    Args:
        func: A função de chamada API a ser executada.
        args: Argumentos posicionais para passar para a função.
        kwargs: Argumentos nomeados para passar para a função.
        max_attempts: Número máximo de tentativas (padrão é 10).
        delay: Tempo de espera (em segundos) entre as tentativas (padrão é 1 segundo).
        http_session: A sessão HTTP a ser usada para a chamada API.

    Returns:
        O resultado da chamada API se for bem-sucedida, caso contrário, retorna None após exceder o número máximo de tentativas.
    """
    try:
        attempt = 0
        while attempt < max_attempts:
            result = func(*args, **kwargs) if http_session is None else func(*args, http_session=http_session, **kwargs)
            if result.status_code == 200:
                return result
            else:
                print(f"WARNING(api_communication.retry_api_call): Response = [{result.status_code}], Tentando novamente, Tentativa Atual = {attempt}")
                time.sleep(delay)
                attempt += 1
        return None
    except Exception as e:
        print(f"ERROR(api_communication.retry_api_call): Erro no retry_api_call - error: {e}, traceback: {traceback.format_exc()}")
        return False
    
def GET_unidades_ativas(http_session = requests.Session(), id_campanha = 0, banco_dados = 0):
    # Retorna um dicionario com todas as unidades ativas 
    token_dict = chaves_api.get(banco_dados, {})
    token = token_dict.get(id_campanha, valor_padrao)
    GET_unidades_ativas_URL = base_url + f"unidades/token/{token}"
    response = retry_api_call(http_session.get, args=(GET_unidades_ativas_URL,))
    return response

if __name__ == "__main__":
    print(GET_unidades_ativas(id_campanha=4))