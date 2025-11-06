from utils.config import *
from utils.connect_db import execute_query_db, insert_data_into_db
import json, requests, os, time, pytz
import pandas as pd
from datetime import datetime
import openai
from functools import lru_cache
import traceback
from Models.agente import Agent

URL_OPENAI_STATUS = os.environ['URL_OPENAI_STATUS'] # URL_OPENAI_STATUS = 'https://status.openai.com/api/v2/'
max_cache_size = 50

chaves_api = {
    0: {  # banco_dados = 0
        0: osFunc('API_AGENDAMENTO_NO_SHOW'), 
        1: osFunc('API_AGENDAMENTO_CONFIRMACAO'),
        2: osFunc('API_AGENDAMENTO_CANCELADOS'), 
        3: osFunc('API_AGENDAMENTO_INDIQUE_AMIGOS'), 
        10: osFunc('API_AGENDAMENTO_RECEPTIVO'),
        11: osFunc('API_AGENDAMENTO_AGENDAMENTO_SITES'),
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

if osFunc('Ambiente_Lambda'): # Ambiente Lambda Homolog
    api_psd = osFunc('API_COMMUNICATION')
else:
    api_psd = osFunc('API_COMMUNICATION_LOCAL')


homolog = True if osFunc('AMBIENTE_HOMOLOG') else False 
base_url = "https://homolog.psdsaopaulo.net.br/psd2/app/api/agenda-parceiro/" if homolog else "https://psdsaopaulo.net.br/psd2/app/api/agenda-parceiro/"

def fetch_openai_status(endpoint):
        url = URL_OPENAI_STATUS + endpoint
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"ERROR(api_communication.fetch_openai_status): Error fetching data from {url}. Status code: {response.status_code}")
            return None
        
@lru_cache(maxsize=max_cache_size)
def get_lista_unidades_ativas(http_session):
    unidades_ativas = retry_api_call(GET_unidades_ativas, args=(http_session,))
    unidades_ativas = json.loads(unidades_ativas.text)
    lista_unidades_ativas = [entry["grupoFranquia"] for entry in unidades_ativas]
    return lista_unidades_ativas


def get_lista_unidades_lat_long(http_session):
    unidades_ativas = retry_api_call(GET_unidades_ativas, args=(http_session,))
    unidades_ativas = json.loads(unidades_ativas.text)
    lista_unidades_lat_long = [[entry["grupoFranquia"],entry["latitude"],entry["longitude"]] for entry in unidades_ativas]
    return lista_unidades_lat_long

## Funcoes da API de agendamento da PSD
@lru_cache(maxsize=max_cache_size)
def GET_unidade_estado(estado, http_session = requests.Session(), id_campanha = 0, banco_dados = 0):
    token_dict = chaves_api.get(banco_dados, {})
    token = token_dict.get(id_campanha, valor_padrao)
    GET_unidade_estado_URL = base_url + f'unidades-por-estado/estado/{estado}/token/{token}'
    response = retry_api_call(http_session.get, args=(GET_unidade_estado_URL,))
    return response

@lru_cache(maxsize=max_cache_size)
def GET_estados_unidades(http_session = requests.Session(), id_campanha = 0, banco_dados = 0):
    token_dict = chaves_api.get(banco_dados, {})
    token = token_dict.get(id_campanha, valor_padrao)
    GET_estados_unidades_URL = base_url + f'estados/token/{token}'
    response = retry_api_call(http_session.get, args=(GET_estados_unidades_URL,))
    return response

@lru_cache(maxsize=max_cache_size)
def GET_unidades_proximas(latitude, longitude, http_session = requests.Session(), id_campanha = 0, banco_dados = 0):
    token_dict = chaves_api.get(banco_dados, {})
    token = token_dict.get(id_campanha, valor_padrao)
    GET_unidades_proximas_URL = base_url + f'unidade-mais-proxima/latitude/{latitude}/longitude/{longitude}/token/{token}'
    response = retry_api_call(http_session.get, args=(GET_unidades_proximas_URL,))
    return response

@lru_cache(maxsize=max_cache_size)
def GET_vagas_disponiveis(data_inicial, data_final, unidade, http_session = requests.Session(), id_campanha = 0, banco_dados = 0):
    # Recebe a data inicial e a data final no formato aaaa-mm-dd, a unidade como uma sigla "PSD"
    # Retorna uma request com a resposta 
    token_dict = chaves_api.get(banco_dados, {})
    token = token_dict.get(id_campanha, valor_padrao)
    GET_vagas_disponiveis_URL = base_url + f"vagas-disponiveis-periodo/local/{unidade}/data_inicial/{data_inicial}/data_final/{data_final}/token/{token}"
    response =  retry_api_call(http_session.get, args=(GET_vagas_disponiveis_URL,))
    return response

@lru_cache(maxsize=max_cache_size)
def GET_unidades_ativas(http_session = requests.Session(), id_campanha = 0, banco_dados = 0):
    # Retorna um dicionario com todas as unidades ativas 
    token_dict = chaves_api.get(banco_dados, {})
    token = token_dict.get(id_campanha, valor_padrao)
    GET_unidades_ativas_URL = base_url + f"unidades/token/{token}"
    response = retry_api_call(http_session.get, args=(GET_unidades_ativas_URL,))
    return response

def POST_agendar(conversa, unidade_agendamento, horario_agendamento, data_agendamento, token_reagendamento = None, nome = None):

    # Input: 
    # - Objeto cliente
    # - Sigla da unidade a ser agendada, 
    # - Horario do agendamento no formato hh:mm
    # - Data agendamento no formato aaaa-mm-dd

    # Output:
    # - agendado
    # - id_agendamento
    # - token_confirmacao
    # - token_cancelamento
    # - indice: 0 (Falha no agendamento), 1 (Agendamento com sucesso), 2 (Vaga não mais disponível)
    indice = 0
    
    if nome is None:
        nome = conversa.contato.nome.strip()

    id_campanha = conversa.campanha
    banco_dados = conversa.banco_dados
    token_dict = chaves_api.get(banco_dados, {})
    token = token_dict.get(id_campanha, valor_padrao)

    size_numero = len(str(conversa.contato.id_contato))
    numero = str(conversa.contato.id_contato) 
    token_confirmacao = ""
    token_cancelamento = ""
    id_agendamento = ""
    agendado = False
    if size_numero == 13: # Tem o 9 extra
        dd_pais = numero[0:2]
        dd_estado = numero[2:4]
        numero_limpo = numero[4:]
    elif size_numero == 12: # nao tem o 9 extra
        dd_pais = numero[0:2]
        dd_estado = numero[2:4]
        numero_limpo = numero[4:]
    elif  str(conversa.contato.id_contato)[0:2] != "55":
        dd_pais = numero[0:3]
        dd_estado = ""
        numero_limpo = numero[3:]  

    url_base = "https://homolog.psdsaopaulo.net.br" if homolog else "https://psdsaopaulo.net.br"
    url = f"{url_base}/psd2/app/api/agenda-parceiro/agendar"

    payload = {
        'cel': numero_limpo,
        'ddd_cel': dd_estado,
        'currentData': data_agendamento,
        'email': conversa.contato.email.strip(),
        'hora': horario_agendamento,
        'nome': nome,
        'unidade': unidade_agendamento,
        'token': token if homolog else token
    }

    #Quando realizar um reagendamento na campanha de confirmação ao fazer o agendamento novo passar o token utilizado para fazer o cancelamento previamente.
    #Para que o sistema conte como reagendamento e não como um agendamento novo.
    if token_reagendamento is not None:
        payload['token_cancelamento'] = token_reagendamento

    try: 
        print(f"INFO(api_communication.POST_agendar)-{numero}: Input POST Agendamento PSD2 - url = {url}, payload = {payload}")
        response = retry_api_call(requests.request, args=("POST", url), kwargs={'headers': payload, 'data': payload})
        print(f"INFO(api_communication.POST_agendar)-{numero}: Output POST Agendamento PSD2 - response = {response.text}")
        respostaDic = json.loads(response.text)

        if respostaDic["result"] == 1: # Agendamento com sucesso 
            id_agendamento = respostaDic["id"]
            token_confirmacao = respostaDic["token_confirmacao"]
            token_cancelamento = respostaDic["token_cancelamento"]
            agendado = True
            indice = 1
        
        ################################################################################################
        elif respostaDic["result"] == 0: # Agendamento sem sucesso porque não tem mais a vaga
            if "vaga" in respostaDic and respostaDic["vaga"] == "a vaga selecionada nao esta mais disponivel": 
                indice = 0 
            elif "data" in respostaDic and respostaDic["data"] == "data invalida":
                indice = 0 
 
        elif respostaDic["result"] == 2: # Já possui agendamento
            indice = 2  
        ################################################################################################
          
        return agendado, id_agendamento, token_confirmacao, token_cancelamento, indice

    except Exception as e: 
        print(f"ERROR(api_communication.POST_agendar)-{numero}: Erro no POST Agendamento PSD2 - error: {e}, traceback: {traceback.format_exc()}")
        return agendado, id_agendamento, token_confirmacao, token_cancelamento, 0

def POST_buscar_telefone(telefone, id_campanha = 0, banco_dados = 0, listar_todos = 0): #ALGUMAS FUNÇÕES NÃO PASSAM o id_campanha e banco_dados

    numero = str(telefone) 
    size_numero = len(numero)
    token_dict = chaves_api.get(banco_dados, {})
    token = token_dict.get(id_campanha, valor_padrao)
    
    if size_numero == 13: # Tem o 9 extra
        dd_pais = numero[0:2]
        dd_estado = numero[2:4]
        numero_limpo = numero[4:]
    elif size_numero == 12: # nao tem o 9 extra
        dd_pais = numero[0:2]
        dd_estado = numero[2:4]
        numero_limpo = str(9) +  numero[4:]
    elif  str(telefone)[0:2] != "55":
        dd_pais = numero[0:3]
        dd_estado = ""
        numero_limpo = numero[3:] 

    tel = str(dd_estado + numero_limpo)

    url = base_url + 'buscar-telefone'
    payload = {'token': token, 'telefone': tel, 'listar_todos': listar_todos}
    files = []
    headers = {'token': token, 'telefone': tel}

    response = retry_api_call(requests.request, args=("POST", url), kwargs={'headers': headers, 'data': payload})
    response = json.loads(response.text)['resposta']

    if 'sucesso' in response.keys():
        if not response['sucesso']:
            tel = str(dd_estado + numero_limpo[1:])
            payload = {'token': token, 'telefone': tel}
            response = retry_api_call(requests.request, args=("POST", url), kwargs={'headers': payload, 'data': payload})
            print('retry') 
    else:
        response['sucesso'] = False

    return response
        
    
def POST_cancelar_agendamento(token_agendamento, id_campanha = 0, banco_dados = 0):
    
    token_dict = chaves_api.get(banco_dados, {})
    token = token_dict.get(id_campanha, valor_padrao)
    url = base_url + "cancelar"
    payload = {'token': token, 'token_agendamento': token_agendamento}
    files = []
    headers = {}

    response = retry_api_call(requests.request, args=("POST", url), kwargs={'data': payload})
    return response

def POST_confirmar_agendamento(token_agendamento, id_campanha = 0, banco_dados = 0):
    url = base_url + 'confirmar'
    token_dict = chaves_api.get(banco_dados, {})
    token = token_dict.get(id_campanha, valor_padrao)
    payload = {'token': token,
            'token_agendamento': token_agendamento}
    files=[]
    headers = {}

    response = retry_api_call(requests.request, args=("POST", url), kwargs={'data': payload})
    return response


def check_gpt_api_status(conversa):
    try:
        api_key = osFunc('OPENAI_TESTE_API')
        
        if conversa.banco_dados == 1: 
            api_key = osFunc('OPENAI_TESTE_API_BD')     
            
        agente = Agent()
        discussion = [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Translate 'Hello, world!' to Spanish"}
        ]
        
        agente.model = "gpt-3.5-turbo-1106"
        response = agente.get_response_simples(discussion, conversa, use_functions = False, api_key = api_key)
        
        if response:
            return True
        else:
            print("WARNING(api_communication.check_gpt_api_status): GPT API did not return a response.")
            return False
        
    except Exception as e:
        print("ERROR(api_communication.check_gpt_api_status): An error occurred with the GPT API:", e)
        return False
    
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

def verificar_agendamento(telefone, campanha, datetime_conversa):
    try:
        agendamento = POST_buscar_telefone(telefone, id_campanha = campanha)
        if agendamento.status_code != 200:
            #print(f"{telefone}: Erro ao verificar agendamento anterior")
            return False
    
        agendamento_json = json.loads(agendamento.text)["resposta"]
        if agendamento_json["sucesso"]: 
            data_agendamento = datetime.strptime(agendamento_json["atendimento_em"], '%Y-%m-%d %H:%M:%S')
            if data_agendamento.date() >= datetime.now(pytz.timezone('America/Sao_Paulo')).date():
                #print(f"{telefone}: Agendamento encontrado para o {data_agendamento}")
                current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo'))
                formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
                
                # conversa.set_flag(6, formatted_datetime)
                execute_query_db(f'''UPDATE CONVERSA SET 
                                ID_LAST_FLAG = {osFunc('FINALIZADO')} 
                                WHERE 
                                DATETIME_CONVERSA = "{datetime_conversa}" AND
                                ID_CAMPANHA = {campanha} AND
                                TELEFONE = {telefone} LIMIT 1;''') 

                data_to_upload = {
                    'TELEFONE': [telefone],
                    'ID_CAMPANHA': [campanha],
                    'DATETIME_CONVERSA': [datetime_conversa],
                    'DATETIME_FLAG': [formatted_datetime],
                    'ID_FLAG': [osFunc('FINALIZADO')]
                }
                insert_data_into_db(pd.DataFrame(data_to_upload), "FLAG_HISTORICO")
                
                query = f"""
                    UPDATE CONVERSA
                    SET ID_RESULTADO = {osFunc('TELEMARKETING')}, ID_LAST_FLAG = {osFunc('FINALIZADO')} 
                    WHERE TELEFONE = {telefone}
                        AND ID_CAMPANHA = {campanha}
                        AND DATETIME_CONVERSA = '{datetime_conversa}'
                        AND ID_RESULTADO != {osFunc('TELEMARKETING')}
                        LIMIT 1;
                """
                execute_query_db(query)
                
                return True
        
        #print(f"{telefone}: Nenhum agendamento encontrado")
        return False
    except:
        print(f"WARNING(api_communication.verificar_agendamento)-{telefone}: Erro ao verificar agendamento anterior")
        return False
    
def verificar_confirmacao(telefone, id_campanha = 0):
    try:
        verificacao_confirmacao = {'agendado': False, 'data': False, 'horario': False, 'confirmado': False}
        agendamento = POST_buscar_telefone(telefone, id_campanha = id_campanha)
        if agendamento.status_code != 200:
            # print(f"{telefone}: Erro ao verificar agendamento")
            return verificacao_confirmacao
    
        agendamento_json = json.loads(agendamento.text)["resposta"]
        if agendamento_json["sucesso"]: 
            verificacao_confirmacao['agendado'] = True
            data = datetime.strptime(agendamento_json["atendimento_em"], '%Y-%m-%d %H:%M:%S')
            data_agendamento = data.date().strftime('%Y-%m-%d')
            horario_agendamento = data.time().strftime('%H:%M')
            verificacao_confirmacao['data'] = data_agendamento
            verificacao_confirmacao['horario'] = horario_agendamento
            # print(f"{telefone}: Agendamento encontrado para o dia {data_agendamento} às {horario_agendamento}")

            if agendamento_json["confirmado"]: 
                verificacao_confirmacao['confirmado'] = True
                # print(f"{telefone}: Agendamento dia {data_agendamento} às {horario_agendamento} já confirmado")
        
        return verificacao_confirmacao
    except:
        print(f"WARNING(api_communication.verificar_confirmacao)-{telefone}: Erro ao verificar agendamento anterior")
        return {'agendado': False, 'data': False, 'horario': False, 'confirmado': False}

def realiza_agendamento(conversa, datetime_agendamento, nomes_agendamento, unidade_agendamento, tokens2delete):
    # dicionario como resultado
    agendamento_resultado = {}

    for datetime_estudado, nome in zip(datetime_agendamento, nomes_agendamento):
        
        # No caso de apenas um agendamento, o nome do agendamento será o cadastrado
        # if (conversa.agendamento_multiplo == 1):
        #     nome = conversa.contato.nome
        
        # resgatando o horario e data do datetime
        resultado = False 
        horario_final = format_time(datetime_estudado[-5:])
        data_final = datetime_estudado[:-6]

        # definindo token de reagendamento
        if tokens2delete != [] and not tokens2delete is None:
            token_reagendamento = tokens2delete.pop()
        else: 
            token_reagendamento = None

        # realizando o agendamento
        ##################BUG FIX#############################################################
        agendado, id_agendamento, token_confirmacao, token_cancelamento, indice_resposta = POST_agendar(conversa, unidade_agendamento, horario_final, data_final,token_reagendamento = token_reagendamento, nome = nome)
        ##################BUG FIX#############################################################
        
        agendamento_resultado['ID_AGENDAMENTO'] = []
        agendamento_resultado['TOKEN_CONFIRMACAO'] = []
        agendamento_resultado['TOKEN_CANCELAMENTO'] = []
        agendamento_resultado['TELEFONE'] = []
        agendamento_resultado['NOME'] = []
        agendamento_resultado['DATA'] = []
        agendamento_resultado['HORARIO'] = []
        agendamento_resultado['UNIDADE_PSD'] = []
        
        if agendado:
            resultado = True 
            agendamento_resultado['ID_AGENDAMENTO'].append(id_agendamento)
            agendamento_resultado['TOKEN_CONFIRMACAO'].append(token_confirmacao)
            agendamento_resultado['TOKEN_CANCELAMENTO'].append(token_cancelamento)
            agendamento_resultado['TELEFONE'].append(conversa.contato.id_contato)
            agendamento_resultado['NOME'].append(nome)
            agendamento_resultado['DATA'].append(data_final)
            agendamento_resultado['HORARIO'].append(horario_final)
            agendamento_resultado['UNIDADE_PSD'].append(unidade_agendamento)
            
        else:
            resultado = False
            print(f"ERROR(utils_lambda.realiza_agendamento)-{conversa.contato.id_contato}: erro no agendamento {datetime_estudado}")
            ##################BUG FIX#############################################################
            return resultado, False, indice_resposta
    
    return resultado, agendamento_resultado, indice_resposta
    ##################BUG FIX#############################################################
        
def registra_agendamentoAPI(conversa, unidade, nomes_agendamento, datetime_agendamento):
    # Input:
    # Unidade como sigla: ex "PSD"
    # Horario no formato "hh:mm"
    # Data do agendamento no formato "aaaa-mm-dd"
    # Output:
    # True/False para indicar se agendou ou não
    # indice_resposta: 0 (Falha no agendamento), 1 (Agendamento com sucesso), 2 (Vaga não mais disponível), 3 (Confirmação mesma data/horário)
    
    try:
        # verificando se o número de nomes está igual a qtd de pessoas a serem agendadas
        if conversa.agendamento_multiplo > 1 and conversa.agendamento_multiplo != len(nomes_agendamento):
            return None , 5, None
        
        unidade_agendamento = unidade.sigla
         
        tokens2delete = []
        
        # checando se já há um agendamento do cliente
        agendado, agendamento_detalhes = checa_agendamento(conversa, unidade_agendamento)
        print('agendado ', agendado, ' , detalhes : ', agendamento_detalhes)
        # adicionando os tokens a serem deletados para as campanhas de confirmação
        if isinstance(agendamento_detalhes, list):
            tokens2delete += agendamento_detalhes

        print(f"INFO(utils_lambda.registra_agendamentoAPI)-{conversa.contato.id_contato}: agendamento {agendado}, {agendamento_detalhes}")


        agendamentos_match = []    
        # Em caso de já existir um agendamento, cancelar os horarios que não estão mais sendo agendados
        if agendado:
            for i, agendamento_antigo in enumerate(agendamento_detalhes["agendamentos"]):
                
                similar_word_found, similar_index = similarity_into_list(agendamento_antigo['nome_cliente'], nomes_agendamento)

                if similar_word_found:
                    print(f"{agendamento_antigo['atendimento_em'][:-3]} == {datetime_agendamento[similar_index]}")
                    if agendamento_antigo['atendimento_em'][:-3] == datetime_agendamento[similar_index]:
                        nomes_agendamento.pop(similar_index)
                        horario_match = datetime_agendamento.pop(similar_index)
                        agendamentos_match.append(horario_match)

                    else:
                        tokens2delete.append(agendamento_antigo['token_cancelamento'])
        
        # verificando se os horarios pedidos estao disponiveis
        disponibilidade_hrr, hrr_indisponiveis, hrr_disponiveis = verifica_horarios_disponiveis(datetime_agendamento, unidade)
        
        
        # em caso de indisponibilidade dos horários solicitados, retorna-se 4
        if not disponibilidade_hrr:
            hrr_disponiveis += agendamentos_match
            
            horarios_resultados = {'hrr_indisponiveis': hrr_indisponiveis,
                                   'disponibilidade_hrr': hrr_disponiveis}
            
            return None, 4, horarios_resultados
        
        # disponibilidade dos horários solicitados
        else:

            # tratando cancelamentos antes
            # em caso de confirmação e horários iguais
            if conversa.campanha in [osFunc('CONFIRMACAO'),osFunc('CONFIRMACAO_2H')] and tokens2delete == []:
                print(f"INFO(utils_lambda.registra_agendamentoAPI)-{conversa.contato.id_contato}: Ignorando agendamento pois se trata de confirmação...")
                return None, 3, None #Confirmação detectou agendamento com mesma data e horário já agendado, ou seja, ignorar!

            # agendamento normal sem token
            elif datetime_agendamento == []:
                # Para um agendamento verifica-se se o agendamento é igual 
                # se o horário for igual, não faz nada
                print(f"INFO(utils_lambda.registra_agendamentoAPI)-{conversa.contato.id_contato}: Agendamento anterior detectado com horario igual")
                return None, 1, None
            
            else:                
                print(f"INFO(utils_lambda.registra_agendamentoAPI)-{conversa.contato.id_contato}: Cancelando agendamentos anteriores")
                tokens2delete = cancelar_agendamento(conversa, tokens2delete)
        
            ##################BUG FIX#############################################################
            resultado, resultado_agendamento, indice_resposta_post_agendar = realiza_agendamento(conversa, datetime_agendamento, nomes_agendamento, unidade_agendamento, tokens2delete)
            ##################BUG FIX#############################################################
        
            if resultado: 
                # inserindo os dados de agendamento no database
                print(f"INFO(utils_lambda.registra_agendamentoAPI)-{conversa.contato.id_contato}: Salvando os dados de agendamento no Database")
                insert_data_into_db(pd.DataFrame(resultado_agendamento), 'AGENDAMENTO_REALIZADO')
                return None, 1, None
            
            ##################BUG FIX#############################################################
            elif indice_resposta_post_agendar == 2:
                return None, 1, None
            ##################BUG FIX#############################################################
            
            else:
                return None, -1, None 

 
    except Exception as e:
        traceback.print_exc()
        print(f"ERROR(utils_lambda.registra_agendamentoAPI)-{conversa.contato.id_contato}: Erro no agendamento {e}")
        return False, 0, None

def format_time(time_str):
    """
    Converte uma string de hora no formato hh:mm ou hh:mm:ss para o formato hh:mm.

    Args:
    time_str (str): Uma string representando a hora, que pode estar no formato hh:mm ou hh:mm:ss.

    Returns:
    str: A hora no formato hh:mm. Se o formato de entrada for inválido, retorna uma mensagem de erro.
    """
    try:
        # Tenta parsear a string no formato hh:mm:ss
        time_obj = datetime.strptime(time_str, "%H:%M:%S")
    except ValueError:
        try:
            # Se falhar, tenta parsear no formato hh:mm
            time_obj = datetime.strptime(time_str, "%H:%M")
        except ValueError:
            # Se ambos falharem, retorna uma mensagem de erro
            return "Formato de hora inválido. Use hh:mm ou hh:mm:ss."

    # Retorna a string formatada em hh:mm
    return time_obj.strftime("%H:%M")


def buscar_unidades(endereco, qtd_unidades = 4):
    if osFunc('Ambiente_Lambda'): 
        if 'homolog' in os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            response = requests.get(f'https://homolog.psdsaopaulo.net.br/psd2/app/api/agenda-parceiro/unidade-mais-proxima-busca/{endereco}/unidades/{qtd_unidades}/token/6990aba8b6966e98be0de64d3bc80dcaa1abde8d788714f58329eaca9b3ad78c')
        else:
            response = requests.get(f'https://psdsaopaulo.net.br/psd2/app/api/agenda-parceiro/unidade-mais-proxima-busca/{endereco}/unidades/{qtd_unidades}/token/6990aba8b6966e98be0de64d3bc80dcaa1abde8d788714f58329eaca9b3ad78c')
    else:
        response = requests.get(f'https://homolog.psdsaopaulo.net.br/psd2/app/api/agenda-parceiro/unidade-mais-proxima-busca/{endereco}/unidades/{qtd_unidades}/token/6990aba8b6966e98be0de64d3bc80dcaa1abde8d788714f58329eaca9b3ad78c')
     
    dist_max = 500

    if response.status_code == 200 or response.status_code == 201:
        response = json.loads(response.text)

        # caso a reposta da api seja um vetor
        if isinstance(response, list):
            # verifica se a primaira unidade do vetor está abaixo do limite de distancia
            # caso sim, retorna um dicionario vazio
            if float(response[0]['distancia_km'].replace(',','')) > dist_max:
                return {}
            else:
                # filtro da distancia para as unidades do vetor
                for index, unidade in enumerate(response):
                    if float(unidade['distancia_km'].replace(',','')) > dist_max:
                        response = response[:index]
                        break
        # resposta da api vazia
        elif response != {}:
            if float(response['distancia_km'].replace(',','')) > dist_max:
                return {}
    else:
        response = {}
  
    return response




def verifica_horarios_disponiveis(datetime_agendamento, unidade):
    horarios_indisponiveis = []
    horarios_disponiveis = []
    for i in range(len(datetime_agendamento)):
        datetime_estudado = datetime_agendamento[i]
        horarios_indisponiveis.append(datetime_estudado)
        horarios_disponiveis.append(datetime_estudado)
        if datetime_estudado[:-6] in unidade.agenda.keys():
            #checando se há horários disponíveis para todas as pessoas 
            if datetime_estudado[-5:] in unidade.agenda[datetime_estudado[:-6]]:
                horarios_indisponiveis.pop()
            else:
                horarios_disponiveis.pop()
        else:
                horarios_disponiveis.pop()
 
    if horarios_indisponiveis == []:
        return True, horarios_indisponiveis, horarios_disponiveis
    else:
        return False, horarios_indisponiveis, horarios_disponiveis
    


def cancelar_agendamento(conversa, tokens2delete):
    
    successful_cancel = []
    for token in tokens2delete:
        response = POST_cancelar_agendamento(token, id_campanha = conversa.campanha).text        
        print(f"INFO(utils_lambda.registra_agendamentoAPI)-{conversa.contato.id_contato}: Cancelando agendamento pela API, Response: {response}")
        
        try:
            response = json.loads(response)
        except:
            pass 

        if response['resposta']['sucesso'] == "true" or response['resposta']['sucesso'] == True:
            successful_cancel.append(token)

    # deletando os agendamentos do database
    successful_cancel += successful_cancel
    
    if successful_cancel != []:
        print(f"INFO(utils_lambda.registra_agendamentoAPI)-{conversa.contato.id_contato}: Deletando os tokens do database")
        execute_query_db(f'DELETE FROM AGENDAMENTO_REALIZADO WHERE TOKEN_CANCELAMENTO IN {tuple(successful_cancel)}')
    
    
 
def checa_agendamento(conversa, unidade_agendamento):
    # Deletando agendamentos que restaram na API
    # Reduzindo chamadas de API: Obtem a resposta da busca pelo telefone do cliente
    resposta = POST_buscar_telefone(conversa.contato.id_contato, id_campanha = conversa.campanha, listar_todos=1)
    agendado = resposta["sucesso"] 
     
    if agendado: 
        print(f"""INFO(utils_lambda.registra_agendamentoAPI)-{conversa.contato.id_contato}: {len(resposta['agendamentos'])} agendamento(s) anterior detectado: {[x["atendimento_em"] for x in resposta['agendamentos']]}""")
        return agendado, resposta
    else:
        if conversa.campanha in [osFunc('CONFIRMACAO'), osFunc('CONFIRMACAO_2H')]:
            df = query_to_dataframe(f"SELECT * FROM AGENDAMENTO_REALIZADO WHERE TELEFONE = {conversa.contato.id_contato} AND DATA  >= CURDATE() ORDER BY DATA DESC")
            
            if len(df) > 0:
                return False, df['TOKEN_CANCELAMENTO'].to_list()

        return False, False
    
def similarity_into_list(word, array, threshold = 0.5):
    from utils.text import similar
    # esta função calcula a similaridade entre uma palavra comparada com todas 
    # strings de uma lista
    # retorno do index com a maior similaridade dentro do array
    index = None
    found_similar = False
    highest_similiarity = threshold
    array = [name.split()[0] for name in array]
    word = word.split()[0]
    for i, word2 in enumerate(array):
        similiarity = similar(word, word2)

        print(f'similarity {similiarity} - {word}, {word2}')

        if similiarity > highest_similiarity:
            highest_similiarity = similiarity
            index = i
            found_similar = True
    
    return found_similar, index
    

