from utils.config import * 

import json
import openai
import re  
import requests
import time
import traceback
import pytz
from datetime import datetime

from utils.api_communication import check_gpt_api_status
from utils.message import calcular_preco_gpt
from utils.connect_db import query_to_dataframe, DatabaseConnection
from utils.config import osFunc
from utils.text import get_data_proxima, extrair_nomes_unidades
from Models.unidade import Unidade

if osFunc('Ambiente_Lambda'):  
    from context import get_agent, get_user_string
    from filtro_conversa import filtra_resposta, filtro_hardcoded
    from functions import find_nearest_units, available_times_updated
else: 
    from geraResposta.context import get_agent, get_user_string
    from geraResposta.filtro_conversa import filtra_resposta, filtro_hardcoded
    from geraResposta.functions import find_nearest_units, available_times_updated


def gera_resposta(tarefa, conversa, http_session, extras = None):
    #if check_gpt_api_status(conversa):
    try:
        if tarefa == "ATENDENTE":
            resposta, preco = get_response(conversa, http_session)
            if filtro_hardcoded(resposta):
                print(f'INFO(filtro_hardcoded)-{conversa.contato.id_contato}: resposta antes:{resposta}')
                resposta, preco = get_response(conversa, http_session, hard_temperature = 1)
                print(f'INFO(filtro_hardcoded)-{conversa.contato.id_contato}: resposta depois:{resposta}')
                # Use regex to find everything after "Atendente:"
                match = re.search(r'Atendente:\s*(.*)', resposta, re.DOTALL)
                if match: 
                    resposta = match.group(1)
                print(f'INFO(filtro_hardcoded)-{conversa.contato.id_contato}: resposta depois:{resposta}')
        else:
            resposta, preco = get_response_tarefas(tarefa, conversa, http_session, extras)
        if resposta == "Desculpe não entendi, você poderia repetir?":
            return "", False, 0
        return resposta, True, preco
    except:
            traceback_str = traceback.format_exc()
            print(f'ERROR(gera_resposta)-{conversa.contato.id_contato}:', traceback_str)
            return "", False, 0
    # else:
    #     print(f'ERROR(gera_resposta)-{conversa.contato.id_contato}: erro no check_gpt_api_status')
    #     return "", False, 0
    
def get_response_tarefas(tarefa, conversa, http_session, extras = None): 
    agent = get_agent(conversa, tarefa, extras=extras)
    print("INFO(get_response_tarefas): TAREFA:", tarefa, "bool_user:", agent.bool_user_string, "json_mode:", agent.json_mode)
    if agent.json_mode:
        if agent.bool_user_string:
            texto_user = get_user_string(tarefa, conversa, extras = extras)
            discussion = [{"role": "system", "content": agent.context}, {"role": "user", "content": texto_user}]
        else:
            discussion = [{"role": "system", "content": agent.context}]
        completion = agent.get_response_simples(discussion, conversa)
        preco = calcular_preco_gpt(completion, agent)
        resposta = completion.choices[0].message.content
        resposta = json.loads(resposta)
        if 'label' in  resposta.keys():
            return resposta['label'], preco
    else:
        if agent.bool_user_string:
            texto_user = get_user_string(tarefa, conversa, extras = extras)
            discussion = [{"role": "system", "content": agent.context}, {"role": "user", "content": texto_user}]
        else:
            discussion = [{"role": "system", "content": agent.context}]
        completion = agent.get_response_simples(discussion, conversa)
        preco = calcular_preco_gpt(completion, agent)
        resposta = completion.choices[0].message.content
    return resposta, preco
    

def get_response(conversa, http_session, hard_temperature = None):
        agent = get_agent(conversa, "ATENDENTE", hard_temperature = hard_temperature)
        discussion = conversa.get_discussion()
        discussion.insert(0, {"role":"system", "content":agent.context})
        response, function_name, function_response, preco = generate_gpt(discussion, conversa, agent, http_session)
        filtrado, resposta_filtrada = filtra_resposta(response, function_response, function_name)
        return resposta_filtrada, preco
    
def generate_gpt(discussion, conversa, agent, http_session = requests.Session()):
    function_name = ""
    function_response = "" 
    try:
        if agent.functions:
            completion = agent.get_response_simples(discussion, conversa, use_functions = True)
        else:
            completion = agent.get_response_simples(discussion, conversa, use_functions = False)
        response_message = completion["choices"][0]["message"]
        function_name = "" 
        tokens_input = completion["usage"]["prompt_tokens"]
        tokens_output = completion["usage"]["completion_tokens"]
        # Step 2: check if GPT wanted to call a function
        if response_message.get("function_call"): # Chamado de função
            available_functions = {
                "find_nearest_units": find_nearest_units, 
                "available_times_updated": available_times_updated,
            } 

            function_name = response_message["function_call"]["name"]            
            function_to_call = available_functions[function_name]
            function_args = json.loads(response_message["function_call"]["arguments"])
            print(f"INFO(function_args, generate_gpt) - {conversa.contato.id_contato}: function_args:", function_args)
            # Convert function_args to JSON with ensure_ascii=False
            function_args = json.dumps(function_args)
            function_args = clean_encoding(function_args)
            function_args = json.loads(function_args)
            t0 = time.time()

            function_response = function_to_call(http_session = http_session, conversa=conversa, **function_args)

            if function_name == "find_nearest_units":
                nome_contexto = 'FUNCTION_FNU'
                if conversa.completar_cadastro:
                    if type(function_response) != list:
                        function_response = [function_response]

                    siglas_unidades = extrair_nomes_unidades(function_response, http_session) 
                    result = []
                    for index, unidade_estudada in enumerate(siglas_unidades):
                        data_proxima = get_data_proxima(Unidade(unidade_estudada), datetime.now(pytz.timezone('America/Sao_Paulo')))

                        #BUGFIX (caso a unidade não tenha data)
                        if isinstance(data_proxima, datetime):
                            continue
                        
                        # Verifica se há uma correspondência de data
                        date_match = re.search(r'dia (\d{2}/\d{2}/\d{4})', data_proxima)
                        if date_match:
                            date_str = date_match.group(1)
                            date_object = datetime.strptime(date_str, "%d/%m/%Y")
                            formatted_date = date_object.strftime("%Y-%m-%d")
 
                        available_times = available_times_updated([formatted_date], unidade_estudada, weekday= "quinta", http_session=http_session, conversa= conversa)
 
                        if "Agendamento" not in available_times:  
                            available_times_str = ', '.join(available_times)
                            result.append( function_response[index] + ' - ' + available_times_str)
                            
                    function_response = result

                info_aux = f"""{function_response}"""

            if function_name == "available_times_updated": 
                nome_contexto = 'FUNCTION_ATU'
                info_aux = f"""{function_response}"""
                    
            print(f'INFO({nome_contexto}:function_response = info_aux)-:{conversa.contato.id_contato}', function_response)
            string_conversa = get_string_conversa(discussion, n = 3)
            extras = {"string_conversa":string_conversa, "info_aux": info_aux}
            print(f'INFO(generate_gpt:extras->input gera_resposta(nome_contexto = {nome_contexto}))-:{conversa.contato.id_contato}', extras)
            response, error, preco = gera_resposta(nome_contexto, conversa, http_session, extras=extras)

        else: #Sem chamado de função: verificar FAQ
            first_response = completion["choices"][0]["message"]["content"]
            # Se não chamou nenhuma função, checar se alguma informação do FAQ pode ser útil
            aux_FAQ = get_FAQ(conversa, http_session)
            #Checar se a pergunta está presente no FAQ
            if not aux_FAQ["Pergunta"] == "None": #Pergunta encontrada no FAQ
                tarefa = 'AUX_FAQ'
                extras = {"resposta_sem_faq":first_response, "resposta_faq": aux_FAQ['Resposta']}
                print(f'INFO(generate_gpt:extras->input gera_resposta(nome_contexto = {tarefa}))-:{conversa.contato.id_contato}', extras)
                response, error, preco = gera_resposta(tarefa, conversa, http_session, extras = extras)
                

            else: #Pergunta não encontrada no FAQ
                print(f"INFO(AUX_FAQ:generate_gpt) - {conversa.contato.id_contato}: aux_FAQ[Pergunta] == None -> Nenhuma informação encontrada")
                response = first_response
        
        input_3_5 = float(osFunc('INPUT_3_5_TURBO'))
        output_3_5 = float(osFunc('OUTPUT_3_5_TURBO'))
        input_4_T = float(osFunc('INPUT_4_TURBO'))
        output_4_T = float(osFunc('OUTPUT_4_TURBO'))
        input_4 = float(osFunc('INPUT_4'))
        output_4 = float(osFunc('OUTPUT_4'))
        
        if agent.model == "gpt-3.5-turbo-1106":
            preco_input = input_3_5
            preco_output = output_3_5
        elif agent.model == "gpt-4-1106-preview":
            preco_input = input_4_T
            preco_output = output_4_T
        else:
            preco_input = input_4
            preco_output = output_4
    
        preco = round(((tokens_input*preco_input + tokens_output*preco_output)/1000), 5)
        return response, function_name, function_response, preco
    
    except openai.error.OpenAIError as e:
        error_type = "Erro na API do GPT"
        if isinstance(e, openai.error.APIConnectionError):
            error_type = "Erro na conexão com o GPT"
        elif isinstance(e, openai.error.RateLimitError):
            error_type = "Erro de RateLimit do GPT"
        
        traceback_str = traceback.format_exc()
        print(f"ERROR(generate_gpt) - {conversa.contato.id_contato}: OpenAI API error: {error_type}, Details: {e}", "TRACEBACK:",traceback_str)
    
        function_name = ""
    
        response = "Desculpe não entendi, você poderia repetir?"
        return response, "", "", 0
    
    except Exception as e:
        traceback_str = traceback.format_exc()
        print(f"ERROR(generate_gpt) - {conversa.contato.id_contato}:TRACEBACK:",traceback_str)
        response = "Desculpe não entendi, você poderia repetir?"
        return response, "", "", 0
    

def get_FAQ(conversa, http_session):
    try:
        categoria, error, preco_1 = get_FAQ_category(conversa, http_session)
        label, error, answer, preco_2 = get_FAQ_answer(categoria, conversa, http_session)
        print(f"INFO(get_FAQ) - {conversa.contato.id_contato}: Resposta FAQ - categoria = {categoria}, label = {label}, answer = {answer}")
        return {"Pergunta": label, "Resposta": answer}
    except Exception as e:
        print(f"ERROR(get_FAQ) - {conversa.contato.id_contato}:", e)
        return {"Pergunta": "None", "Resposta": "None"}

def get_FAQ_category(conversa, http_session):
    categoria, error, preco = gera_resposta("CLASSIFICADOR_FAQ", conversa, http_session)
    return categoria, error, preco

def get_FAQ_answer(category, conversa, http_session):
    if category == "CONTINUAR AGENDAMENTO":
        preco = 0
        return "None", False, "None", preco
    ultima_mensagem_user = conversa.get_ultima_msg(autor = 1)
    str_grupo_faq = get_grupo_faq(conversa.campanha)
    df = query_to_dataframe(f"SELECT * FROM FAQ WHERE ID_GRUPO_FAQ_CAMPANHA IN ({str_grupo_faq});")
    faq_db = df.set_index('Question')['Answer'].to_dict()
    candidate_labels = list(faq_db.keys())
    extras = {"question":ultima_mensagem_user, 'candidate_labels':candidate_labels}
    DatabaseConnection.dispose_engine() 
    print(f'INFO(get_FAQ_answer:extras->input gera_resposta(nome_contexto = "GET_RESPOSTA_FAQ"))-:{conversa.contato.id_contato}', extras)
    label, error, preco = gera_resposta("GET_RESPOSTA_FAQ", conversa, http_session, extras=extras)
    answer = faq_db.get(label[0])

    if '{telefone_unidade}' in answer:
        unidade = conversa.contato.agendamento.unidade_agendamento
        telefone = unidade.telefone
        answer = answer.replace('{telefone_unidade}', telefone) 

    return label[0], error, answer, preco

def get_grupo_faq(id_campanha):
    id_grupos_df = query_to_dataframe(f'SELECT IDS_GRUPOS FROM FAQ_POR_CAMPANHA WHERE ID_CAMPANHA = {id_campanha}')
    if not id_grupos_df.empty:
        ids_grupos_string = id_grupos_df['IDS_GRUPOS'].iloc[0]
    else:
        ids_grupos_string = 'Nenhum grupo encontrado para esta campanha'
    return ids_grupos_string
    
def get_string_conversa(discussion_input, n=1):
    """
    Gera uma string formatada para a última mensagem do assistente e mensagens subsequentes, até o n-ésimo assistente.

    Parâmetros:
    - discussion_input (list): Uma lista de dicionários representando a conversa.
    - n (int): O número do assistente para o qual você deseja obter a mensagem. O padrão é 1.

    Retorna:
    - string_conversa (str): Uma string formatada representando a conversa do n-ésimo assistente em diante.
    Se não houver assistentes suficientes, retorna None.

    Exemplo de uso:
    discussion = [
        {'role': 'user', 'content': 'Olá'},
        {'role': 'assistant', 'content': 'Oi!'},
        {'role': 'user', 'content': 'Como você está?'},
        {'role': 'assistant', 'content': 'Estou bem, obrigado!'},
        {'role': 'user', 'content': 'Que bom!'},
    ]
    string_conversa = get_string_conversa(discussion, n=2)
    print(string_conversa)
    """

    # Copia a lista de discussão para não modificar a original
    discussion = discussion_input.copy()
    # Mapeamento de papéis para uma melhor legibilidade
    role_mapping = {'user': 'Cliente', 'assistant': 'Atendente'}

    # Encontra os índices das mensagens do assistente
    assistant_indices = [i for i, entry in enumerate(discussion) if entry['role'] == 'assistant']

    if len(assistant_indices) == 0:
        nth_last_i = 0
        
    # Verifica se há assistentes suficientes
    elif len(assistant_indices) < n:
        # Se não houver, ajusta n para o número total de assistentes
        n = len(assistant_indices)
        # Obtém o índice da n-ésima última mensagem do assistente
        nth_last_i = assistant_indices[-n]
    
    else:
        # Obtém o índice da n-ésima última mensagem do assistente
        nth_last_i = assistant_indices[-n]


    # Obtém a sublista da conversa a partir do índice da n-ésima última mensagem do assistente
    discussion_aux = discussion[nth_last_i:]

    # Formata a string de conversa
    string_conversa = "\n".join(
        f"{role_mapping.get(entry['role'], '')}: {entry.get('content', '')}"
        for entry in discussion_aux
    ).strip()

    return string_conversa
    
def clean_encoding(string):
    # Check if '\u' is present in the string
    if '\\u' in string:
        # Handle Unicode escape sequences
        string = string.encode('utf-8').decode('unicode-escape')
    # elif re.search(r'&#[0-9]+;', string):
    #     string = unescape(string)
    # Load the JSON again after handling Unicode escape sequences
    return string