from utils.config import *

import re
import pytz
from datetime import datetime

from utils.connect_db import query_to_dataframe, DatabaseConnection
from utils.text import get_data_proxima, data_formatada_com_soma, data_proxima_num, datas_futuras_formatadas
from utils.locations import agendamento_passado
from Models.agente import Agent

if osFunc('Ambiente_Lambda'): 
    from functions import functions_noshow
else:
    from geraResposta.functions import functions_noshow

data_hoje = data_formatada_com_soma()
data_amanha =  data_formatada_com_soma(1)

def get_agent(conversa, nome_contexto = 'ATENDENTE', extras = None, hard_temperature = None):
    context, temperature, functions, model, func_temperature, json_mode, bool_user_string  = get_context(nome_contexto, conversa, extras = extras)
    if hard_temperature is not None:
        temperature = hard_temperature
    return Agent(context=context, temperature=temperature, model=model, functions=functions, func_temperature=func_temperature, json_mode = json_mode, bool_user_string = bool_user_string)

def get_context(nome_contexto, conversa, extras, user_string = False, json_mode = False):
    contato = conversa.contato
    campanha = conversa.campanha
    unidade = contato.agendamento.unidade_agendamento
    unidade.get_agenda(unidade.sigla,conversa.agendamento_multiplo)
    if campanha == osFunc("CONFIRMACAO") and nome_contexto == "CLASSIFICADOR_REPESCAGEM":
        agendamento = contato.agendamento
        if agendamento_passado(agendamento.data_agendamento, agendamento.horario_agendamento, agendamento.unidade_agendamento.sigla):
            #trocar o contexto para nem tentar classificar como confirmar
            nome_contexto = "CLASSIFICADOR_REPESCAGEM_POS_AVALIACAO"
    query_string = f"""SELECT *
    FROM CONTEXTO C
    JOIN STRING_CONTEXTO SC ON C.ID_string_contexto = SC.ID_string_contexto
    WHERE C.id_campanha = {campanha} AND C.nome_contexto = '{nome_contexto}';"""
    
    # Executar a consulta e obter o resultado em um DataFrame
    df = query_to_dataframe(query_string)
    if (df is None) or (df.empty):
        print(f"ERROR(get_context) - {conversa.contato.id_contato}: df vazio, query_string = {query_string}")
        
    if df is not None and not df.empty:
        string_contexto = df['string_contexto'].iloc[0]
        id_user_contexto = df['id_user_contexto'].iloc[0]
        temperature = float(df['temperature'].iloc[0])
        func_temperature = float(df['func_temperature'].iloc[0])
        model = df['model'].iloc[0]
        response_format = df['response_format'].iloc[0]
        function = int(df['functions'].iloc[0])
        if function == 1:
            function = functions_noshow
        if id_user_contexto!=0:
            user_string = True
        if response_format == 'json_mode':
            json_mode = True
        # Regex para encontrar texto dentro de chaves
        padrao_regex = r"\{([^\s{}]*?)\}"
        # Encontrando todas as ocorrências
        string_variaveis = re.findall(padrao_regex, string_contexto)
        texto_modificado = string_contexto
        if 'nome_cliente' in string_variaveis:
            nome_cliente = contato.nome
            texto_modificado = texto_modificado.replace(r'{nome_cliente}', nome_cliente)
        if 'horario_cliente' in string_variaveis:
            horario_cliente = contato.agendamento.horario_agendamento
            texto_modificado = texto_modificado.replace(r'{horario_cliente}', horario_cliente)
        if 'data_cliente' in string_variaveis:
            data_cliente = contato.agendamento.data_agendamento
            texto_modificado = texto_modificado.replace(r'{data_cliente}', data_cliente)
        if 'data_hoje' in string_variaveis:
            texto_modificado = texto_modificado.replace(r'{data_hoje}', data_hoje)
        if 'data_amanha' in string_variaveis:
            texto_modificado = texto_modificado.replace(r'{data_amanha}', data_amanha)
        if 'unidade_cliente' in string_variaveis:
            unidade = contato.agendamento.unidade_agendamento
            unidade_cliente = unidade.nome
            texto_modificado = texto_modificado.replace(r'{unidade_cliente}', unidade_cliente)
        if 'endereco_cliente' in string_variaveis:
            unidade = contato.agendamento.unidade_agendamento
            endereco_cliente = unidade.endereco
            texto_modificado = texto_modificado.replace(r'{endereco_cliente}', endereco_cliente)
        if 'telefone_unidade' in string_variaveis:
            unidade = contato.agendamento.unidade_agendamento
            telefone = unidade.telefone
            texto_modificado = texto_modificado.replace(r'{telefone_unidade}', telefone) 
        if 'nome_amigo' in string_variaveis:
            nome_amigo = contato.agendamento.nome_amigo
            texto_modificado = texto_modificado.replace(r'{nome_amigo}', nome_amigo)
        if 'conversa_inteira' in string_variaveis:
            conversa_inteira = conversa.discussion_to_string()
            texto_modificado = texto_modificado.replace(r'{conversa_inteira}', conversa_inteira)
        if 'data_proxima' in string_variaveis:
            data_proxima = get_data_proxima(unidade, data_amanha)
            texto_modificado = texto_modificado.replace(r'{data_proxima}', data_proxima)
        
        matches = re.findall(r"\{data_proxima_(\d+)\}", texto_modificado)
        for match in matches:
            X = int(match)
            data_proxima = data_proxima_num(X, unidade)
            datas_futuras_str = '; '.join(data_proxima)
            texto_modificado = texto_modificado.replace(f'{{data_proxima_{X}}}', datas_futuras_str)
        # DATAS_FUTURAS_FORMATADAS
        matches = re.findall(r"\{datas_futuras_(\d+)\}", texto_modificado)
        for match in matches:
            X = int(match)
            datas_futuras = datas_futuras_formatadas(X)
            datas_futuras_str = '; '.join(datas_futuras)
            texto_modificado = texto_modificado.replace(f'{{datas_futuras_{X}}}', datas_futuras_str)
        if extras:
            for key in extras.keys():
                texto_modificado = texto_modificado.replace(f'{{{key}}}', str(extras[key]))
                     
    DatabaseConnection.dispose_engine()            
    return texto_modificado, temperature, function, model, func_temperature, json_mode, user_string


def get_user_string(nome_contexto, conversa, extras = None):
    
    # dic_infos_gerais 
    campanha = conversa.campanha
    query_string = f"""
    SELECT US.STRING
    FROM CONTEXTO C
    JOIN USER_STRING US ON C.id_user_contexto = US.ID_STRING
    WHERE C.nome_contexto = '{nome_contexto}'  AND C.id_campanha = {campanha};
    """

    # Executar a consulta e obter o resultado em um DataFrame
    df = query_to_dataframe(query_string)
    # Verificar se o DataFrame não está vazio e exibir o resultado
    if df is not None and not df.empty:
        string_contexto = df['STRING'].iloc[0]
        texto_modificado = string_contexto

        # Regex para encontrar texto dentro de chaves
        padrao_regex = r"\{([^\s{}]*?)\}"

        # Encontrando todas as ocorrências
        string_variaveis = re.findall(padrao_regex, string_contexto)

        if 'ultima_mensagem_bot' in string_variaveis:
            ultima_mensagem_bot = conversa.get_ultima_msg(autor=0)
            texto_modificado = texto_modificado.replace(r'{ultima_mensagem_bot}', ultima_mensagem_bot)
        if 'ultima_mensagem_user' in string_variaveis:
            ultima_mensagem_user =  conversa.get_ultima_msg(autor=1)
            texto_modificado = texto_modificado.replace(r'{ultima_mensagem_user}', ultima_mensagem_user)
        if extras:
            for key in extras.keys():
                texto_modificado = texto_modificado.replace(f'{{{key}}}', str(extras[key]))

    DatabaseConnection.dispose_engine() 
    return texto_modificado

