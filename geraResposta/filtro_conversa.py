from utils.config import *

import re

from utils.text import next_three_sundays, variacoes_horarios


# Falta: 
# - Adicionar um filtro para os casos que ele agenda sem chamar a ATU
# - Adicionar um filtro de endereços 

def filtro_hardcoded(resposta_gpt):
    # Define os padrões regex para buscar
    patterns = [r"vou verificar", "vamos verificar", "Atendente:", "User:", "um momento", "Cliente:" ,r"\[", r"\]", r"\{", r"\}", r"find_nearest_units", r"available_times_updated"]

    # Usa a função search do módulo re para buscar cada padrão na string resposta_gpt
    return any(re.search(pattern, resposta_gpt, re.IGNORECASE) for pattern in patterns)

def filtra_resposta(response, function_response, function_name):

    filtrado = False

    word_pattern_agendamento = r'Ótimo|ótimo|perfeito|Perfeito|Claro|claro|agendado|Agendado|anotei|Anotei|Entendi!'
    pattern_horario_quebrado = r'((?:[01]?\d|2[0-3])[h:](?!00)[0-5]\d)'

    filtrado, resposta_filtrada = filtro_horario_quebrado(word_pattern_agendamento, pattern_horario_quebrado, response)
    
    # if (not filtrado#RETIRADO POIS AS PESSOAS COM NOME OU SOBRENOME "DOMINGOS" ESTAVAM CAINDO NESSE FILTRO): 
    #     filtrado, resposta_filtrada = filtro_domingo(word_pattern_agendamento, response)
    
    if function_name == "available_times_updated" and (not filtrado): 
        filtrado, resposta_filtrada = filtro_agendamento(function_response, response, word_pattern_agendamento)
    
    return filtrado, resposta_filtrada

def filtro_domingo(word_pattern_agendamento, response):

    # Mantem a resposta original 
    resposta_filtrada = response

    # Indica um agendamento 
    word_match = re.search(word_pattern_agendamento, response)

    # Indica um domingo 
    prox_domingos = next_three_sundays()
    prox_domingos.append("domingo")
    prox_domingos.append("Domingo")
    domingo_match = any(word in response for word in prox_domingos)

    # Checa se agendamento e se domingo 
    agendamento_domingo = bool(word_match and domingo_match)

    # Se há uma agendamento, filtrar a resposta 
    if agendamento_domingo: 
        resposta_filtrada = f"Desculpe, não fazemos atendimentos aos domingos, posso sugerir outra data ?"

    return agendamento_domingo, resposta_filtrada

def filtro_horario_quebrado(word_pattern_agendamento, pattern_horario_quebrado, response):

    # Mantem a resposta original 
    resposta_filtrada = response

    # Indica um agendamento 
    word_match = re.search(word_pattern_agendamento, response)

    # Indica que tem um horario quebrado
    match_horario_quebrado = re.search(pattern_horario_quebrado, response)
    
    # Checa se agendamento e se horario quebrado  
    agendamento_hora_quebrada =  bool(word_match and match_horario_quebrado)

    if match_horario_quebrado: 
        horario_quebrado = match_horario_quebrado.group(0)

    # Se há uma agendamento, filtrar a resposta 
    if agendamento_hora_quebrada: 
        resposta_filtrada = f"Desculpe, mas só fazemos marcações em horários cheios, posso sugerir outro horário?"

    return agendamento_hora_quebrada, resposta_filtrada

def filtro_agendamento(function_response, response, word_pattern_agendamento):
    
    try:
        # Mantem a resposta original 
        resposta_filtrada = response


        # Indica um agendamento 
        word_match = re.search(word_pattern_agendamento, response)

        # Pega os horarios da function_response: 
        horarios = re.findall(r"\b\d{2}:\d{2}\b", function_response)

        # Funcao para adicionar variacoes do horario, 09:00 = 9:00, 9h, 9 horas, etc 
        aumenta_horarios = variacoes_horarios(horarios)

        # Checa se o horario disponivel esta contido na resposta do BOT 
        horario_encontrado = any(horario in response for horario in aumenta_horarios)

        # # Dentro da ATU tem returns que não significam que não ha horario, checar por esses 

        # Se há um agendamento e NÃO há o horario correto ou o horario é indisponivel 
        agendamento_indisponivel = word_match and (not horario_encontrado)
        resposta_filtrada = f"Desculpe, para essa unidade não temos esse horário, existe outra data ou horário que seja conveniente para você?"

        if agendamento_indisponivel: 
            return agendamento_indisponivel, resposta_filtrada
        else:
            return False, response
    except:
        return False, response
    