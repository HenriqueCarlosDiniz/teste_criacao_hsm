from utils.config import *

import openai
import traceback
from datetime import datetime, timedelta
import pytz

from utils.locations import get_lat_lon, haversine_distance, get_location, get_all_units
from utils.text import parse_optimized_date, corrige_nome_unidade, formatar_data, next_weekday
from utils.api_communication import get_lista_unidades_lat_long, buscar_unidades, GET_unidades_ativas
from Models.unidade import Unidade 

if osFunc('Ambiente_Lambda'):  from context import data_formatada_com_soma, datas_futuras_formatadas
else: from geraResposta.context import data_formatada_com_soma, datas_futuras_formatadas

import json


############################- A Fazeres ###################################

# - Arrumar as funções que podem ser chamadas da API da pés (testing)
# - Ver se alguma função pode ser substituida/retirada - principalmente a locations.py e a text.py (testing)
# - Na ATU atualizar os valores padrão (testing)
# - Checar se a saida da GT_unidade_proxima ta certa  (testing) 
# - Documentar as mudanças  (check)
# - Ver sobre o tratamento de siglas e nomes das unidades (testing)
# - Tratar caso de usuário dizer uma unidade inexistente
# - Ver se a Find_nearest ta funcionando direito 
# - Ver se as siglas tão funcionando direito 

###########################################################################


GOOGLE_API  = osFunc('API_MAPS') 
    
def find_nearest_units_alternativas(address, http_session, conversa = None):

    """
    Encontra as três unidades mais próximas de um endereço fornecido.
    
    Parâmetros:
    - address (str): O endereço do qual queremos encontrar as unidades mais próximas.
    
    Retorna:
    - str: Uma string contendo os nomes das unidades mais próximas, separados por vírgulas.
    """ 
    
    api_key = [os.environ["API_MAPS"], os.environ["API_IQLocation"]]
    sigla_cliente = conversa.contato.agendamento.unidade_agendamento.sigla
    unidade_cliente = Unidade(sigla_cliente)
    _, estado = get_location(unidade_cliente.endereco, api_key[0])

    unidades = get_all_units(http_session)

    lista_unidades = [value['Nome'] for value in unidades.values()]
    unit_sigla = corrige_nome_unidade(address, lista_unidades, http_session)
    if unit_sigla:
        address = Unidade(unit_sigla).endereco
        
    lat, lng = get_lat_lon(address, api_key)
    if lat is None or lng is None:
        return f"Unidade: {unidade_cliente.nome}, Endereço: {unidade_cliente.endereco}"

    dist_max = 500

    if (haversine_distance(lat, lng, float(unidade_cliente.latitude), float(unidade_cliente.longitude)) > dist_max) and not (conversa.tipologia_campanha):
        address = address + f", próximo ao estado de {estado}, Brasil."
        lat, lng = get_lat_lon(address, api_key)
        if lat is None or lng is None:
            return f"Unidade: {unidade_cliente.nome}, Endereço: {unidade_cliente.endereco}"
    elif conversa.tipologia_campanha:
        address = address + ", Brasil."
        lat, lng = get_lat_lon(address, api_key)
        if lat is None or lng is None:
            return f"Unidade: {unidade_cliente.nome}, Endereço: {unidade_cliente.endereco}"
        
    # Lista das unidades
    lista_siglas = get_lista_unidades_lat_long(http_session)
                                                                    
    # Ordenar as unidades por distância
    dist_max_unidade = 100 #Distância máxima entre endereço informado e endereço da unnidade
    quantidade = 3 #Quantidade de unidades mais próximas a serem sugeridas
    distances = []
    for unidade_sigla,unidade_latitude,unidade_longitude in lista_siglas:
        distance = haversine_distance(lat, lng, float(unidade_latitude), float(unidade_longitude))
        distances.append((unidade_sigla, distance))
    distances.sort(key=lambda x: x[1])

    #Filtrar apenas as unidades mais próximas com horário disponível
    top_units = [] 
    for unit_sigla, distance in distances:
        if len(top_units) == quantidade:
            break
        
        unit = Unidade(unit_sigla)
        
        lista_horarios = unit.agenda
        if lista_horarios and len(top_units) < quantidade: #Se a unidade não tiver horários disponíveis, pular! Parar quanto atingir a quantidade necessária
            if len(top_units) < 1: #Se ainda não tiver nenhuma unidade na lista top_units, adicionar sem verificar distancia
                top_units.append(unit) 
            else: #Se já tiver pelo menos 1 unidade na lista, verificar distância para não sugerir unidades muito distantes
                if distance <  dist_max_unidade:
                    top_units.append(unit) 
                else:
                    break

    return top_units


def find_nearest_units(address, http_session, conversa = None):

    """
    Encontra as três unidades mais próximas de um endereço fornecido.
    
    Parâmetros:
    - address (str): O endereço do qual queremos encontrar as unidades mais próximas.
    
    Retorna:
    - str: Uma string contendo os nomes das unidades mais próximas, separados por vírgulas.
    """
    
    # resgatando as unidades ativas
    unidades_ativas = GET_unidades_ativas()
    unidades_ativas = json.loads(unidades_ativas.text)
    unidades_ativas = [unidade['grupoFranquia'] for unidade in unidades_ativas]
    
    if isinstance(address, str): 
        address = address.replace("/", " ").replace("\\", " ")
        if address in ['São Paulo','sao paulo', 'são paulo', 'SP', 'sp']:
            address = 'São Paulo, SP'
        elif address in ['Rio de janeiro', 'Rio de Janeiro', 'RJ', 'rio de janeiro', 'rio', 'Rio']: ### Cidades com nomes equivalentes de Estados, onde temos mais de uma unidade
            address = 'Rio de Janeiro, RJ'

    
    unidades = get_all_units(http_session)
    lista_unidades = [value['Nome'] for value in unidades.values()]
    unit_sigla = corrige_nome_unidade(address, lista_unidades, http_session)
    if unit_sigla:
        address = Unidade(unit_sigla).endereco.replace("/", " ").replace("\\", " ")

    # Resgatando as unidades mais próximas do endereço pela api do PSD2        
    unidades_json = buscar_unidades(address, qtd_unidades = 3)

    #tentativa de resgate das unidades pelo ddd
    if unidades_json == {}:
        print(f"INFO(find_nearest_units): Função de busca pela API da PSD com ")
        ddd = str(conversa.contato.id_contato)[2:4]
        df_estado = query_to_dataframe(f"SELECT UF FROM DDD_ESTADO WHERE DDD = {ddd}")
        estado = df_estado['UF'].iloc[0] 
        unidades_json = buscar_unidades(address + f', {estado}', qtd_unidades = 3)
        
    #filtrando a unidade PSD-TELEV
    if any(unidade['grupoFranquia'] == 'PSD-TELEV' for unidade in unidades_json):
        unidades_json = buscar_unidades(address, qtd_unidades = 4)
        unidades_json = [unidade for unidade in unidades_json if unidade['grupoFranquia'] != 'PSD-TELEV']
        
    # verificando se a api retornou o resultado sem erros
    if not (unidades_json == {}): 
        print(f"INFO(find_nearest_units): Função de busca pela API da PSD")
        top_units = []

        for unidade in unidades_json:
            
            if unidade['grupoFranquia'] not in unidades_ativas:
                continue
            
            unit = Unidade(unidade['grupoFranquia']) 
            lista_horarios = unit.agenda

            # verifica se a unidade possui horario
            if not lista_horarios: 
                continue
            # limite de 3 unidades
            elif len(top_units) <= 3: 
                top_units.append(unit)
            else:
                break
        
        

    else:
        print(f"INFO(find_nearest_units): Função de busca alternativa")    
        top_units = find_nearest_units_alternativas(address, http_session, conversa)

    # Gerar resposta da função
    print(f"INFO(find_nearest_units): Endereço considerado na busca: {address}")            
    nearest_units = [f'Unidade: {unit.nome}, Endereço: {unit.endereco}' for unit in top_units]

    print(nearest_units)
    
    return nearest_units


def available_times_updated(date_list, unit,  http_session, number_clients = 1, weekday = [], time=None, period=None, conversa = None):
    
    ########################################################################### 

    # - Qualquer mudança nos returns aqui tem que ser alterada no filtro também 

    ########################################################################### 

    """
    Retorna os horários disponíveis para agendamento em uma unidade específica em uma data específica.

    A função verifica a disponibilidade de horários para agendamento em uma unidade específica na data informada.
    Ela realiza essa checagem acessando dados de um sistema de gerenciamento de unidades e processa a disponibilidade de acordo com os parâmetros fornecidos.

    A função também é capaz de sugerir alternativas se a unidade desejada não estiver disponível ou se a data 
    solicitada não tiver horários disponíveis, podendo oferecer a unidade mais próxima ou a próxima data disponível.
    
    Parâmetros:
    - date (str): A data desejada para o agendamento no formato "aaaa-mm-dd".
    - unit (str): O nome da unidade para a qual se deseja verificar a disponibilidade.
    - time (str, opcional): Um horário específico no formato "HH:MM". Se fornecido, a função tentará retornar os horários mais próximos a ele.
    - period (str, opcional): Um período do dia ("manha", "tarde", "noite") para filtrar os horários disponíveis.
    - cliente (objeto, opcional): Informações do cliente que podem ser usadas para sugerir a unidade mais próxima em caso de indisponibilidade.

    Retorna:
    - str: Uma string formatada com os horários disponíveis ou alternativas de agendamento conforme os critérios fornecidos.

    Notas:
    - A função possui lógica para sugerir a próxima data disponível ou a última data disponível, caso a data especificada não esteja disponível.
    - Se a unidade desejada não estiver disponível para agendamento, a função é capaz de sugerir a unidade mais próxima ao cliente.
    - O endereço da unidade é incluído na mensagem de retorno quando há horários disponíveis.
    """
    
    try:  
        # se o cliente não tenha sugerido nenhuma data, sugira uma data apenas 
        # {"motivos":, "sugestao":}
        unidades = get_all_units(http_session)
        lista_siglas = unidades.keys()
        lista_unidades = [value['Nome'] for value in unidades.values()]
        agendamento = conversa.contato.agendamento
        # #print('resultado_conversa:', result)
        data_agendamento = agendamento.data_agendamento
        hora_agendamento = agendamento.horario_agendamento

        # Checa se foi passada a sigla ao inves da unidade 
        if unit in lista_siglas: 
            unit_sigla = unit
            unit = unidades[unit_sigla]['Nome']
        else: 
            unit_sigla = corrige_nome_unidade(unit, lista_unidades, http_session)

        # Localizar a unidade especificada.
        if unit_sigla == None:
            closest_unit = find_nearest_units(unit, http_session, conversa)
            # #print(f"A unidade {unit} não está disponível para Agendamento, ofereça as unidades: {closest_unit}.")
            return f"A unidade {unit} não está disponível para Agendamento, ofereça as unidades: {closest_unit}."
        
        # Buscar datas disponíveis para a unidade (somente até 14 dias no futuro)
        unidade = Unidade(unit_sigla)
        unidade.get_agenda(unidade.sigla, n_clientes = conversa.agendamento_multiplo)
        available_dates = unidade.agenda 
        available_dates = sorted(list(available_dates.keys()))

        result_date = []
        result_weekday = []
        result_time = []
        result_period = []
        for i, date in enumerate(date_list):
            date_original = date
            # 3. Localizar a data especificada dentro dessa unidade.
            date = parse_optimized_date(date)
            data_disponivel = False
            if date in unidade.agenda.keys():
                data_disponivel = True 

            # 3.1. Verificar dia da semana especificado
            extra_info = ""
            if i < len(weekday) and weekday[i] in ['segunda','terça','terca','quarta','quinta','sexta','sabado','sábado','domingo']:
                weekday_name, weekday_date = next_weekday(weekday[i], date)
                if weekday_date in available_dates and unidade.agenda[weekday_date] == []:
                    weekday_date = formatar_data(weekday_date)
                    result_weekday.append(f"Para {weekday_name} dia {weekday_date} não há disponibilidade de horário, pergunte se o cliente deseja verificar disponibilidade para {weekday_name} da próxima semana." )
                    ### WEEKDAY NÃO DISPONÍVEL

            #3.4. Verificar se data está a mais de 14 dias no futuro -> não pode agendar
            if date != '0001-01-01':
                data_fornecida = pytz.timezone('America/Sao_Paulo').localize(datetime.strptime(date_original, '%Y-%m-%d'))
            elif int(date.split('-')[0]) != 2024:
                data_fornecida = pytz.timezone('America/Sao_Paulo').localize(datetime.strptime(date_original, '%Y-%m-%d'))
            else:
                data_fornecida = datetime.strptime(date, '%Y-%m-%d')
            data_atual = datetime.now(pytz.timezone('America/Sao_Paulo'))
            if data_fornecida > (data_atual + timedelta(days=14)):
                # extra_info = "Não fazemos Agendamentos com mais de 14 dias de antecedência."
                return f"Para o dia {date} não será possível agendar, pois não fazemos Agendamentos com mais de 14 dias de antecedência."

            if not data_disponivel or unidade.agenda[date] == []: 
                # Verificar próxima data disponível (antes ou após data solicitada)
                next_available_date = None
                last_available_date = None
                # Iterating over available dates for the specified unit
                for data_disp in available_dates:
                    if not unidade.agenda[data_disp] == []: #Se a data não estiver vazia
                        # Convert the date string to datetime format
                        data_disponivel = datetime.strptime(data_disp, '%Y-%m-%d')
                        # Check if the available date is in the future
                        if data_disponivel > datetime.strptime(date, '%Y-%m-%d'):
                            next_available_date = data_disponivel.strftime('%Y-%m-%d')
                            break  # Found the next available date, so break the loop
                        # Check if the available date is in the past
                        elif data_disponivel < datetime.strptime(date, '%Y-%m-%d'):
                            last_available_date = data_disponivel.strftime('%Y-%m-%d')   
                
                date = formatar_data(date)
                # Generate the response based on the available dates
                if next_available_date: #Se houver uma data no futuro disponível
                    available_times = sorted(unidade.agenda[next_available_date])
                    next_available_date = formatar_data(next_available_date)
                    # return f"A data {date} não está disponível para Agendamento na unidade {unit}, ofereça a próxima data disponível {next_available_date} nos seguintes horários: {', '.join(available_times)}. {extra_info}"
                    result_date.append(f"A data {date} não está disponível para Agendamento na unidade {unit}, ofereça a próxima data disponível {next_available_date} nos seguintes horários: {', '.join(available_times)}. {extra_info}")
                elif last_available_date: #Se houver uma data no passado disponível
                    available_times = sorted(unidade.agenda[last_available_date])
                    last_available_date = formatar_data(last_available_date)
                    result_date.append(f"A data {date} não está disponível para Agendamento na unidade {unit}, ofereça a última data disponível {last_available_date} nos seguintes horários: {', '.join(available_times)}. {extra_info}")
                else: #Se não houver nenhuma data disponível
                    closest_unit = find_nearest_units(unit, conversa)
                    # #print(f"A unidade {unit} não está disponível para agendamento, ofereça as unidades: {closest_unit}.")
                    result_date.append(f"A unidade {unit} não está disponível para Agendamento, ofereça as unidades: {closest_unit}.")    
            
            #Se a data está disponível, pegar os horários
            if date_original in unidade.agenda.keys():
                available_times = sorted(unidade.agenda[date_original])
            else:
                result_date.append(f"A unidade não abre no dia {formatar_data(date_original)}")
                continue
            try:
                if hora_agendamento and data_agendamento and date == data_agendamento: #Se já tiver agendamento e a data solicitada for a mesma que a data do agendamento
                    if data_agendamento not in available_dates:
                        available_dates.append(data_agendamento)
                        available_dates = sorted(available_dates)
                    if hora_agendamento not in available_times:
                        available_times.append(hora_agendamento)
                        available_times = sorted(available_times)
            except:
                pass

            # 4. Se o horário (time) for especificado, verificar se consta na unidade.
            if time:
                if i >= len(time):
                    i = -1
                if time[i] not in available_times: #Se o horário náo estiver disponível
                    #Verificar próximo dia na qual aquele horário está disponível
                    extra_info = ""
                    # Separar as datas em duas listas: datas após 'date' e datas antes de 'date'
                    dates_after = [d for d in available_dates if d > date]
                    dates_before = [d for d in available_dates if d < date]
                    sorted_dates = sorted(dates_after) + sorted(dates_before, reverse=True)
                    for data_disp in sorted_dates:
                        if not unidade.agenda[data_disp] == [] and time[i] in unidade.agenda[data_disp]: #Se a data não estiver vazia e o horário estiver disponível para a data
                            extra_info = f"Também ofereça o horário {time[i]} {formatar_data(data_disp)}."
                            break

                    date = formatar_data(date_original)
                    result_time.append(f"O horário {time[i]} não está disponível para Agendamento na unidade {unit} {date}. Ofereça os outros horários {date}: {', '.join(available_times)}. {extra_info}")
                
                else: #Se o horário estiver disponível
                    date = formatar_data(date_original)
                    endereco_unidade = unidade.endereco
                    result_time.append(f"O horário {time[i]} está disponível para Agendamento na unidade {unit} {date}. O endereço é: {endereco_unidade}.")

            # 5. Se o período (period) for especificado, retornar os horários disponíveis para esse período.
            if period:
                if period == "manha" or period == "manhã":
                    start_time, end_time = "06:00", "11:59"
                elif period == "tarde":
                    start_time, end_time = "12:00", "23:59"
                else:
                    start_time, end_time = "06:00", "23:59"

                available_times_period = sorted([t for t in available_times if start_time <= t <= end_time])
                if available_times_period == []: # Não há horários disponíveis no período solicitado
                    #Verificar próximo dia na qual aquele período está disponível
                    extra_info = ""

                    dates_after = [d for d in available_dates if d > date]
                    dates_before = [d for d in available_dates if d < date]
                    sorted_dates = sorted(dates_after) + sorted(dates_before, reverse=True)
                    for data_disp in sorted_dates:
                        available_times_temp = sorted(unidade.agenda[data_disp])
                        available_times_period = sorted([t for t in available_times_temp if start_time <= t <= end_time])
                        if not available_times_period == []: #Se a data não estiver vazia e o horário estiver disponível para a data
                            extra_info = f"Também ofereça os horários {', '.join(available_times_period)} no período {period} {formatar_data(data_disp)}."
                            break

                    result_period.append(f"O período {period} não está disponível para Agendamento na unidade {unit} {formatar_data(date_original)}. Ofereça os outros horários {formatar_data(date_original)}: {', '.join(available_times)}. {extra_info}")
                else: # Há horários disponíveis no período solicitado
                    available_times = available_times_period
            
            endereco_unidade = unidade.endereco
            date = formatar_data(date_original)
            result_date.append(f"Para a unidade {unit} no {date}, há disponibilidade somente nos seguintes horários: {', '.join(available_times)}. O endereço é: {endereco_unidade}. Ofereça essa data nesses horários. ")

        info_aux = ''

        if len(result_date) > 0:
            info_aux += "\n Informações relacionadas à data:\n"
            for idx, item in enumerate(result_date, 1):
                info_aux += f"{idx}) {item}\n"

        if len(result_period) > 0:
            info_aux += "\n Informações relacionadas ao período do dia:\n"
            for idx, item in enumerate(result_period, 1):
                info_aux += f"{idx}) {item}\n"

        if len(result_time) > 0:
            info_aux += "\n Informações relacionadas ao horário pedido:\n"
            for idx, item in enumerate(result_time, 1):
                info_aux += f"{idx}) {item}\n"

        if len(result_weekday) > 0:
            info_aux += "\n Informações relacionadas ao dia da semana pedido pelo cliente:\n"
            for idx, item in enumerate(result_weekday, 1):
                info_aux += f"{idx}) {item}\n"
        return info_aux
    except:
        traceback_str = traceback.format_exc()
        print("ERROR(available_times_updated):TRACEBACK:",traceback_str)
        return f"Para a unidade {unit} no {date}, não há horários disponíveis. Pergunte ao cliente se deseja verificar outra data ou outra unidade."
        
functions_noshow = [
        {
        "name": "find_nearest_units",
        "description": "Acha as unidades mais proximas do endereço ou referência geográfica. Utilizar essa função quando o usuário perguntar qual unidade existe próxima a uma referência ou pedir para trocar de unidade",
        "parameters": {
            "type": "object",
            "properties": {
            "address": {
                "type": "string",
                "description": "O endereço, localização ou referência geográfica informado pelo usuário."
            }
            },
            "required": ["address"]
        },
        "returns": {
            "type": "list",
                "description": "Retorna uma lista que relaciona as três unidades mais próximas, em ordem de proximidade, e seus endereços com o formato [Unidade - Endereço]"
        }
        }, 
        {
        "name": "available_times_updated",
        "description": f"Retorna os horários disponíveis para uma unidade e data especificadas. Sempre utilizar quando se tratar de horários, datas ou disponibilidades em unidades. Hoje é dia {data_formatada_com_soma()} e os próximos 7 dias são: {datas_futuras_formatadas(7)}. Caso o cliente não especifique um dia requerido, verifique a disponibilidade do dia mais próximo",
        "parameters": {
            "type": "object",
            "properties": {
            "date_list": {
                "type": "array",
                "items": {
                    "type": "string",
                    "format": "date",
                    "description": "Data no formato 'aaaa-mm-dd' para consultar os horários."
                },
                "description": "Vetor de datas especificadas para consultar os horários."
            },
            "unit": {
                "type": "string",
                "description": "Unidade para consultar os horários."
            },
            "number_clients": {
                "type": "integer",
                "description": "Número de clientes que estão sendo agendados."
            },
            "weekday": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": ["segunda", "terça", "quarta", "quinta", "sexta", "sábado", "domingo"]
                },
                "description": "Um vetor de dias da semana emparelhado com o vetor date_list, onde cada elemento representa o dia da semana correspondente à data no mesmo índice do vetor date_list. Por exemplo: ['terça', 'quinta']."
            },
            "time": {
                "type": "array",
                "items": {
                    "type": "string",
                    "pattern": "^([01]?[0-9]|2[0-3]):[0-5][0-9]$"
                },
                "description": "Vetor de horários específicos no formato 'HH:MM'"
            },
            "period": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": ["manhã", "tarde", "noite"]
                },
                "description": "Vetor de períodos do dia. Ex: ['manhã', 'manhã']"
            }
            },
            "required": ["date_list", "unit","weekday"] 
        }
    },
]