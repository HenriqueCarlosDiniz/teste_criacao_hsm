import  datetime, re, json
from difflib import SequenceMatcher 
from utils.locations import get_all_units
import unicodedata
import pytz
import locale


def get_data_proxima(unidade, data_amanha): 
    unidade.get_agenda(unidade.sigla)
    available_dates = unidade.agenda.keys() 
    data_proxima = data_amanha
    # Itera sobre as datas disponíveis para aquela unidade
    for data_disponivel in available_dates:
        if not len(unidade.agenda[data_disponivel]) == 0:
            # Converte a string da data para o formato datetime
            data_disponivel = datetime.datetime.strptime(data_disponivel, '%Y-%m-%d')
            # Verifica se a data disponível é hoje ou após
            if data_disponivel.date() >= datetime.datetime.now(pytz.timezone('America/Sao_Paulo')).date():
                # Formata a data no formato DD/MM/YYYY
                data_proxima = data_disponivel.strftime('%Y-%m-%d')
                data_proxima = formatar_data(data_proxima)
                break  # Interrompe o loop após encontrar a primeira data disponível após hoje

    return data_proxima

def nome_p_sigla(unit_name, http_session):
    data = get_all_units(http_session)
    for key in data.keys():
        if data[key]['Nome'] == unit_name:
            return key

def variacoes_horarios(lista_horarios):
    """
    Função para gerar variações de horários a partir de uma lista de horários, incluindo todos os horários da lista.
    """
    lista_corrigida = []
    for horario in lista_horarios:
        # Dividindo as horas e minutos
        horas, minutos = horario.split(':')

        # Adicionando o horário original
        lista_corrigida.append(horario)

        # Adicionando variações do horário
        # Exemplo: "09:00" -> "10:00", "9:00", "9h", "9horas", "9 h", "09h00"
        hora_int = int(horas)
        proxima_hora = f"{hora_int + 1:02d}:00"
        sem_zero = f"{hora_int}:{minutos}" if horas.startswith('0') else horario
        formato_h = f"{hora_int}h"
        formato_horas = f"{hora_int}horas"
        formato_h_espaco = f"{hora_int} h"
        formato_h_minutos = f"{horas}h{minutos}"

        # Adicionando as variações na lista
        lista_corrigida.extend([proxima_hora, sem_zero, formato_h, formato_horas, formato_h_espaco, formato_h_minutos])

    return lista_corrigida

def next_three_sundays():
    """
    Returns the dates of the next three Sundays from the current date in various formats.

    Output:
        - List of strings: Dates in various formats
        
    Example:
    --------
    If today is 16/10/2023, the function will return:
    [
        '22/10/2023', '22/10', 'dia 22', '2023/10/22', '22-10-2023', '10-22-2023', '22-10',
        '29/10/2023', '29/10', 'dia 29', '2023/10/29', '29-10-2023', '10-29-2023', '29-10',
        '05/11/2023', '05/11', 'dia 05', '2023/11/05', '05-11-2023', '11-05-2023', '05-11'
    ]
    """
    # Get the current date
    today = datetime.datetime.today()

    # Calculate how many days are left until the next Sunday from today
    days_until_sunday = 6 - today.weekday()

    # Determine the date of the next Sunday
    next_sunday = today + datetime.timedelta(days=days_until_sunday)

    # List to store the next 3 Sundays in various formats
    formatted_dates = []

    # Append the next 3 Sundays to the list in different formats
    for _ in range(3):
        formatted_dates.extend([
            next_sunday.strftime('%d/%m/%Y'),
            next_sunday.strftime('%d/%m'),
            'dia ' + next_sunday.strftime('%d'),
            next_sunday.strftime('%Y/%m/%d'),
            next_sunday.strftime('%d-%m-%Y'),
            next_sunday.strftime('%m-%d-%Y'),
            next_sunday.strftime('%d-%m')
        ])
        next_sunday += datetime.timedelta(days=7)
    
    return formatted_dates

def formatar_data(date):
    # Transforma de YYYY-MM-DD para DD/MM/YYYY, quinta-feira, X de novembro 
    # Dividir a data em partes (ano, mês, dia)
    parts = date.split('-')
    ano, mes, dia = int(parts[0]), int(parts[1]), int(parts[2])

    # Obter o nome do dia da semana e do mês
    dias_semana = ["segunda-feira", "terça-feira", "quarta-feira", "quinta-feira", "sexta-feira", "sábado", "domingo"]
    meses = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]
    dia_semana_pt = dias_semana[datetime.datetime(ano, mes, dia).weekday()]
    mes_pt = meses[mes - 1]

    # Obter a data atual
    data_atual = datetime.datetime.now(pytz.timezone('America/Sao_Paulo')).date()

    # Verificar se a data é hoje ou amanhã
    if date == data_atual.strftime('%Y-%m-%d'):
        hoje_ou_amanha = "hoje,"
    elif date == (data_atual + datetime.timedelta(days=1)).strftime('%Y-%m-%d'):
        hoje_ou_amanha = "amanhã,"
    else:
        hoje_ou_amanha = ""

    # Formatar a data no formato desejado
    data_formatada = f"{hoje_ou_amanha} dia {dia:02}/{mes:02}/{ano} ({dia_semana_pt})" #, {dia} de {mes_pt}"
    return data_formatada

def corrige_nome_unidade(unit_name, data, http_session):
    max_similarity = 0.65
    # unit_name = unit_name.encode('utf-8')
    closest_unit_name = None
    if unit_name.startswith("Unidade "):
        unit_name = unit_name[len("Unidade "):]
    for name in data:
        similarity = similar(unit_name, name) 
        if similarity > max_similarity:
            max_similarity = similarity
            closest_unit_name = name
    if closest_unit_name == None:
        return None
    return nome_p_sigla(closest_unit_name, http_session)
        
def data_formatada_com_soma(n=0):
    '''
    input: número com n com o número de dias que se quer para o futuro.
    
    output: data no formato adequado (dd/mm/yyyy)
    '''
    # Obtendo a data atual
    agora = datetime.datetime.now(pytz.timezone('America/Sao_Paulo'))
    
    # Se n dias for solicitado, adiciona n dias à data atual
    if n:
        agora += datetime.timedelta(days=n)
    
    # Se o dia resultante for um domingo, adicione um dia para que seja segunda-feira
    if agora.weekday() == 6:
        agora += datetime.timedelta(days=1)
    
    # Listas para traduzir os dias da semana e os meses
    dias_semana = ["segunda-feira", "terça-feira", "quarta-feira", "quinta-feira", "sexta-feira", "sábado", "domingo"]
    meses = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]
    
    # Formatando a data
    dia_semana_pt = dias_semana[agora.weekday()]
    mes_pt = meses[agora.month - 1]
    return f"Dia {agora.day:02}/{agora.month:02}/{agora.year}, {dia_semana_pt}, {agora.day} de {mes_pt}"

def datas_futuras_formatadas(X):
    dias_da_semana = ["Segunda-feira", "Terça-feira", "Quarta-feira", "Quinta-feira", "Sexta-feira", "Sábado", "Domingo"]
    meses = ["", "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", 
             "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]

    data_atual = datetime.datetime.now(pytz.timezone('America/Sao_Paulo'))
    lista_datas = [data_atual + datetime.timedelta(days=i) for i in range(X)]

    return [data.strftime("%d/%m/%Y, ") + dias_da_semana[data.weekday()] + ", " + meses[data.month] for data in lista_datas]

def parse_optimized_date(input_string):
# Dicionários para mapear os nomes dos meses, dias e dias da semana
    months = {
        "janeiro": "01",
        "fevereiro": "02",
        "março": "03",
        "abril": "04",
        "maio": "05",
        "junho": "06",
        "julho": "07",
        "agosto": "08",
        "setembro": "09",
        "outubro": "10",
        "novembro": "11",
        "dezembro": "12"
    }

    days = {
        "primeiro": "01",
        "dois": "02",
        "três": "03",
        "quatro": "04",
        "cinco": "05",
        "seis": "06",
        "sete": "07",
        "oito": "08",
        "nove": "09",
        "dez": "10",
        "onze": "11",
        "doze": "12",
        "treze": "13",
        "quatorze": "14",
        "quinze": "15",
        "dezesseis": "16",
        "dezessete": "17",
        "dezoito": "18",
        "dezenove": "19",
        "vinte": "20",
        "vinte e um": "21",
        "vinte e dois": "22",
        "vinte e três": "23",
        "vinte e quatro": "24",
        "vinte e cinco": "25",
        "vinte e seis": "26",
        "vinte e sete": "27",
        "vinte e oito": "28",
        "vinte e nove": "29",
        "trinta": "30",
        "trinta e um": "31"
    }

    weekdays = {
        "segunda-feira": 0,
        "terça-feira": 1,
        "quarta-feira": 2,
        "quinta-feira": 3,
        "sexta-feira": 4,
        "sábado": 5,
        "domingo": 6
    }

    # Expressões regulares para diferentes formatos de data
    patterns = [
        r"(\d{2})-(\d{2})-(\d{4})",  # DD-MM-YYYY
        r"(\d{2})/(\d{2})/(\d{4})",  # DD/MM/YYYY
        r"(\d{2})-(\d{2})-(\d{4})",  # MM-DD-YYYY
        r"(\d{2})/(\d{2})/(\d{4})",  # MM/DD/YYYY
        r"(\d{4})-(\d{2})-(\d{2})",  # YYYY-MM-DD
        r"(\d{4})/(\d{2})/(\d{2})"   # YYYY/MM/DD
    ]

    day_month_year_pattern = r"(" + "|".join(days.keys()) + r") de (" + "|".join(months.keys()) + r") de (\d{4})"
    next_weekday_pattern = r"na (próxima|proxima)? ?(" + "|".join(weekdays.keys()) + r")( da semana que vem)?"

    # Compilação de expressões regulares para execução mais rápida
    patterns_compiled = [re.compile(pattern) for pattern in patterns]
    day_month_year_pattern_compiled = re.compile(day_month_year_pattern)
    next_weekday_pattern_compiled = re.compile(next_weekday_pattern)

    today = datetime.datetime.now(pytz.timezone('America/Sao_Paulo'))

    # Tratar strings especiais
    if input_string.lower() == "amanhã":
        return (today + datetime.timedelta(days=1)).strftime('%d-%m-%Y')
    elif input_string.lower() == "hoje":
        return today.strftime('%d-%m-%Y')

    # Verificar expressão "next weekday"
    match = next_weekday_pattern_compiled.match(input_string.lower())
    if match:
        _, weekday, next_week = match.groups()
        today_weekday = today.weekday()
        target_weekday = weekdays[weekday]
        days_difference = (target_weekday - today_weekday + 7) % 7
        if days_difference == 0:
            days_difference = 7
        if next_week:
            days_difference += 7
        target_date = today + datetime.timedelta(days=days_difference)
        return target_date.strftime('%d-%m-%Y')

    # Verificar formato escrito por extenso
    match = day_month_year_pattern_compiled.match(input_string.lower())
    if match:
        day, month, year = match.groups()
        return f"{days[day]}-{months[month]}-{year}"

    # Verificar padrões padrão de data
    for pattern in patterns_compiled:
        match = pattern.match(input_string)
        if match:
            year, month, day = match.groups()
            # Verifica se o formato é DD-MM-YYYY e ajusta para YYYY-MM-DD
            if len(year) == 4: #Se "year" for YYYY, está correto
                return f"{year}-{month}-{day}"
            else: #Se "year" for DD, tem que inverter
                return f"{day}-{month}-{year}"
    
    return "FALHEI"

def similar(a, b):
    '''
    input: a, b - strings

    output: indice de similaridade entre as duas strings.
    '''
    # Convert both strings to lowercase to ensure case-insensitivity
    a, b = a.lower(), b.lower()

    # Use SequenceMatcher to compute similarity
    ratio = SequenceMatcher(None, a, b).ratio()
    
    # Account for string lengths
    length_penalty = 1 - abs(len(a) - len(b)) / max(len(a), len(b))
    
    return ratio * length_penalty


    
def next_weekday(weekday, date): # Função para encontrar o próximo dia da semana após uma determinada data
    # Variáveis
    # weekday = 'sabado'  # Substitua isso pela variável que contém o dia da semana desejado
    # date = '2023-11-20'  # Substitua isso pela sua data específica no formato 'YYYY-MM-DD'

    # Mapeamento de nomes de dias da semana para os códigos correspondentes do Python (0 é segunda-feira, 6 é domingo)
    dict_weekday = {'segunda': 0, 'terça': 1, 'terca': 1, 'quarta': 2, 'quinta': 3, 'sexta': 4, 'sabado': 5, 'sábado': 5, 'domingo': 6}

    # Obtemos o código do dia da semana
    desired_weekday = dict_weekday[weekday]

    # Convertendo a data para o formato datetime
    try:
        date_obj = datetime.datetime.strptime(date, '%Y-%m-%d')
    except:
        return 0, "00-00-0000"
    # Calcula a diferença de dias entre o dia informado e o próximo desejado
    days_until_desired_weekday = (desired_weekday - date_obj.weekday() + 7) % 7
    if days_until_desired_weekday == 0:
        next_date = date_obj + datetime.timedelta(days=7)
    else:
        # Calcula a data do próximo dia desejado
        next_date = date_obj + datetime.timedelta(days=days_until_desired_weekday)

    dictweekday = {'segunda': "segunda-feira", 'terça': "terça-feira", 'terca': "terça-feira", 'quarta': "quarta-feira", 'quinta': "quinta-feira", 'sexta': "sexta-feira", 'sabado': "sábado", 'sábado': "sábado", 'domingo': "domingo"}
    
    return dictweekday[weekday], next_date.strftime('%Y-%m-%d')  # Retorna a string da data correspondente ao próximo dia desejado

def data_proxima_num(X, unidade):
    available_dates = unidade.agenda.keys()
    data_proxima = data_formatada_com_soma(X)
    # Itera sobre as datas disponíveis para aquela unidade
    for data_disponivel in available_dates:
        if not len(unidade.agenda[data_disponivel]) == 0:
            # Converte a string da data para o formato datetime
            data_disponivel = datetime.datetime.strptime(data_disponivel, '%Y-%m-%d')
            # Verifica se a data disponível é hoje ou após
            if data_disponivel.date() >= datetime.datetime.now(pytz.timezone('America/Sao_Paulo')).date():
                # Formata a data no formato DD/MM/YYYY
                data_proxima = data_disponivel.strftime('%Y-%m-%d')
                
                data_proxima = formatar_data(data_proxima)
                break  # Interrompe o loop após encontrar a primeira data disponível após hoje
    texto_modificado = texto_modificado.replace(r'{data_proxima}', data_proxima)

def extrair_nomes_unidades(input_strings, http_session):
    # Define the pattern using regular expression
    pattern = r":\s*(.*?),"

    siglas_unidades = []
    # Loop through each input string
    for input_string in input_strings:
        # Use re.search to find the first match
        match = re.search(pattern, input_string)
        
        # Check if there is a match and extract the captured group
        if match:
            nome_unidade = match.group(1)
            siglas_unidades.append(nome_p_sigla(nome_unidade, http_session))
        
    return siglas_unidades

def remover_acentos(s):
    return ''.join((c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn'))

def format_data_feira(date):
    locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')
    
    weekday = date.strftime("%A")
    formatted_date = date.strftime("%d/%m/%Y")
    
    # Append "-feira" if the weekday is not Saturday or Sunday
    if weekday not in ['sábado', 'domingo']:
        weekday += "-feira"
    
    return f"{formatted_date} ({weekday})"

