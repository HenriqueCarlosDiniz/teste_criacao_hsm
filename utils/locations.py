import requests
import math
from datetime import datetime, timedelta
import json 
import pytz
from utils.api_communication import GET_unidades_ativas, GET_unidades_proximas
import os

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calcula a distância entre dois pontos na superfície da Terra usando a fórmula de Haversine.
    
    Parâmetros:
    - lat1 (float): Latitude do primeiro ponto em graus.
    - lon1 (float): Longitude do primeiro ponto em graus.
    - lat2 (float): Latitude do segundo ponto em graus.
    - lon2 (float): Longitude do segundo ponto em graus.
    
    Retorna:
    - distance (float): Distância entre os dois pontos em quilômetros.
    
    Nota:
    Esta função assume que a Terra é uma esfera perfeita com raio de 6371 km.
    """
    lat1 = float(lat1)
    lat2 = float(lat2)
    lon1 = float(lon1)
    lon2 = float(lon2)
    R = 6371  # Raio da Terra em km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * 
         math.sin(dlon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance


def get_location(address, api_key):
    """
    Obtém a cidade e o estado de um endereço usando a API de geocodificação do Google Maps.

    Parâmetros:
    - address (str): O endereço para o qual a cidade e o estado são desejados.
    - api_key (str): A chave de API para acessar a API do Google Maps.

    Retorna:
    - str: Uma string formatada contendo a cidade e o estado do endereço fornecido. Se não for possível determinar,
      retorna uma mensagem de erro.
    """
    
    # Endpoint da API de geocodificação do Google Maps
    endpoint = "https://maps.googleapis.com/maps/api/geocode/json"

    # Parâmetros da solicitação
    params = {
        'address': address,  # Endereço que queremos pesquisar
        'key': api_key       # Chave de API para autenticar nossa solicitação
    }

    # Faça a solicitação à API
    response = requests.get(endpoint, params=params).json()

    # Pegue os resultados da resposta. 'results' é uma lista de possíveis correspondências para o endereço fornecido.
    results = response.get('results', [])

    if not results:
        return "Não foi possível encontrar o endereço", "None"

    # 'address_components' é uma lista de componentes do endereço, como rua, cidade, estado, país, etc.
    address_components = results[0].get('address_components', [])

    city, state = None, None
    for component in address_components:
        # 'administrative_area_level_2' geralmente se refere à cidade ou município
        if 'administrative_area_level_2' in component['types']:
            city = component['long_name']
        
        # 'administrative_area_level_1' geralmente se refere ao estado ou província
        if 'administrative_area_level_1' in component['types']:
            state = component['short_name']

    if city and state:
        return city, state
    else:
        return "Não foi possível determinar a cidade e o estado", "None"

def get_lat_lon_from_address(api, address, api_key):
    '''
    Retorna a latitude e a longitude de um endereço passado para a API.
    api (str) - entradas possíveis: "maps" ou "nominatim"
    address(str) - string contendo o endereço completo do local
    api_key(str) - Nossa chave para usar a api do google maps
    '''

    if api == 'maps':
        base_url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            "address": address,
            "key": api_key
        }
        response = requests.get(base_url, params=params)
        data = response.json()
        if data['status'] == "OK":
            location = data['results'][0]['geometry']['location']
            return location['lat'], location['lng']
        else:
            return None, None
    elif api == 'IQLocation':
        """
        Retorna as coordenadas geográficas de um endereço usando a API da LocationIQ.

        :param endereco: String contendo o endereço a ser pesquisado.
        :param chave_api: Sua chave API da LocationIQ.
        :return: Uma tupla (latitude, longitude) ou None se não for encontrado.
        """
        base_url = "https://us1.locationiq.com/v1/search.php"
        params = {
            'key': api_key,
            'q': address,
            'format': 'json',
            'limit': 1  # Limita os resultados para apenas o mais relevante
        }

        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()  # Lança um erro para respostas não bem-sucedidas

            data = response.json()
            if data:
                return (float(data[0]['lat']), float(data[0]['lon']))
            else:
                return None
        except requests.RequestException as e:
            return None
    else:
        base_url = "https://nominatim.openstreetmap.org/search"
        params = {
            'q': address,
            'format': 'json'
        }
        response = requests.get(base_url, params=params)
        data = response.json()
        if not data:
            return None, None
    
        latitude = data[0]['lat']
        longitude = data[0]['lon']
        return latitude, longitude  

def get_lat_lon(address, api_key):
    lat, lng = get_lat_lon_from_address('maps', address, api_key[0])
    if lat == None or lng == None: 
        print("WARNING(locations.get_lat_lon): API Maps deu errado - Tentando Maps de novo")
        lat, lng =  get_lat_lon_from_address("maps", (address + ", Brasil"), api_key[0])
        if lat == None or lng == None: 
            print("WARNING(locations.get_lat_lon): API Maps deu errado de novo - Usando IQLocation")
            lat, lng = get_lat_lon_from_address("IQLocation", address, api_key[1])
            if lat == None or lng == None: 
                print("WARNING(locations.get_lat_lon): API IQLocation deu errado - Usando nominatin")
                lat, lng = get_lat_lon_from_address("nominatin", address, api_key)
    return lat, lng

def get_all_units(http_session):
    data_all = GET_unidades_ativas(http_session)
    data_all = json.loads(data_all.text)
    retorno = {}
    for value in data_all:
        retorno[value['grupoFranquia']] = {"Endereço": value['enderecoFranquia'] + ',' + value['numeroFranquia'], 
                                     "Latitude": value['latitude'],
                                     "Longitude": value['longitude'],
                                     "Nome": converter_nome(value['nomeFranquia'])
        }
    retorno.pop("PSD-TELEV", None)
    http_session.close()
    return retorno

def converter_nome(nome):
        """
        Converte o nome da unidade para um formato específico.

        :param nome: O nome original da unidade.
        :type nome: str

        :return: O nome convertido para o formato específico, corrigindo nomes errados e eliminando a palavra "Unidade"
        :rtype: str
        """
        # Condições para verificar e converter o nome da unidade.
        if nome == "Pés Sem Dor | Matriz Av. Paulista":
            return 'Paulista'
        elif nome == "Pés Sem Dor | Flex - Botucatu":
            return "Botucatu"
        elif nome == "Niterói ":
            return "Niterói"
        else:
            # Removendo os primeiros 14 caracteres e substituindo 'Unidade ' por uma string vazia.
            return nome[14:].replace('Unidade ', '')

def unidades_indisponiveis(http_session):
        
    # Pega a lista com todas as unidades disponiveis e fica so com as siglas 
    unidades = get_all_units(http_session)
    lista_siglas = []
    for i in unidades: 
        lista_siglas.append(i)
    
    # Pega a data atual e 15 dias pra frente 
    n = 7 
    data_inicial = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d")
    data_final = (datetime.now(pytz.timezone('America/Sao_Paulo')) + timedelta(days=n)).strftime("%Y-%m-%d")

    # Funcao que checa se tem algum horario com uma vaga ou mais 
    def checar_vagas_disponiveis(dicionario):
        for data in dicionario:
            for horario in dicionario[data]:
                if dicionario[data][horario]["total"] >= 1:
                    return True 
        return False
    
def agendamento_passado(data_agendamento, horario_agendamento, unidade):
    try:
        
        try:
            with requests.Session() as session:
                unidades_ativas = GET_unidades_ativas(session)
            unidades_ativas = json.loads(unidades_ativas.text)
            timezone = get_timezone_for_unidade(unidade, unidades_ativas)
        except Exception as e:
            print(f"ERROR(locations.agendamento_passado): Erro aopegar timezone, usando Rio_Branco: {e}")
            timezone = 'America/Rio_Branco'

        current_datetime = datetime.now(pytz.timezone(timezone))
        agendamento_datetime = pytz.timezone(timezone).localize(datetime.strptime(f"{data_agendamento} {horario_agendamento}", "%Y-%m-%d %H:%M"))

        return agendamento_datetime < current_datetime
    except Exception as e:
        print(f"ERROR(locations.agendamento_passado): Erro ao determinar agendamento passado, considerando que no futuro: {e}")
        return False

def get_timezone_for_unidade(unidade, unidades_ativas):
    for unidade_ativa in unidades_ativas:
        if unidade_ativa.get('grupoFranquia') == unidade:
            return unidade_ativa.get('fuso_horario_local', 'America/Rio_Branco')
    return 'America/Rio_Branco'

def get_sigla_ddd(telefone):
    ddd = str(telefone)[2:4]
    coordenadas_ddd = obter_coordenadas_ddd(ddd)
    if coordenadas_ddd:
        unidades_proximas = GET_unidades_proximas(coordenadas_ddd['latitude'], coordenadas_ddd['longitude'])
        if unidades_proximas:
            unidades_proximas = json.loads(unidades_proximas.text)
            unidade_sigla = unidades_proximas['grupoFranquia']
            return unidade_sigla
    return 'PSD'

def obter_coordenadas_ddd(ddd):
    caminho_arquivo = obter_caminho_arquivo('coordenadas_ddd.json')
    with open(caminho_arquivo, 'r') as arquivo:
        dados = json.load(arquivo)
        return dados.get(ddd)

def obter_caminho_arquivo(nome_arquivo):
    # Obter o diretório do script em execução
    diretorio_script = os.path.dirname(os.path.realpath(__file__))
    # Construir o caminho completo para o arquivo
    caminho_arquivo = os.path.join(diretorio_script, nome_arquivo)
    return caminho_arquivo