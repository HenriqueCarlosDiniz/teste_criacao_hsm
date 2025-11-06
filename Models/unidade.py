# Importando as funções necessárias da biblioteca datetime para lidar com datas e timedelta para manipulação de datas.
from datetime import datetime, timedelta
# Importando funções específicas do módulo comunicacaoAPI do pacote utils.
from utils.api_communication import GET_vagas_disponiveis, GET_unidades_ativas
from utils.locations import converter_nome
# Importando a biblioteca json para lidar com dados JSON.
import json
import pytz

# Definindo a classe Unidade.
class Unidade():
    def __init__(self, sigla):
        # Atribuindo a sigla da unidade ao atributo 'sigla' da instância.
        self.sigla = sigla
        # Obtendo informações sobre as unidades ativas e armazenando em 'info_unidades'.]
        info_unidades = self.get_unidades()
        # Atribuindo informações específicas da unidade aos atributos da instância.
        self.nome = info_unidades[sigla]['Nome']
        endereco = info_unidades[sigla]['Endereço']
        self.endereco = json.loads(json.dumps(endereco.replace('<strong>', '').replace('</strong>', '').replace('\n', '').replace('\r', '').replace('"', '')))
        self.latitude = info_unidades[sigla]['Latitude']
        self.longitude = info_unidades[sigla]['Longitude']
        self.telefone = info_unidades[sigla]['Telefone']
        # Obtendo a agenda da unidade.
        self.agenda = self.get_agenda(self.sigla)
        
    def get_agenda(self, unidade, n_clientes = 1):
        """
        Obtém a agenda de disponibilidade de vagas para uma unidade específica.

        :param unidade: A sigla da unidade para a qual se deseja obter a agenda (por exemplo, 'PSD').
        :type unidade: str

        :return: Um dicionário contendo as datas como chaves e listas de horários disponíveis como valores.
                Exemplo: {'2023-12-01': ['09:00', '10:00'], '2023-12-02': ['14:00', '15:00']}
        :rtype: dict
        """
        # Obtendo a data atual no formato 'YYYY-MM-DD'.
        data_hoje = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%Y-%m-%d')
        
        # Convertendo a data atual para um objeto datetime.
        data_obj = datetime.strptime(data_hoje, '%Y-%m-%d')
        
        # Calculando a data daqui a 14 dias.
        data_14 = data_obj + timedelta(days=14)
        
        # Convertendo a data calculada para o formato 'YYYY-MM-DD'.
        data_14 = data_14.strftime('%Y-%m-%d')

        updated_available_dates = GET_vagas_disponiveis(data_inicial=data_hoje, data_final=data_14, unidade=unidade)

        # Convertendo a resposta da API (JSON) para um objeto Python.
        updated_available_dates = json.loads(updated_available_dates.text)
        
        # Chamando o método 'formatar_agenda' para formatar os dados da agenda.
        retorno = self.formatar_agenda(updated_available_dates)

        # if n_clientes > 1:
        #     retorno = self.horarios_seguidos(retorno, n_clientes)
            
        self.agenda = retorno
        return retorno
    
    def horarios_seguidos(self, time_dict, n_clientes):
        result = {}
        for key in time_dict.keys():
            if len(time_dict[key])>1:
                hours = [int(x[:2]) for x in time_dict[key] ]

                available_hours = [] 
 
                for i in range(len(hours) - n_clientes + 1):
                    if hours[i+n_clientes-1] - hours[i] + 1 == n_clientes:
                        if hours[i] < 10:
                            horario = '0' + str(hours[i])
                        else:
                            horario = str(hours[i])

                        available_hours.append(horario + ':00' )
 
                if available_hours != []:
                    result[key] = available_hours
                
        return result


    def formatar_agenda(self, json):
        """
        Formata os dados da agenda recebidos da API do PSD2.

        :param json: Os dados brutos da agenda no formato JSON retornados pela API.
        :type json: dict

        :return: Um dicionário formatado com as datas como chaves e listas de horários disponíveis como valores.
                Exemplo: {'2023-12-01': ['09:00', '10:00'], '2023-12-02': ['14:00', '15:00']}
        :rtype: dict
        """
        # Criando um novo dicionário para armazenar a agenda formatada.
        new_data = {}

        # Iterando sobre as datas no JSON recebido.
        for dia in json.keys():
            # Adicionando uma lista vazia para cada dia no novo dicionário.
            new_data[dia] = []

            # Iterando sobre as horas disponíveis para cada dia.
            for hora in json[dia]:
                # Adicionando a hora formatada (sem os últimos 3 caracteres) à lista do dia.
                new_data[dia].append(hora[:-3])

        # Retornando a agenda formatada.
        return new_data


    # Método para obter informações sobre todas as unidades.
    def get_unidades(self):
        """
        Obtém informações formatadas sobre todas as unidades ativas da franquia PSD2.

        :return: Um dicionário contendo informações formatadas de todas as unidades ativas.
                Cada unidade é representada por uma chave (sigla) e contém um dicionário com detalhes, incluindo endereço, latitude, longitude e nome.
                Exemplo:
                {
                    'PSD': {'Endereço': 'Rua XYZ, 123, CidadeA', 'Latitude': -23.456, 'Longitude': -46.789, 'Nome': 'Unidade A'},
                    'ABC': {'Endereço': 'Rua ABC, 456, CidadeB', 'Latitude': -12.345, 'Longitude': -56.789, 'Nome': 'Unidade B'},
                    ...
                }
        :rtype: dict
        """
        # Obtendo informações sobre todas as unidades ativas.
        data_all = GET_unidades_ativas()
         
        # Convertendo a resposta da API (JSON) para um objeto Python.
        data_all = json.loads(data_all.text)
 
        # Criando um dicionário vazio para armazenar as informações formatadas das unidades.
        retorno = {}

        # Iterando sobre cada unidade nos dados recebidos da API.
        for value in data_all: 
            # Adicionando informações formatadas da unidade ao dicionário 'retorno'.
            retorno[value['grupoFranquia']] = {
                "Endereço": (
                    value.get('enderecoFranquia', '') + ', ' +
                    value.get('numeroFranquia', '') + ', ' +
                    value.get('bairroFranquia', '') + ', ' +
                    value.get('cidadeFranquia', '') + ', ' +
                    value.get('complementoFranquia', '')
                ),
                "Latitude": value.get('latitude', 0.0),
                "Longitude": value.get('longitude', 0.0),
                "Nome": converter_nome(value.get('nomeFranquia', '')),
                "Telefone": value.get('telefoneFranquia', '4003-8033') + ', ou ' + value.get('contatoAdicional', '4003-8033')
            }

        # Retornando o dicionário com as informações das unidades.
        return retorno