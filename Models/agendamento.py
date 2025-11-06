import json
from utils.connect_db import query_to_dataframe
from utils.api_communication import POST_buscar_telefone, buscar_unidades
from utils.locations import get_sigla_ddd
import requests
import json, traceback
import os
from datetime import datetime
import pandas as pd
from Models.unidade import Unidade
from utils.config import *

class Agendamento:
    """
    Classe para representar agendamentos.

    Gerencia agendamentos, incluindo aqueles que já ocorreram (NO-SHOW),
    bem como agendamentos futuros, confirmações, cancelamentos e novos agendamentos.
    """

    def __init__(self, id_contato, 
                 id_campanha = None, 
                 unidade_agendamento = None,
                 data_agendamento = None,
                 horario_agendamento = None,
                 nome_amigo = '',
                 nome_especialista = '',
                 dor_principal = '',
                 cidade = '',
                 estado = ''):
        """
        Inicializa uma instância da classe Agendamento.

        Args:
            id_contato (int): O identificador do contato.

        Atributos são inicializados com valores padrão ou obtidos através de métodos específicos.
        """
        self.id_contato = id_contato
        self.telefone = str(id_contato)
        self.id_agendamento = 0
        self.data_agendamento = "0001-01-01"                       
        self.horario_agendamento = "01:01"                         
        self.data_criacao_agendamento = "0001-01-01 01:01:01"   
        sigla = get_sigla_ddd(self.telefone) 
        self.unidade_agendamento = Unidade(sigla)                         
        self.token_confirmacao = "Sem Token"                       # Token utilizado para confirmar (string)
        self.token_cancelamento = "Sem token"                      # Token utilizado para cancelar (string)
        
        if id_campanha is None:
            self.campanha = self.get_campanha()   
        else: self.campanha = id_campanha                     # Nome da campanha (string)
        self.confirmado = False                                    # Confirmacao do agendamento (Bool)
        self.nome_amigo = nome_amigo
        self.nome_especialista = nome_especialista
        self.dor_principal = dor_principal
        
        if (unidade_agendamento == None) or (data_agendamento == None) or (horario_agendamento == None):
            if self.campanha in [osFunc("NOSHOW"), osFunc("CANCELADOS"), osFunc("REMARKETING")]: 
                self.get_dados_agendamento()
            elif self.campanha in [osFunc("CONFIRMACAO"), osFunc("CONFIRMACAO_2H")]:
                #self.get_dados_agendamentoAPI()  PARA TESTES USAR A OUTRA FUNÇÃO 
                self.get_dados_agendamento()
        else: 
            self.id_agendamento = ''
            self.unidade_agendamento = Unidade(str(unidade_agendamento))
            self.data_agendamento = data_agendamento
            self.horario_agendamento = horario_agendamento
        
        if self.campanha == osFunc("INDIQUE_AMIGOS") and nome_amigo == '': # Indique Amigos 
            query_amigo = f"SELECT * FROM CAMP_INDICADOR WHERE TELEFONE = '{self.telefone}'"
            df = query_to_dataframe(query_amigo)
            nome_amigo = df['NOME_AMIGO'][0]
            self.nome_amigo = nome_amigo

        elif self.campanha == osFunc("REMARKETING"): ### REMARKETING ###
            query_remarketing = f"SELECT * FROM CAMP_REMARKETING WHERE TELEFONE = '{self.telefone}'"
            df = query_to_dataframe(query_remarketing)
            nome_especialista = df['NOME_ESPECIALISTA'][0]
            dor_principal = df['DOR_PRINCIPAL'][0]
            self.nome_especialista = nome_especialista
            self.dor_principal = dor_principal

        elif self.campanha == osFunc("LEADS_META"): ### LEADS META ###
            query_leads_meta = f"SELECT * FROM CAMP_LEADS_META WHERE TELEFONE = '{self.telefone}'"
            df = query_to_dataframe(query_leads_meta)
            cidade = df['CIDADE'][0]
            estado = df['ESTADO'][0]
            self.cidade = cidade
            self.estado = estado
            sigla = self.obter_unidade_proxima()
            self.unidade_agendamento = Unidade(sigla) 

    def get_dados_agendamentoAPI(self):
        """
        Obtém os dados do agendamento da API.

        Busca detalhes do agendamento usando o telefone do contato via API.
        """
        try: 
            id_campanha = 0 if self.campanha is None else self.campanha
            agendamento_json = POST_buscar_telefone(self.telefone, id_campanha=id_campanha)
            if agendamento_json["sucesso"]: 
                self.confirmado = agendamento_json["confirmado"]
                self.token_confirmacao = agendamento_json["token_confirmacao"]
                self.token_cancelamento = agendamento_json["token_cancelamento"]
                self.data_criacao_agendamento = agendamento_json["criado_em"]
                data_agendamento = datetime.strptime(agendamento_json["atendimento_em"], '%Y-%m-%d %H:%M:%S')
                self.data_agendamento = data_agendamento.date().strftime('%Y-%m-%d')
                self.horario_agendamento = data_agendamento.time().strftime('%H:%M')
                return True
            
            return False
            
        except Exception as e: 
            print(f"ERROR(agendamento.get_dados_agendamentoAPI)-{self.id_contato}: Erro na API POST_buscar_telefone {e}")
            return False  

    def obter_unidade_proxima(self):
        if self.estado and self.cidade:
            localizacao = buscar_unidades(f"{self.cidade}, {self.estado}, Brasil", 1)
        else:
            return get_sigla_ddd(self.telefone)
        if localizacao:
            if localizacao['grupoFranquia']:
                unidade_sigla = localizacao['grupoFranquia']
                return unidade_sigla
        return get_sigla_ddd(self.telefone)

    def get_dados_agendamento(self):
        """
        Obtém os dados do agendamento do banco de dados.

        Consulta o banco de dados para obter detalhes do agendamento com base no telefone do contato.
        """
        try: 
            query_SQL = f"SELECT * FROM CAMP_AGENDAMENTO WHERE TELEFONE = '{self.telefone}' ORDER BY DATA DESC LIMIT 1;"
            df = query_to_dataframe(query_SQL)
            if not df.empty:
                self.id_agendamento = str(df["ID_AGENDAMENTO"].iloc[0])
                self.unidade_agendamento = Unidade(str(df["UNIDADE_PSD"].iloc[0]))
                self.data_agendamento = df["DATA"].iloc[0].strftime('%Y-%m-%d')
                self.horario_agendamento = str(df["HORARIO"].iloc[0]).split(' ')[2][:5]
        except Exception as e:
            print(f"ERROR(agendamento.get_dados_agendamento)-{self.id_contato}: Erro na get_dados_agendamento {e}")  
    
    def get_campanha(self):
        """
        Obtém a campanha associada ao agendamento.

        Consulta o banco de dados para determinar a campanha mais recente associada ao telefone do contato.
        """
        try: 
            query_SQL = f"SELECT ID_CAMPANHA FROM CONVERSA WHERE TELEFONE = '{self.telefone}' ORDER BY DATETIME_CONVERSA DESC LIMIT 1;"
            df = query_to_dataframe(query_SQL)
            if not df.empty:
                return df["ID_CAMPANHA"].iloc[0]
        except Exception as e:
            print(f"ERROR(agendamento.get_campanha)-{self.id_contato}: Erro na obtenção dos dados da campanha {e}") 
        return None

    def mostrar_detalhes(self):
        """
        Exibe os detalhes do agendamento.

        Imprime no console todos os detalhes relevantes do agendamento.
        """
        print("Detalhes do Agendamento:")
        print(f"ID do Contato: {self.id_contato}")
        print(f"Telefone: {self.telefone}")
        print(f"ID do Agendamento: {self.id_agendamento}")
        print(f"Data do Agendamento: {self.data_agendamento}")
        print(f"Horário do Agendamento: {self.horario_agendamento}")
        print(f"Data de Criação do Agendamento: {self.data_criacao_agendamento}")
        print(f"Unidade do Agendamento: {self.unidade_agendamento}")
        print(f"Token de Confirmação: {self.token_confirmacao}")
        print(f"Token de Cancelamento: {self.token_cancelamento}")
        print(f"Campanha: {self.campanha}")
        print(f"Confirmado: {'Sim' if self.confirmado else 'Não'}")

if __name__ == "__main__":
    ola = Agendamento(id_contato="5511963628298", id_campanha=4)
    print(ola.unidade_agendamento)