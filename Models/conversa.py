# Importando as funções 
from utils.config import *
from utils.connect_db import query_to_dataframe, insert_data_into_db, execute_query_db
import pandas as pd
from datetime import datetime
import pytz

# Definindo a classe Conversa.
class Conversa():
    def __init__(self, contato, campanha = None, datetime_conversa = None, banco_dados = 0):
        self.banco_dados = banco_dados
        self.contato = contato
        self.campanha = campanha
        self.discussion = None
        if datetime_conversa and campanha:
            self.datetime_conversa = datetime_conversa
            self.campanha = campanha
        else:
            self.datetime_conversa, self.campanha = self.get_conversa_recente()
        self.id_conversa = (self.contato.id_contato, self.campanha, self.datetime_conversa)
        
        (self.resultado, 
            self.flag, 
            self.HSM, 
            self.tipologia_campanha, 
            self.completar_cadastro, 
            self.agendamento_cadastrado, 
            self.hsm_conteudo, 
            self.hsm_payload, 
            self.wpp_header, 
            self.qtd_msg,
            self.telefone_wpp,
            self.agendamento_multiplo) = self.initialize()

    def atualiza_agendamento_multiplo(self, new_number):
        execute_query_db(f"UPDATE CONVERSA SET AGENDAMENTO_MULTIPLO = {new_number} WHERE TELEFONE = {self.contato.id_contato} AND DATETIME_CONVERSA = '{self.datetime_conversa}' LIMIT 1")
        self.agendamento_multiplo = new_number

    def atualizar_telefone_wpp(self, new_number):
        execute_query_db("UPDATE CONVERSA SET TELEFONE_WPP = ? WHERE TELEFONE = ? AND DATETIME_CONVERSA = ? LIMIT 1",
                 (new_number, self.contato.id_contato, self.datetime_conversa))
    
    
    def get_HSM_message(self):
        query = f''' SELECT *
                    FROM HSM
                    WHERE ID_HSM = "{self.HSM}"'''
        # Executando a query e obtendo os resultados em um DataFrame.
        df = query_to_dataframe(query)

        message = df.loc[0, 'CONTEUDO']
        
        return message

    def get_discussion(self):
        if self.discussion is not None:
            return self.discussion
        """
        Recupera a discussão (mensagens) com base nos atributos de instância da classe.

        :return: Uma lista de dicionários representando a discussão.
                Cada dicionário contém informações sobre o papel (user ou assistant) e o conteúdo da mensagem.
                Exemplo: [{"role": "user", "content": "Olá, como posso ajudar?"}, {"role": "assistant", "content": "Olá! Eu sou um assistente virtual."}]
        :rtype: list
        """
        # Construindo a query SQL para obter as mensagens da discussão.
        query = f''' SELECT *
                    FROM MENSAGEM
                    WHERE TELEFONE = '{self.id_conversa[0]}' AND ID_CAMPANHA = '{self.id_conversa[1]}' AND DATETIME_CONVERSA = '{self.id_conversa[2]}'
                    ORDER BY MENSAGEM_DATETIME ASC;'''

        # Executando a query e obtendo os resultados em um DataFrame.
        df = query_to_dataframe(query)

        # Inicializando uma lista para armazenar a discussão.
        if df is not None and not df.empty:
            discussion = []

            # Iterando sobre as linhas do DataFrame.
            for _, row in df.iterrows():
                # Determinando o papel (role) com base no ID do autor.
                role = 'user' if row['AUTOR_ID_AUTOR'] == 1 else 'assistant'

                # Adicionando um dicionário representando a mensagem à lista de discussão.
                discussion.append({"role": role, "content": row['CONTEUDO']})
        else:
            discussion = []

        # Retornando a lista de discussão.
        self.discussion = discussion
        return discussion
    
    def get_num_msgs(self):
        """
        Recupera o número de mensagens em uma discussão com base nos atributos de instância da classe.

        A função constrói e executa uma query SQL para obter a contagem de mensagens da discussão
        associada ao telefone, ID da campanha e data/hora da conversa especificados nos atributos de instância.

        :return: O número de mensagens na discussão.
        :rtype: int
        """
        # Construindo a query SQL para obter a contagem de mensagens da discussão.
        query = f'''SELECT COUNT(*)
                    FROM MENSAGEM
                    WHERE TELEFONE = '{self.id_conversa[0]}' AND ID_CAMPANHA = '{self.id_conversa[1]}' AND DATETIME_CONVERSA = '{self.id_conversa[2]}'
                    ORDER BY MENSAGEM_DATETIME ASC;'''

        # Executando a query e obtendo os resultados em um DataFrame.
        df = query_to_dataframe(query)

        # Inicializando a variável para armazenar a contagem de mensagens.
        count = 0

        # Verificando se o DataFrame não está vazio e obtendo a contagem de mensagens.
        if df is not None and not df.empty:
            count = int(df['COUNT(*)'].iloc[0])

        # Retornando o número de mensagens na discussão.
        return count
    
    def get_horario_ultima_msg_repescagem(self):
        """
        Recupera o horário da última mensagem em uma discussão com base nos atributos de instância da classe.

        A função constrói e executa uma query SQL para obter a data e hora da última mensagem
        associada ao telefone, ID da campanha e data/hora da conversa especificados nos atributos de instância.
        O horário é calculado em relação ao fuso horário local.

        :return: O número de minutos decorridos desde o envio da última mensagem até o momento atual.
        :rtype: int
        """
        # Construindo a query SQL para obter a data e hora da última mensagem da discussão.
        query = f'''SELECT 
                        MAX(MENSAGEM.MENSAGEM_DATETIME) AS MAX_DATETIME,
                        TIMESTAMPDIFF(MINUTE, MAX(MENSAGEM.MENSAGEM_DATETIME), CONVERT_TZ(NOW(), 'UTC', '-03:00')) AS TIME_DIFFERENCE_MINUTES
                        FROM MENSAGEM
                        WHERE 
                        TELEFONE = '{self.id_conversa[0]}' AND 
                        ID_CAMPANHA = '{self.id_conversa[1]}' AND 
                        DATETIME_CONVERSA = '{self.id_conversa[2]}' AND
                        AUTOR_ID_AUTOR = 1'''

        # Executando a query e obtendo os resultados em um DataFrame.
        df = query_to_dataframe(query)

        # Inicializando a variável para armazenar o horário da última mensagem.
        horario_ultima_msg = None

        # Verificando se o DataFrame não está vazio e obtendo o horário da última mensagem.
        if df is not None and df['TIME_DIFFERENCE_MINUTES'].iloc[0] is not None and df['MAX_DATETIME'].iloc[0] is not None:
            minutos_ultima_msg = int(df['TIME_DIFFERENCE_MINUTES'].iloc[0])
            horario_ultima_msg = str(df['MAX_DATETIME'].iloc[0])
            if horario_ultima_msg and isinstance(horario_ultima_msg, str):
                if "." in horario_ultima_msg:  # Check if it has milliseconds
                    horario_ultima_msg = datetime.strptime(horario_ultima_msg, "%Y-%m-%d %H:%M:%S.%f")
                else:
                    horario_ultima_msg = datetime.strptime(horario_ultima_msg, "%Y-%m-%d %H:%M:%S")
        else:
            minutos_ultima_msg = 0
            horario_ultima_msg = datetime.strptime('2000-01-01 00:00:00.000000', "%Y-%m-%d %H:%M:%S.%f")


        # Retornando o número de minutos decorridos desde o envio da última mensagem até o momento atual.
        return minutos_ultima_msg, horario_ultima_msg
    
    def get_horario_ultima_msg(self):
        """
        Recupera o horário da última mensagem em uma discussão com base nos atributos de instância da classe.

        A função constrói e executa uma query SQL para obter a data e hora da última mensagem
        associada ao telefone, ID da campanha e data/hora da conversa especificados nos atributos de instância.
        O horário é calculado em relação ao fuso horário local.

        :return: O número de minutos decorridos desde o envio da última mensagem até o momento atual.
        :rtype: int
        """
        # Construindo a query SQL para obter a data e hora da última mensagem da discussão.
        query = f'''SELECT 
                        MAX(MENSAGEM.MENSAGEM_DATETIME) AS MAX_DATETIME,
                        TIMESTAMPDIFF(MINUTE, MAX(MENSAGEM.MENSAGEM_DATETIME), CONVERT_TZ(NOW(), 'UTC', '-03:00')) AS TIME_DIFFERENCE_MINUTES
                        FROM MENSAGEM
                        WHERE 
                        TELEFONE = '{self.id_conversa[0]}' AND 
                        ID_CAMPANHA = '{self.id_conversa[1]}' AND 
                        DATETIME_CONVERSA = '{self.id_conversa[2]}' AND
                        AUTOR_ID_AUTOR = 1 AND
                        CONTEUDO NOT IN (SELECT DISTINCT(CONTEUDO_ENTRADA) FROM MENSAGENS_ESTATICAS WHERE TIPO_ENTRADA = "BUTTON");
                        '''

        # Executando a query e obtendo os resultados em um DataFrame.
        df = query_to_dataframe(query)

        # Inicializando a variável para armazenar o horário da última mensagem.
        horario_ultima_msg = None

        # Verificando se o DataFrame não está vazio e obtendo o horário da última mensagem.
        if df is not None and df['TIME_DIFFERENCE_MINUTES'].iloc[0] is not None and df['MAX_DATETIME'].iloc[0] is not None:
            minutos_ultima_msg = int(df['TIME_DIFFERENCE_MINUTES'].iloc[0])
            horario_ultima_msg = str(df['MAX_DATETIME'].iloc[0])
            if horario_ultima_msg and isinstance(horario_ultima_msg, str):
                if "." in horario_ultima_msg:  # Check if it has milliseconds
                    horario_ultima_msg = datetime.strptime(horario_ultima_msg, "%Y-%m-%d %H:%M:%S.%f")
                else:
                    horario_ultima_msg = datetime.strptime(horario_ultima_msg, "%Y-%m-%d %H:%M:%S")
        else:
            minutos_ultima_msg = 0
            horario_ultima_msg = datetime.strptime('2000-01-01 00:00:00.000000', "%Y-%m-%d %H:%M:%S.%f")


        # Retornando o número de minutos decorridos desde o envio da última mensagem até o momento atual.
        return minutos_ultima_msg, horario_ultima_msg
    
    def get_ultima_msg(self, autor = None, num_msg = None):
        """
        Recupera o horário da última mensagem em uma discussão com base nos atributos de instância da classe.

        A função constrói e executa uma query SQL para obter a data e hora da última mensagem
        associada ao telefone, ID da campanha e data/hora da conversa especificados nos atributos de instância.
        O horário é calculado em relação ao fuso horário local.

        :autor: int - 0 (ATENDENTE), 1 (CLIENTE)
        :num_msgs: int - quantidade de mensagens a serem consideradas

        :return: O número de minutos decorridos desde o envio da última mensagem até o momento atual.
        :rtype: int
        """
        # Construindo a query SQL para obter a data e hora da última mensagem da discussão.
        if num_msg is not None:
            if autor is not None:
                query = f'''SELECT CONTEUDO
                            FROM MENSAGEM
                            WHERE TELEFONE = '{self.id_conversa[0]}' AND ID_CAMPANHA = '{self.id_conversa[1]}' AND DATETIME_CONVERSA = '{self.id_conversa[2]}' AND AUTOR_ID_AUTOR = '{str(autor)}'
                            ORDER BY MENSAGEM_DATETIME DESC
                            LIMIT {str(num_msg)};'''
            else:
                query = f'''SELECT CONTEUDO
                            FROM MENSAGEM
                            WHERE TELEFONE = '{self.id_conversa[0]}' AND ID_CAMPANHA = '{self.id_conversa[1]}' AND DATETIME_CONVERSA = '{self.id_conversa[2]}'
                            ORDER BY MENSAGEM_DATETIME DESC
                            LIMIT {str(num_msg)};'''
        else: #num_msg não especificado
            query = f'''SELECT MENSAGEM_DATETIME,AUTOR_ID_AUTOR,CONTEUDO
                        FROM MENSAGEM
                        WHERE TELEFONE = '{self.id_conversa[0]}' AND ID_CAMPANHA = '{self.id_conversa[1]}' AND DATETIME_CONVERSA = '{self.id_conversa[2]}'
                        ORDER BY MENSAGEM_DATETIME DESC;'''

        # Executando a query e obtendo os resultados em um DataFrame.
        df = query_to_dataframe(query)

        # Inicializando a variável para armazenar o horário da última mensagem.
        ultima_msg = ""

        # Verificando se o DataFrame não está vazio e obtendo o horário da última mensagem.
        if df is not None and not df.empty:
            if num_msg is not None:
                ultima_msg = df['CONTEUDO'].iloc[num_msg::-1].str.cat(sep='\n\n')
            else: #num_msg não especificado
                lista_ultima_msg = []
                if autor is None:
                    autor = df['AUTOR_ID_AUTOR'].iloc[0]
                
                bool_autor = False
                for index, row in df.iterrows():
                    if not bool_autor and row['AUTOR_ID_AUTOR'] != autor:
                        pass
                    else:
                        bool_autor = True

                    if bool_autor:
                        if row['AUTOR_ID_AUTOR'] == autor:
                            lista_ultima_msg.insert(0, row['CONTEUDO'])
                        else:
                            break
                ultima_msg = "\n".join(lista_ultima_msg)
            
        # Retornando o número de minutos decorridos desde o envio da última mensagem até o momento atual.
        return ultima_msg
    
    def get_link_forms(self):
        query_forms = f"SELECT * FROM CONVERSA WHERE TELEFONE = {self.contato.id_contato} AND DATETIME_CONVERSA = '{self.datetime_conversa}' AND ID_CAMPANHA = {self.campanha};"
        df_forms = query_to_dataframe(query_forms)
        # Verifica se o dataframe está vazio
        if df_forms.empty:
            return None
        else:
            link = df_forms['URL_FORMS'][0]
            return link


    def discussion_to_string(self):
        """
        Converte a lista de dicionários representando uma discussão em uma única string formatada para exibição.

        :param discussion: A lista de dicionários representando a discussão.
                        Cada dicionário deve conter informações sobre o papel (user ou assistant) e o conteúdo da mensagem.
                        Exemplo: [{"role": "user", "content": "Olá, como posso ajudar?"}, {"role": "assistant", "content": "Olá! Eu sou um assistente virtual."}]
        :type discussion: list

        :return: Uma string formatada contendo a conversa, onde as mensagens do cliente são prefixadas com "Cliente: " e as mensagens do assistente são prefixadas com "Atendente: ".
        :rtype: str
        """
        # Inicializando uma lista para armazenar as linhas da conversa.
        discussion = self.get_discussion()

        conversation = []

        # Iterando sobre as entradas da discussão.
        for entry in discussion:
            # Verificando se a entrada não é nula.
            if entry is not None:
                # Verificando o papel do autor e o conteúdo da mensagem.
                if entry["role"] == "assistant" and entry["content"] is not None:
                    conversation.append("(Atendente: " + entry["content"])
                elif entry["content"] is not None:
                    conversation.append("(Cliente: " + entry["content"])

        # Unindo as linhas da conversa em uma única string.
        string = ")\n ".join(conversation)
        return string + ')'


    def get_flag_e_agendamento(self):
        """
        Obtém o valor da coluna ID_FLAG da tabela FLAG_HISTORICO com base nos atributos de instância da classe
        e verifica a ocorrencia da flag 5 (agendamento realizado)

        :return: O valor da coluna ID_FLAG ou "None" se a tabela estiver vazia.
        :rtype: str
        """
        # Construindo a query SQL para obter a flag.
        agendamento = False
        query = f'''SELECT * FROM FLAG_HISTORICO 
	                WHERE TELEFONE = '{self.id_conversa[0]}' AND ID_CAMPANHA = '{self.id_conversa[1]}' AND DATETIME_CONVERSA = '{self.id_conversa[2]}'
                    ORDER BY DATETIME_FLAG DESC;'''
        # Executando a query e obtendo os resultados em um DataFrame.
        print(query)
        df = query_to_dataframe(query)

        # Verificando se o DataFrame não está vazio.
        if df is not None and not df.empty:
            id_flag = df.loc[0, 'ID_FLAG']
            if df['ID_FLAG'].isin([5]).any():
                agendamento = True
                print(f'agendamento: {agendamento}')
        else:
            # Lidando com o caso em que o DataFrame está vazio.
            id_flag = osFunc("NONE")  # ou qualquer outro valor apropriado

        # Retornando o valor da flag.
        return id_flag, agendamento
    
    def get_flag(self):
        """
        Obtém o valor da coluna ID_FLAG da tabela FLAG_HISTORICO com base nos atributos de instância da classe.

        :return: O valor da coluna ID_FLAG ou "None" se a tabela estiver vazia.
        :rtype: str
        """
        # Construindo a query SQL para obter a flag.
        query = f''' SELECT *
                    FROM CONVERSA
                    WHERE TELEFONE = '{self.id_conversa[0]}' AND ID_CAMPANHA = '{self.id_conversa[1]}' AND DATETIME_CONVERSA = '{self.id_conversa[2]}'
                    ORDER BY DATETIME_CONVERSA DESC
                    LIMIT 1;'''

        # Executando a query e obtendo os resultados em um DataFrame.
        df = query_to_dataframe(query)

        # Verificando se o DataFrame não está vazio.
        if df is not None and not df.empty:
            id_flag = df.loc[0, 'ID_LAST_FLAG']
        else:
            # Lidando com o caso em que o DataFrame está vazio.
            id_flag = osFunc("NONE")  # ou qualquer outro valor apropriado

        # Retornando o valor da flag.
        return id_flag

    def set_flag(self, flag, flag_datetime):
        """
        Sets a flag for a conversation and records the change in the FLAG_HISTORICO table.

        This method is responsible for updating the flag for a conversation and keeping a historical record of the flag changes
        by inserting the relevant information into the FLAG_HISTORICO table in the database.

        :param flag: The new flag value to be set for the conversation.
        :type flag: Any valid data type (int, str, etc.) representing the flag.

        :param flag_datetime: The datetime when the flag is set.
        :type flag_datetime: datetime.datetime

        :return: None
        :rtype: None

        :raises: Exception if there is an issue with data insertion into the database.
        """
        if type(flag) is str:
            id_flag = osFunc(flag)
        else:
            id_flag = flag

        # Constructing a dictionary with conversation information for data insertion.
        data_to_upload = {
            'TELEFONE': [self.id_conversa[0]],
            'ID_CAMPANHA': [self.id_conversa[1]],
            'DATETIME_CONVERSA': [self.id_conversa[2]],
            'DATETIME_FLAG': [flag_datetime],
            'ID_FLAG': [id_flag]
        }

        self.flag = id_flag

        try:
            execute_query_db(f'''UPDATE CONVERSA SET 
                                ID_LAST_FLAG = {id_flag} 
                                WHERE 
                                DATETIME_CONVERSA = "{self.id_conversa[2]}" AND
                                ID_CAMPANHA = {self.id_conversa[1]} AND
                                TELEFONE = {self.id_conversa[0]} LIMIT 1;''')
            insert_data_into_db(pd.DataFrame(data_to_upload), "FLAG_HISTORICO")
        
        except Exception as e:
            print(f"ERROR(conversa.set_flag)-{self.id_conversa[0]}: Erro ao inserir flag {id_flag} no db - {e}")
            
    def get_resultado(self):
        
        query = f'''SELECT *
                    FROM CONVERSA
                    WHERE TELEFONE = '{self.id_conversa[0]}' AND ID_CAMPANHA = '{self.id_conversa[1]}' AND DATETIME_CONVERSA = '{self.id_conversa[2]}';'''

        # Executando a query e obtendo os resultados em um DataFrame.
        df = query_to_dataframe(query)

        # Verificando se o DataFrame não está vazio.
        if df is not None and not df.empty:
            id_resultado = df.loc[0, 'ID_RESULTADO']
        else:
            # Lidando com o caso em que o DataFrame está vazio.
            id_resultado = osFunc("FALHA")  # ou qualquer outro valor apropriado

        # Retornando o valor da flag.
        return id_resultado
    
    def set_resultado(self, resultado):
        """
        Sets the id_resultado for a conversation and records the change in the CONVERSA table.

        :param id_resultado: The new id_resultado value to be set for the conversation.
        :type id_resultado: Any valid data type (int, str, etc.) representing the id_resultado.

        :return: None
        :rtype: None

        :raises: Exception if there is an issue with data insertion into the database.
        """
        if type(resultado) is str:
            id_resultado = osFunc(resultado)
        else: 
            id_resultado = resultado
        query = f"""
        UPDATE CONVERSA
        SET ID_RESULTADO = {id_resultado}
        WHERE TELEFONE = {self.id_conversa[0]}
        AND ID_CAMPANHA = {self.id_conversa[1]}
        AND DATETIME_CONVERSA = '{self.id_conversa[2]}'
        AND ID_RESULTADO != {id_resultado};
        """

        try:
            # Converting the data dictionary to a DataFrame and inserting into the CONVERSA table.
            execute_query_db(query)
            # Update the id_resultado field in the object
            self.resultado = id_resultado
        except Exception as e:
            print(f"ERROR(conversa.set_resultado)-{self.id_conversa[0]}: Erro ao atualizar resultado {id_resultado} no db - {e}")

    def set_cadastro(self, nome_completo, email):

        if nome_completo !='False':
            execute_query_db(f"""
            UPDATE CAMP_INDICADOR
            SET NOME_COMPLETO = '{nome_completo}'
            WHERE TELEFONE = {self.id_conversa[0]}
            """)
        if "@" in email:
            execute_query_db(f"""
            UPDATE CAMP_INDICADOR
            SET EMAIL = '{email}'
            WHERE TELEFONE = {self.id_conversa[0]}
            """)

    

    def get_conversa_recente(self):

        if self.campanha:
            query = f''' SELECT *
                    FROM CONVERSA
                    WHERE TELEFONE = '{self.contato.id_contato}' AND ID_CAMPANHA = '{self.campanha}' AND ID_RESULTADO != {osFunc("ADICIONADO")}
                    ORDER BY DATETIME_CONVERSA DESC
                    LIMIT 1;'''
        
        else:
            query = f''' SELECT *
                    FROM CONVERSA
                    WHERE TELEFONE = '{self.contato.id_contato}' AND ID_RESULTADO != {osFunc("ADICIONADO")}
                    ORDER BY DATETIME_CONVERSA DESC
                    LIMIT 1;'''

        # Executando a query e obtendo os resultados em um DataFrame.
        df = query_to_dataframe(query)

        # Verificando se o DataFrame não está vazio.
        if df is not None and not df.empty:
            datetime_conversa = df.loc[0, 'DATETIME_CONVERSA'].strftime('%Y-%m-%d %H:%M:%S')
            campanha = df.loc[0, 'ID_CAMPANHA']
        else:
            # Lidando com o caso em que o DataFrame está vazio.
            current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')) 
            datetime_conversa = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
            campanha = osFunc('RECEPTIVO') 

        # Retornando o valor da flag.
        return datetime_conversa, campanha
    
    def add_mensagem(self, msg, datetime):
        """
        Adiciona uma mensagem à conversa e registra as informações na tabela MENSAGEM.

        Este método é responsável por adicionar uma mensagem à conversa, registrando as informações relevantes
        na tabela MENSAGEM do banco de dados.

        :param msg: O conteúdo da mensagem a ser adicionada.
        :type msg: str

        :param datetime: A data e hora em que a mensagem foi enviada.
        :type datetime: datetime.datetime

        :return: None
        :rtype: None

        :raises: Exception se houver um problema durante a inserção dos dados no banco de dados.
        """
        # Construção de um dicionário com informações da conversa para inserção de dados.
        dados_para_inserir = {
            'TELEFONE': [self.id_conversa[0]],
            'ID_CAMPANHA': [self.id_conversa[1]],
            'DATETIME_CONVERSA': [self.id_conversa[2]],
            'MENSAGEM_DATETIME': [datetime],
            'CONTEUDO': [msg],
            'CUSTO': [0],
            'AUTOR_ID_AUTOR': [0],
            'ID_MENSAGEM': ['']
        }

        try:
            # Convertendo o dicionário de dados para um DataFrame e inserindo na tabela MENSAGEM.
            insert_data_into_db(pd.DataFrame(dados_para_inserir), "MENSAGEM")
        
        except Exception as e:
            print(f"ERROR(conversa.add_mensagem)-{self.id_conversa[0]}: Erro ao Erro ao adicionar mensagem <{msg}> à conversa - {e}")

    def set_resultado_flag_mensagem(self, resultado, flag, msg, datetime):
        ##### INCOMPLETO, reduz de 4 pra 3 queries só
        id_flag = osFunc(flag)
        id_resultado = osFunc(resultado)
        self.flag = id_flag
        self.resultado = id_resultado

        execute_query_db(f'''UPDATE CONVERSA SET 
                                ID_LAST_FLAG = {id_flag} 
                                WHERE 
                                DATETIME_CONVERSA = "{self.id_conversa[2]}" AND
                                ID_CAMPANHA = {self.id_conversa[1]} AND
                                TELEFONE = {self.id_conversa[0]} LIMIT 1;''') 

        data_to_upload = {
            'TELEFONE': [self.id_conversa[0]],
            'ID_CAMPANHA': [self.id_conversa[1]],
            'DATETIME_CONVERSA': [self.id_conversa[2]],
            'DATETIME_FLAG': [datetime],
            'ID_FLAG': [id_flag]
        }
        insert_data_into_db(pd.DataFrame(data_to_upload), "FLAG_HISTORICO")
            
        query = f"""
        UPDATE CONVERSA
        SET ID_RESULTADO = {id_resultado}, ID_LAST_FLAG = {id_flag} 
        WHERE TELEFONE = {self.id_conversa[0]}
        AND ID_CAMPANHA = {self.id_conversa[1]}
        AND DATETIME_CONVERSA = '{self.id_conversa[2]}'
        AND ID_RESULTADO != {id_resultado} AND ID_LAST_FLAG != {id_flag}
        LIMIT 1;
        """


        execute_query_db(query)

            
        dados_para_inserir = {
            'TELEFONE': [self.id_conversa[0]],
            'ID_CAMPANHA': [self.id_conversa[1]],
            'DATETIME_CONVERSA': [self.id_conversa[2]],
            'MENSAGEM_DATETIME': [datetime],
            'CONTEUDO': [msg],
            'CUSTO': [0],
            'AUTOR_ID_AUTOR': [0],
            'ID_MENSAGEM': ['']
        }

        insert_data_into_db(pd.DataFrame(dados_para_inserir), "MENSAGEM")


    def initialize(self):
        query = f'''
            SELECT 
                CONVERSA.*, 
                FLAG_HISTORICO.ID_FLAG, 
                FLAG_HISTORICO.DATETIME_FLAG, 
                CAMPANHA.TIPOLOGIA, 
                CAMPANHA.COMPLETAR_CADASTRO, 
                CAMPANHA.AGENDAMENTO_CADASTRADO,
                HSM.MESSAGE_PAYLOAD,
                HSM.CONTEUDO,
                TELEFONE_WPP.TELEFONE_WPP,
                TELEFONE_WPP.TOKEN,
                COUNT(MENSAGEM.MENSAGEM_DATETIME) AS QTD_MSG
            FROM CONVERSA
            LEFT JOIN FLAG_HISTORICO ON CONVERSA.TELEFONE = FLAG_HISTORICO.TELEFONE
                AND CONVERSA.ID_CAMPANHA = FLAG_HISTORICO.ID_CAMPANHA     
                AND CONVERSA.DATETIME_CONVERSA = FLAG_HISTORICO.DATETIME_CONVERSA
            LEFT JOIN CAMPANHA ON CAMPANHA.ID_CAMPANHA = CONVERSA.ID_CAMPANHA
            LEFT JOIN HSM ON CONVERSA.ID_HSM = HSM.ID_HSM
            LEFT JOIN TELEFONE_WPP ON TELEFONE_WPP.TELEFONE_WPP = CONVERSA.TELEFONE_WPP
            LEFT JOIN MENSAGEM ON CONVERSA.TELEFONE = MENSAGEM.TELEFONE AND
                                  CONVERSA.DATETIME_CONVERSA = MENSAGEM.DATETIME_CONVERSA AND
                                  CONVERSA.ID_CAMPANHA = MENSAGEM.ID_CAMPANHA
            WHERE 
                CONVERSA.TELEFONE = '{self.id_conversa[0]}'
                AND CONVERSA.ID_CAMPANHA = '{self.id_conversa[1]}' 
                AND CONVERSA.DATETIME_CONVERSA = '{self.id_conversa[2]}'
            ORDER BY FLAG_HISTORICO.DATETIME_FLAG DESC
            LIMIT 1;'''
        df = query_to_dataframe(query)

        if df is not None and not df.empty:
            self.banco_dados = df.loc[0, 'BANCO_DADOS']
            resultado = df.loc[0, 'ID_RESULTADO']
            flag = df.loc[0, 'ID_FLAG']
            HSM = df.loc[0, 'ID_HSM']
            tipologia_campanha = df.loc[0, 'TIPOLOGIA'] 
            completar_cadastro = df.loc[0, 'COMPLETAR_CADASTRO'] 
            agendamento_cadastrado = df.loc[0, 'AGENDAMENTO_CADASTRADO']
            hsm_conteudo = df.loc[0, 'CONTEUDO']
            hsm_payload = df.loc[0, 'MESSAGE_PAYLOAD']
            wpp_header = {
                            'D360-Api-Key': df.loc[0, 'TOKEN'],
                            'Content-Type': "application/json",
                        }
            qtd_msg = df.loc[0, 'QTD_MSG']
            telefone_wpp = df.loc[0, 'TELEFONE_WPP']
            agendamento_multiplo = df.loc[0, 'AGENDAMENTO_MULTIPLO']
        else:
            resultado, flag, HSM, tipologia_campanha, completar_cadastro,agendamento_cadastrado, hsm_conteudo, hsm_payload, wpp_header = "None", "None", "None", "None", "None", "None", "None", "None", "None"  # ou qualquer outro valor apropriado
            qtd_msg = "None"
            telefone_wpp = "None"
            agendamento_multiplo = "None"

        return resultado, flag, HSM, tipologia_campanha, completar_cadastro, agendamento_cadastrado, hsm_conteudo, hsm_payload, wpp_header, qtd_msg, telefone_wpp, agendamento_multiplo

    def repeat_last_flag(self, flag_datetime):
        try:
            execute_query_db(f'''INSERT INTO FLAG_HISTORICO (TELEFONE, ID_CAMPANHA, DATETIME_CONVERSA, DATETIME_FLAG, ID_FLAG)
                                SELECT TELEFONE, ID_CAMPANHA, DATETIME_CONVERSA, '{flag_datetime}', ID_LAST_FLAG
                                FROM CONVERSA
                                WHERE DATETIME_CONVERSA = "{self.id_conversa[2]}" AND
                                    ID_CAMPANHA = {self.id_conversa[1]} AND
                                    TELEFONE = {self.id_conversa[0]} LIMIT 1;''')
        except:
            print(f"ERROR(conversa.repeat_last_flag)-{self.id_conversa[0]}: Erro ao repetir flag")

    def change_campanha(self, id_campanha_new):
        telefone = self.contato.id_contato
        
        print(f"TELEFONE: {telefone} -> CAMPANHA: {self.campanha} to {id_campanha_new}")
        id_campanha_old = self.campanha

        query_update = f"""UPDATE CONVERSA SET ID_CAMPANHA = {id_campanha_new}
            WHERE TELEFONE = {telefone} AND DATETIME_CONVERSA = '{self.datetime_conversa}' AND ID_CAMPANHA = {id_campanha_old};"""
        # Executa as queries usando a função execute_query_db
        execute_query_db(query_update)

