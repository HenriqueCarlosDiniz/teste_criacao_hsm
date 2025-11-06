from utils.config import *
import requests, json, os, re
import locale
from datetime import datetime
from utils.connect_db import query_to_dataframe
from utils.text import format_data_feira
import boto3

URL_DIALOG_MESSAGE = os.environ["URL_DIALOG_MESSAGE"]

HEADERS = {
    'D360-Api-Key': os.environ["API_DIALOG"],
    'Content-Type': "application/json",
}

locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')

def enviar_HSM(conversa, http_session = None, amigo = "", payload = None, header = HEADERS):
    """
    Envia a primeira mensagem no WhatsApp com base no tipo de campanha.

    :param conversa: Objeto representando a conversa.
    :type conversa: Conversa

    :param http_session: Objeto da sessão HTTP.
    :type http_session: requests.Session

    :return: O resultado da solicitação de envio da mensagem.
    :rtype: str
    """

    # Preparando a mensagem para ser enviada
    try:
        if payload:
            payload = preparar_payload_hsm(conversa, amigo, payload)
        else:
            payload = get_payload_hsm(conversa, amigo)
        payload = json.loads(payload)
        payload = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    except Exception as e:
        print(f"ERROR(send_wpp.enviar_HSM)-{conversa.contato.id_contato}: Erro no preparar payload do HSM {e}")
        return 'False'

    # Making the POST request to send the response using the provided HTTP session
    try:
        if http_session:
            response = http_session.post(URL_DIALOG_MESSAGE, data=payload, headers=header)  # Use HEADERS variable
        else:
            response = requests.post(URL_DIALOG_MESSAGE, data=payload, headers=header)
        print(f"INFO(send_wpp.enviar_HSM)-{conversa.contato.id_contato}: HSM Enviado = {payload}, Resposta POST Dialog360 = {response.text}")
        
        if response.headers['Content-Type'] == 'application/json':
            response_json = json.loads(response.text)
            if 'errors' in response_json:
                if 'details' in response_json['errors'][0] and response_json['errors'][0]['details'] == "Recipient is not a valid WhatsApp user":
                    return 'Sem Whatsapp'
                return 'False'
            elif 'meta' in response_json and 'api_status' in response_json['meta'] and response_json['meta']['api_status'] == "stable":
                return 'True'
        else:
            print(f"WARNING(send_wpp.enviar_wpp) - {conversa.contato.id_contato}: Received non-JSON response. Status code: {response.status_code}, Content-Type: {response.headers.get('Content-Type')}")

        return 'False'

    except (requests.ConnectionError, requests.Timeout, requests.TooManyRedirects) as e:
        print(f"ERROR(send_wpp.enviar_HSM)-{conversa.contato.id_contato}: Erro ao enviar HSM {e}")
        return 'False'
    
def retry_sqs(telefone, msg, header):
    if osFunc('Ambiente_Lambda'):
        print('STATUS READ ENVIANDO PARA FILA')
        # Configura o cliente do SQS
        sqs_client = boto3.client('sqs')
        # URL da fila SQS
        queue_url = osFunc('urlSQS_retry_enviar_msg')
        # Envia a mensagem para a fila SQS
        sqs_response = sqs_client.send_message(
            QueueUrl = queue_url,
            MessageBody=json.dumps({'telefone': str(telefone), 'header': str(header), 'msg': msg}), 
            MessageGroupId='envio_direto') 
        
        print('SQS Response ', sqs_response)

def enviar_wpp(telefone, msg, quebrar_mensagem_condition=True, header = None):
    """
    Envia uma mensagem no WhatsApp para um número específico.

    :param telefone: O número de telefone do destinatário.
    :type telefone: str
    :param msg: O conteúdo da mensagem a ser enviada.
    :type msg: str
    :param quebrar_mensagem: Indica se a mensagem deve ser quebrada (True) ou não (False).
    :type quebrar_mensagem: bool

    :return: O resultado da solicitação de envio da mensagem.
    :rtype: requests.Response
    """
    retorno = 'False'
    # Verificando se a mensagem deve ser quebrada
    if quebrar_mensagem_condition:
        # Quebrando a mensagem em pedaços de até 30 caracteres
        msgArray = quebrar_mensagem(msg, 30)

        # Iterando sobre os pedaços da mensagem
        for msg_part in msgArray:
            message_payload = {
                "to": str(telefone),
                "type": "text",
                "messaging_product": "whatsapp",
                "text": {
                    "body": msg_part
                }
            }

            payload = json.dumps(message_payload, ensure_ascii=False).encode('utf-8')

            api_response = requests.post(URL_DIALOG_MESSAGE, data=payload, headers=header)
            print(f"INFO(send_wpp.enviar_wpp)-{telefone}: Mensagem enviada = {msg_part}, response = {api_response.text}")
            
            try:
                api_response = json.loads(api_response.text)
            except Exception as e:
                print(f"ERRO(lambda_handler:enviar_wpp): {e}")
                retry_sqs(telefone, msg, header)
                return e

            if 'meta' in api_response and 'api_status' in api_response['meta'] and api_response['meta']['api_status'] == "stable":
                retorno = 'True'
            elif 'meta' in api_response and 'developer_message' in api_response['meta'] and api_response['meta']['developer_message'] == "No authorization provided":
                retorno = 'No authorization provided'
            else:
                retorno = 'False'

    else:
        message_payload = {
            "to": telefone,
            "type": "text",
            "messaging_product": "whatsapp",
            "text": {
                "body": f"{msg}"
            }
        }

        payload = json.dumps(message_payload, ensure_ascii=False).encode('utf-8')

        api_response = requests.post(URL_DIALOG_MESSAGE, data=payload, headers=header)
        print(f"INFO(send_wpp.enviar_wpp)-{telefone}: Mensagem enviada = {msg}, response = {api_response.text}")

        try:
            api_response = json.loads(api_response.text)
        except Exception as e:
            print(f"ERRO(lambda_handler:enviar_wpp): {e}")
            retry_sqs(telefone, msg, header)
            return e
        
        if 'meta' in api_response and 'api_status' in api_response['meta'] and api_response['meta']['api_status'] == "stable":
            retorno = 'True'
        elif 'meta' in api_response and 'developer_message' in api_response['meta'] and api_response['meta']['developer_message'] == "No authorization provided":
            retorno = 'No authorization provided'
        else:
            retorno = 'False'

    return retorno
       

def quebrar_mensagem(texto, limite):
    """
    Divide um texto em frases, considerando regras específicas de formatação e limitando o número de palavras por frase.

    :param texto: O texto a ser dividido em frases.
    :type texto: str
    :param limite: O número máximo de palavras por frase.
    :type limite: int

    :return: Uma lista de frases divididas conforme as regras especificadas.
    :rtype: list of str
    """
    try:
        # Verificando se o texto contém listas numeradas
        if re.search(r"1\.|2\.", texto):  # Uma lista identificada não separa o texto
            return [texto]

        # Dividindo o texto em frases
        frases = re.split(r'(?<=[.!?])\s+', texto)
        frases = [frase for frase in frases if frase.strip()] #limpar elementos de string vazia
        # Tratando casos onde uma frase termina com abreviações específicas
        merged_phrases = []
        i = 0
        while i < len(frases):
            current_phrase = frases[i]
            last_word = current_phrase.split()[-1]

            if last_word in ["1.", "2.", "3.", "4.", "5.", "6.", "7.", "8.", "9.","R.", "Av.", "Trav.", "Pç.", "Prç.", "Al.", "Bc.", "Lg.", "Rod.", "Vd.", "Estr.", "Estrad."]:
                merged_phrase = current_phrase + ' ' + frases[i + 1] if i + 1 < len(frases) else current_phrase
                merged_phrases.append(merged_phrase)
                i += 2 if i + 1 < len(frases) else 1
            else:
                merged_phrases.append(current_phrase)
                i += 1

        # Atualizando a lista de frases
        frases = merged_phrases

        # Contando o número de palavras em cada frase
        NpalavrasArray = contar_palavras_em_textos(frases)

        # Separando as perguntas independentemente do limite de palavras e removendo o ponto final
        resultado = []
        concat = []
        numero_palavras_acumulado = 0

        for i, frase in enumerate(frases):
            numero_palavras_acumulado += NpalavrasArray[i]

            if '?' in frase:
                if concat:
                    resultado.append(' '.join(concat))
                    concat = []
                    numero_palavras_acumulado = NpalavrasArray[i]

                # Verificando se a pergunta tem pelo menos 2 palavras
                if numero_palavras_acumulado >= 2:
                    resultado.append(frase)
                else:
                    # Se tiver menos de 2 palavras, concatene com a frase anterior
                    if resultado:
                        resultado[-1] += ' ' + frase
                    else:
                        concat.append(frase)
                    numero_palavras_acumulado += NpalavrasArray[i]

            elif numero_palavras_acumulado > limite:
                if concat:
                    resultado.append(' '.join(concat))
                    concat = []
                    numero_palavras_acumulado = NpalavrasArray[i]
                resultado.append(frase)
            else:
                concat.append(frase)

        if concat:
            resultado.append(' '.join(concat))

        return resultado

    except Exception as e:
        print(f"ERROR(send_wpp.quebrar_mensagem)-{texto}: erro ao quebrar mensagem {e}")
        return []


def contar_palavras_em_textos(textos):
    """
    Conta o número de palavras em cada texto presente em uma lista de textos.

    :param textos: Uma lista de textos para os quais o número de palavras deve ser contado.
    :type textos: list of str

    :return: Uma lista contendo o número de palavras em cada texto.
    :rtype: list of int
    """
    contagem_palavras = []
    
    # Iterando sobre cada texto na lista
    for texto in textos:
        # Dividindo o texto em palavras usando o espaço como delimitador
        palavras = texto.split()
        # Contando o número de palavras no texto
        numero_palavras = len(palavras)
        # Adicionando a contagem à lista
        contagem_palavras.append(numero_palavras)
    
    # Retornando a lista com o número de palavras em cada texto
    return contagem_palavras

def get_payload_hsm(conversa, amigo):

    query = f"""
    SELECT *
    FROM CONVERSA
    JOIN HSM ON CONVERSA.ID_HSM = HSM.ID_HSM
    WHERE CONVERSA.TELEFONE = \'{conversa.contato.id_contato}\' AND CONVERSA.DATETIME_CONVERSA = \'{conversa.datetime_conversa}\'
    """
    payload_df = query_to_dataframe(query)
    if payload_df.empty:
        payload = '{"to": {telefone}, "type": "template", "messaging_product": "whatsapp", "template": {"namespace": "37e9fce0_4763_44a6_8294_f680359013ed", "language": {"policy": "deterministic", "code": "pt_BR"}, "name": "no_show_tac_3152_v5"}}'
    else:
        payload = payload_df.iloc[0]['MESSAGE_PAYLOAD']
    if conversa.campanha == int(osFunc('INDIQUE_AMIGOS')):
        if amigo == "":
            query =f"""
                SELECT *
                FROM CAMP_INDICADOR
                WHERE TELEFONE = \'{conversa.contato.id_contato}\'
            """
            df = query_to_dataframe(query)
            amigo = df.iloc[0]['NOME_AMIGO']
    data_agendamento = datetime.strptime(conversa.contato.agendamento.data_agendamento, "%Y-%m-%d").strftime("%d/%m")
    data_agendamento2 = format_data_feira(datetime.strptime(conversa.contato.agendamento.data_agendamento, "%Y-%m-%d"))
    horario_agendamento = f"{conversa.contato.agendamento.horario_agendamento}h"
    horario_agendamento2 = datetime.strptime(conversa.contato.agendamento.horario_agendamento, "%H:%M").strftime("%Hh%M")
    unidade_agendamento = f"Unidade {conversa.contato.agendamento.unidade_agendamento.nome}"
    endereco_agendamento = f"https://pessemdor.link/{conversa.contato.agendamento.unidade_agendamento.sigla}"
    endereco_agendamento2 = f"{conversa.contato.agendamento.unidade_agendamento.endereco}"
    payload = payload.replace('{cliente}', str("\""+conversa.contato.nome+"\"")) \
        .replace('{telefone}', str("\""+conversa.contato.id_contato+"\"")) \
        .replace('{amigo}', str("\""+amigo+"\"")) \
        .replace('{data_agendamento}', str("\""+data_agendamento+"\"")) \
        .replace('{data_agendamento2}', str("\""+data_agendamento2+"\"")) \
        .replace('{horario_agendamento}', str("\""+horario_agendamento+"\"")) \
        .replace('{horario_agendamento2}', str("\""+horario_agendamento2+"\"")) \
        .replace('{unidade_agendamento}', str("\""+unidade_agendamento+"\"")) \
        .replace('{endereco_agendamento}', str("\""+endereco_agendamento+"\"")) \
        .replace('{endereco_agendamento2}', str("\""+endereco_agendamento2+"\""))
    return payload

def preparar_payload_hsm(nome_cliente, telefone, unidade_sigla, unidade_endereco, data_agendamento, horario_agendamento, amigo, payload):
    # if conversa.campanha == int(osFunc('INDIQUE_AMIGOS')):
    #     if amigo == "":
    #         query =f"""
    #             SELECT *
    #             FROM CAMP_INDICADOR
    #             WHERE TELEFONE = \'{conversa.contato.id_contato}\'
    #         """
    #         df = query_to_dataframe(query)
    #         amigo = df.iloc[0]['NOME_AMIGO']
    data_agendamento = datetime.strptime(data_agendamento, "%Y-%m-%d").strftime("%d/%m")
    data_agendamento2 = format_data_feira(datetime.strptime(data_agendamento, "%Y-%m-%d"))
    horario_agendamento = f"{horario_agendamento}h"
    horario_agendamento2 = datetime.strptime(horario_agendamento, "%H:%M").strftime("%Hh%M")
    unidade_agendamento = f"Unidade {unidade_agendamento.nome}"
    endereco_agendamento = f"https://pessemdor.link/{unidade_sigla}"
    endereco_agendamento2 = f"{unidade_endereco}"
    payload = payload.replace('{cliente}', str("\""+nome_cliente+"\"")) \
        .replace('{telefone}', str("\""+telefone+"\"")) \
        .replace('{amigo}', str("\""+amigo+"\"")) \
        .replace('{data_agendamento}', str("\""+data_agendamento+"\"")) \
        .replace('{data_agendamento2}', str("\""+data_agendamento2+"\"")) \
        .replace('{horario_agendamento}', str("\""+horario_agendamento+"\"")) \
        .replace('{horario_agendamento2}', str("\""+horario_agendamento2+"\"")) \
        .replace('{unidade_agendamento}', str("\""+unidade_agendamento+"\"")) \
        .replace('{endereco_agendamento}', str("\""+endereco_agendamento+"\"")) \
        .replace('{endereco_agendamento2}', str("\""+endereco_agendamento2+"\""))
    return payload


def enviar_payload(message_payload, header = HEADERS):
   
    # Encode do dicionário para JSON usando utf-8
    payload = json.dumps(message_payload, ensure_ascii=False).encode('utf-8')

    # Fazendo a requisição POST para enviar a resposta
    response = requests.post(URL_DIALOG_MESSAGE, data=payload, headers= header)
    print(f"INFO(send_wpp.enviar_payload): Payload enviada = {payload}, response = {response.text}")

    return response

