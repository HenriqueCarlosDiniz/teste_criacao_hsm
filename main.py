from utils.config import *
from Models.contato import Contato
from Models.conversa import Conversa
from utils.connect_db import insert_data_into_db, query_to_dataframe, execute_query_db
import os, requests
import pytz
#import utils.config
# from recebeMensagem.lambda_function import  lambda_handler as recebe_mensagem
import json, os
from recebeMensagem.lambda_function import lambda_handler as recebe_mensagem 
from Disparo.lambda_function import lambda_handler as disparo
from adicionaClientes.lambda_function import lambda_handler as adicionaClientes
from StatusRead_envia_Mensagem.lambda_function import lambda_handler as HSM_lido_disparo
import datetime
from checa_validade_HSM.lambda_function import lambda_handler as checaValidadeHSm
from tb_log_contatos_db.tb_log_contatos_db import lambda_handler as tb_log_contatos_db
from enviaPsd2.lambda_function import lambda_handler as enviaPsd2
            
# def mensagem_wpp(mensagem, telefone):
#     # event = {'body': {"contacts":[{"profile":{"name":"Bruno Oshiro"},"wa_id":telefone}],"messages":[{"from":telefone,"id":"ABGHVRGYJTg0bwIJHOAlOXVKWOZL","text":{"body":mensagem},"timestamp":"1702561670","type":"text"}]}}
#     event = {
        
#         "Records":[
#             {
                
#                 "messageId":"c1f2f9dd-9cf6-4df5-beb8-0e379c3d8391",
#                 "receiptHandle":"AQEBHJ0JyJmjEiq5Ycv9OXOsUYEIqCfkxpE3pmJsSIy6Ow37OLjVO17uiUwLxiyHS7jd4vnH2JPOo4Hk3zOBeSJmaqs5Pnm1C1NwB3woPePEBAxUSGNf47gEGjhL5x7Ire4U2/9dUwTyeEI1ZpRFWHWjsTsXSk0u80QMTEjo3Z8gK5AcbI3HLuvz6tlFldyLDiU7Dkw+8pPhmacS2F13cbGnutvQgDZQuPSYOUKEGnujJk3ZXJJbdF3V+hcpfNjPtRVAGND/fQUTKm1S5yPzXhFGvFxirzAC2L7jztW+qzscBxA=",
#                 "body": {"post": 
#                          {
#                              "contacts": [
#                                 { 
#                                     "profile": {"name": "Bruno Oshiro"}, 
#                                     "wa_id": telefone}
#                               ], 
#                                 "messages": [{
#                                     "from": telefone, 
#                                     "id": "ABGHVRGYJTg0bwIJuhkb3Ao3Kb1v", 
#                                     "text": {"body": mensagem}, 
#                                     "timestamp": "1709569418", 
#                                     "type": "text"}
#                                    ],
#                             "timestamp": datetime.datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%Y-%m-%d %H:%M:%S.%f'),
#                             },
#                             "timestamp": datetime.datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%Y-%m-%d %H:%M:%S.%f'),
#                             "telefone_wpp": 5511959861043}, 
                                   
#                 "attributes":{
#                     "ApproximateReceiveCount":"1",
#                     "AWSTraceHeader":"Root=1-65e5f58b-513cfb2754ee6d4a421ec47d;Parent=2bd2a7584102f6ad;Sampled=0;Lineage=734f31d5:0",
#                     "SentTimestamp":"1709569419347",
#                     "SequenceNumber":"18884393845062383616",
#                     "MessageGroupId":"envio_direto",
#                     "SenderId":"AROASZBZPWDM3U24BTXGK:RecebeMensagemV1",
#                     "MessageDeduplicationId":"47d632b475d68a266a61ed0734740b098fcb06ce591d1157722663eb76e32448",
#                     "ApproximateFirstReceiveTimestamp":"1709569419347"
#                 },
#                 "messageAttributes":{

#                 },
#                 "md5OfBody":"d68dbf26bc5e4a7c4c503c93c086d5ce",
#                 "eventSource":"aws:sqs",
#                 "eventSourceARN":"arn:aws:sqs:sa-east-1:191246610649:homolog-bot-fila-post.fifo",
#                 "awsRegion":"sa-east-1"
#             }
#         ]
#         }


#     return event["Records"][0]

# def button_msg(telefone):
#     text = input("Texto do button:")
#     event = {
#         "body":{  
#             "contacts": [
#                 {
#                 "profile": {
#                     "name": "Felipe Benatti"
#                 },
#                 "wa_id": f"{telefone}"
#                 }
#             ],
#             "messages": [
#                 {
#                 "button": {
#                     "text": f"{text}"
#                 },
#                 "context": {
#                     "from": "5511959861043",
#                     "id": "gBGHVRGRkViJLwIJnXpO-LSndXTr"
#                 },
#                 "from": f"{telefone}",
#                 "id": "ABGHVRGRkViJLwIQFeGi7N9C1azMWsNRRfR5Uw",
#                 "timestamp": "1698178409",
#                 "type": "button"
#                 }
#             ]
#             }
#     }
#     return event

# def interactive_msg(telefone):
#     text = input("Texto do interactive:")
#     event = {
#             "body":{
#                 "contacts": [
#                     {
#                     "profile": {
#                         "name": "Pedro Gianjoppe 🇧🇷🇮🇹"
#                     },
#                     "wa_id": f"{telefone}"
#                     }
#                 ],
#                 "messages": [
#                     {
#                     "context": {
#                         "from": "5511959861043",
#                         "id": "gBGHVRGRkViJLwIJQd1cHHCm-7ps"
#                     },
#                     "from": f"{telefone}",
#                     "id": "ABGHVRGRkViJLwILPrCgB8MoUfetmWo",
#                     "interactive": {
#                         "list_reply": {
#                         "description": "Saiba mais sobre a avaliação com nosso especialista!",
#                         "id": "1",
#                         "title": f"{text}"
#                         },
#                         "type": "list_reply"
#                     },
#                     "timestamp": "1703778564",
#                     "type": "interactive"
#                     }
#                 ]
#                 }
#             }       
    
#     return event

def loop_without_wpp():
    #reset = input("Deseja resetar o Discussion: S/N: ")
    #if reset == 'S':
    #    mensagem = 'reset'
    #    recebe_mensagem(mensagem_wpp(mensagem),{})
    #    mensagem = input("REAGENDAR/NÃO TENHO INTERESSE:")
    #    recebe_mensagem(mensagem_wpp(mensagem),{})
    #    reset = input("Deseja resetar o Discussion: S/N: ")
    telefone = "5511955591026" # input("Insira o telefone: ")

#     while True:
#         mensagem = input("Cliente: ")
#         if mensagem == 'break':
#             break
#         elif mensagem == 'button':
#             bot = recebe_mensagem(button_msg(telefone),{})
#         elif mensagem == 'interactive':
#             bot = recebe_mensagem(interactive_msg(telefone),{})
#         else:
#             msg = mensagem_wpp(mensagem, telefone)
#             recebe_mensagem(msg, {})
        
#     return 0

# def loop_for_testes():
#     teste = "Kike" # input("Insira o nome da pasta para salvar teste")
#     telefone = "5511982538346"
#     while True:
#         mensagem = input("Cliente: ")
#         if mensagem == 'break':
#             conversa = Conversa(Contato(int(telefone)),campanha = 0)
#             query = f"SELECT AUTOR_ID_AUTOR, CONTEUDO FROM MENSAGEM WHERE DATETIME_CONVERSA = '{conversa.datetime_conversa.strftime('%Y-%m-%d %H:%M:%S')}' AND TELEFONE = {int(telefone)}"
#             df = query_to_dataframe(query)
#             len_pasta = len(os.listdir("historico_testes"))
#             df.to_csv(f"historico_testes/{teste}_{len_pasta}.csv", index = False)
#             break
#         elif mensagem == 'button':
#             recebe_mensagem(button_msg(telefone),{})
#         else:
#             msg = mensagem_wpp(mensagem, telefone)
#             recebe_mensagem(msg, {})
#     return 0

def main():
    # checaValidadeHSm({},{})
    input = {
    "body": {
        "post": {
    "object": "whatsapp_business_account",
    "entry": [
        {
            "id": "2952971388300533",
            "changes": [
                {
                    "value": {
                        "messaging_product": "whatsapp",
                        "metadata": {
                            "display_phone_number": "5511935026262",
                            "phone_number_id": "318286431366973"
                        },
                        "contacts": [
                            {
                                "profile": {
                                    "name": "Pedro Gianjoppe 🇧🇷🇮🇹"
                                },
                                "wa_id": "5511919158892"
                            }
                        ],
                        "messages": [
                            {
                                "from": "5511919158892",
                                "id": "wamid.HBgNNTUxMTkxOTE1ODg5MhUCABIYIEY4OTNCMDZGOUM4RTJCQkJBN0Y0NjIxMTU4MkVBNzNCAA==",
                                "timestamp": "1716844046",
                                "type": "image",
                                "image": {
                                    "mime_type": "image/jpeg",
                                    "sha256": "UvJH+oM/JCaK1zYmnMEI/SCv/76jL2Fmdok6YdQo+AY=",
                                    "id": "818667756380727"
                                }
                            }
                        ]
                    },
                    "field": "messages"
                }
            ]
        }
    ]
},
        "timestamp": "2024-07-18 11:30:41.880034",
        "telefone_wpp": "5511935026262"
    },
    "attributes": {
        "ApproximateReceiveCount": "1",
        "AWSTraceHeader": "Root=1-6697f175-3cf0eb9103994e623cf948ab;Parent=0040943c541dadb3;Sampled=0;Lineage=5e38bf4c:0",
        "SentTimestamp": "1721233781928",
        "SequenceNumber": "18887379921883119616",
        "MessageGroupId": "envio_direto",
        "SenderId": "AROASZBZPWDM353KBDOT4:homolog_recebePOSTDialog",
        "MessageDeduplicationId": "b68359ec318078f0a70bde5a6170106677508c2e127db9a8e31c0e89252d74f4",
        "ApproximateFirstReceiveTimestamp": "1721233781928"
    },
    "messageAttributes": {},
    "md5OfBody": "cd184c083a87b1168fca03565a02596a",
    "eventSource": "aws:sqs",
    "eventSourceARN": "arn:aws:sqs:sa-east-1:191246610649:homolog-bot-fila-post.fifo",
    "awsRegion": "sa-east-1"
}

    recebe_mensagem(input, {})
    # enviaPsd2(input, {})
    # tb_log_contatos_db({},{})
    # loop_for_testes()
    # adicionaClientes({},{})
    # disparo(qe{},{})
    # pass
    #HSM_lido_disparo({'Records': [{'body':{'telefone': 5511982538346}}]}, '_')
    numero = str(5511963628298)
    valor_padrao = osFunc('API_AGENDAMENTO_NO_SHOW')
    size_numero = len(numero)
    chaves_api = {
    0: {  # banco_dados = 0
        0: osFunc('API_AGENDAMENTO_NO_SHOW'), 
        1: osFunc('API_AGENDAMENTO_CONFIRMACAO'),
        2: osFunc('API_AGENDAMENTO_CANCELADOS'), 
        3: osFunc('API_AGENDAMENTO_INDIQUE_AMIGOS'), 
        4: osFunc('API_AGENDAMENTO_LEADS_META'), 
        9: osFunc('API_AGENDAMENTO_RECEPTIVO_LP_FASCITE'),
        10: osFunc('API_AGENDAMENTO_RECEPTIVO'),
        11: osFunc('API_AGENDAMENTO_RECEPTIVO_GOOGLE'),
        12: osFunc('API_AGENDAMENTO_SAPATOS_SITE'),
        13: osFunc('API_AGENDAMENTO_AGENDAMENTO_SITES'),
        14: osFunc('API_AGENDAMENTO_CONFIRMACAO_2H'),
        15: osFunc('API_AGENDAMENTO_LEADS_WHATSAPP')
    },
    1: {  # banco_dados = 1
        0: osFunc('API_AGENDAMENTO_NO_SHOW_BD'), 
        2: osFunc('API_AGENDAMENTO_CANCELADOS_BD'), 
    },
    }
    print(osFunc("FACEBOOK_RECEPTIVO"))

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

if __name__ == '__main__':
    texto = "Para a unidade Paulista, temos disponibilidade nos seguintes dias e hor\\u00e1rios:\\n\\n- 16/08/2024, sexta-feira: 09:00, 10:00, 11:00, 12:00, 13:00, 14:00, 15:00, 16:00, 17:00\\n- 17/08/2024, s\\u00e1bado: 08:00, 09:00, 10:00, 11:00, 12:00, 14:00, 15:00, 16:00\\n- 19/08/2024, segunda-feira: 09:00, 10:00, 11:00, 12:00, 14:00, 15:00, 16:00, 17:00\\n- 20/08/2024, ter\\u00e7a-feira: 09:00, 10:00, 11:00, 12:00, 13:00, 14:00, 15:00, 16:00, 17:00\\n- 21/08/2024, quarta-feira: 09:00, 10:00, 11:00, 12:00, 13:00, 14:00, 15:00, 16:00, 17:00\\n\\nQual seria o melhor hor\\u00e1rio para voc\\u00ea?"
    quebra_texto = quebrar_mensagem(texto, 20)
    print(quebra_texto)  # Saída: 11987654321
    




        