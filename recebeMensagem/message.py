from utils.config import *

import json
import traceback
import pytz
from datetime import datetime, timedelta 
import re

from utils.api_communication import GET_vagas_disponiveis, POST_buscar_telefone, POST_cancelar_agendamento, POST_confirmar_agendamento, registra_agendamentoAPI, buscar_unidades, GET_unidades_proximas
from utils.text import formatar_data
from utils.connect_db import query_to_dataframe  
from utils.locations import agendamento_passado, get_sigla_ddd
from Models.unidade import Unidade
from Models.conversa import Conversa
from Models.contato import Contato

if osFunc('Ambiente_Lambda'):
    from media import download_media, transcribe_audio
else:
    from recebeMensagem.media import download_media, transcribe_audio 
    
def checa_agenda_vazia(sigla, n = 5):
    print("############ Checando Unidade Vazia")

    unidade_cliente = Unidade(sigla)
    agenda = unidade_cliente.agenda
    bool_agenda_vazia = all(len(value) == 0 for value in agenda.values())
    bool_agenda_vazia = True
    if bool_agenda_vazia:
        address = unidade_cliente.endereco
        response = buscar_unidades(address, n, dist_max = 1000)
        print('resposta buscar unidade:',len(response))
        for i in range(1,n): 
            nova_sigla = response[i]['grupoFranquia']
            unidade_cliente = Unidade(nova_sigla)
            agenda = unidade_cliente.agenda
            bool_agenda_vazia = all(len(value) == 0 for value in agenda.values())
            if not bool_agenda_vazia:
                print("Nova UNIDADE:", nova_sigla)
                return nova_sigla
                
    else:
        return sigla


def extract_message(body_json, conversa, today, http_session, wpp_token, telefone_wpp):
    message_text = None
    id_media = ''
    media_type = ''
    media_extension = ''
    

    try:
        response2Client = {'type': None, 'content': None}
        id_flag = osFunc("RESPONDER")
        id_resultado = osFunc("RESPONDIDO")
        
        print(f"INFO(message.extract_message)-{conversa['TELEFONE'][0]} message_text: áudio:", is_user_audio(body_json))
        
        if not conversa.empty:
            id_campanha = conversa["ID_CAMPANHA"][0]
        else:
            id_campanha = osFunc('RECEPTIVO')

        if is_user_message(body_json): #Mensagem
            message_text, id_mensagem = get_user_message(body_json)
            if message_text == False:
                message_text = "Emoji"
                response2Client['type'] = 'text'
                response2Client['content'] = "Desculpe, mas não consigo ver esse emoji, você poderia enviar pelo chat?"
            if conversa.empty:
                return message_text, id_mensagem, response2Client, id_flag, id_resultado
        
        elif is_user_audio(body_json): #Áudio
            print(f"INFO(message.extract_message)-{conversa['TELEFONE'][0]} message_text: áudio")
            id_mensagem, audio_type = get_audio(body_json)  
            audio_path = download_media(id_mensagem, audio_type, wpp_token)
            
            try:
                if str(id_campanha) ==str(osFunc('INDIQUE_AMIGOS')):
                    openai_key = str(osFunc('OPENAI_INDIQUE_AMIGOS')) 
                elif str(id_campanha) ==str(osFunc('NOSHOW')):
                    openai_key = str(osFunc('OPENAI_NO_SHOW'))
                elif str(id_campanha) ==str(osFunc('CONFIRMACAO')):
                    openai_key = str(osFunc('OPENAI_CONFIRMACAO'))
                elif str(id_campanha) ==str(osFunc('CANCELADOS')):
                    openai_key = str(osFunc('OPENAI_CANCELADOS'))
                elif str(id_campanha) ==str(osFunc('RECEPTIVO')):
                    openai_key = str(osFunc('OPENAI_RECEPTIVO'))
                elif str(id_campanha) ==str(osFunc('CONFIRMACAO_2H')):
                    openai_key = str(osFunc('OPENAI_CONFIRMACAO_2H'))
                elif str(id_campanha) ==str(osFunc('LEADS_WHATSAPP')):
                    openai_key = str(osFunc('OPENAI_LEADS_WHATSAPP'))
                elif str(id_campanha) ==str(osFunc('LEADS_META')):
                    openai_key = str(osFunc('OPENAI_LEADS_META'))
                else:
                    print(f"INFO(message.extract_message)-{conversa['TELEFONE'][0]} Campanha não encontrada para transcrição do áudio: {id_campanha}")
                    openai_key = osFunc('OPENAI_TESTE_API')
                
                message_text = transcribe_audio(audio_path, openai_key)
                
            except: 
                print(f"ERROR(message.extract_message)-{conversa['TELEFONE'][0]}: NAO ACHEI CAMPANHA - Usando Chave comum homolog {traceback.format_exc()}")
                openai_key = osFunc('OPENAI_TESTE_API')
                message_text = transcribe_audio(audio_path, openai_key)
                
            message_text = "Mensagem de áudio: " + message_text

        elif is_user_reaction(body_json): 
            id_mensagem, message_text = get_reaction(body_json)
            
        else: #Outros, pega mensagem estática da base de dados
            type_media = ''
            if is_button_message(body_json): #Botão
                tipo_mensagem = 'BUTTON'
                message_text, id_mensagem = get_button_message(body_json)
            elif is_interactive_message(body_json): #Interativa
                tipo_mensagem = 'INTERACTIVE'
                message_text, id_mensagem = get_interactive_message(body_json)

            elif is_user_location(body_json):
                    tipo_mensagem = 'LOCATION'
                    id_media = body_json["messages"][0]["id"]
                    latitude = body_json["messages"][0]["location"]["latitude"]
                    longitude = body_json["messages"][0]["location"]["longitude"]
                    
                    unidades_proximas = GET_unidades_proximas(latitude, longitude)
                    if unidades_proximas:
                        unidades_proximas = json.loads(unidades_proximas.text)
                        unidade_sigla = unidades_proximas['grupoFranquia']
                        unidade_location = Unidade(unidade_sigla)

                    
                    message_text = f"Localização -> Latitude: {latitude}, Longitude: {longitude}"
                    type_media = "essa localização"

            elif is_contact_message(body_json):

                tipo_mensagem = 'CONTACT'
                message_text = 'contact'

                wa_id = None
                if "messages" in body_json:
                    for message in body_json["messages"]:
                        if message.get("type") == "contacts" and "contacts" in message:
                            for contact in message["contacts"]:
                                if "phones" in contact:
                                    for phone in contact["phones"]:
                                        wa_id = phone.get("wa_id")
                                        break
                                if wa_id:
                                    break
                        if wa_id:
                            break

                if wa_id:
                    message_text = f"Contato: {wa_id}"
                else:
                    message_text = "Contato"
            
                    
            else: #Outras midias
                tipo_mensagem = 'MIDIA'
                
                if is_user_document(body_json):
                    id_media = body_json["messages"][0]["document"]["id"]
                    media_type = body_json["messages"][0]["type"]
                    media_extension =  body_json["messages"][0]["document"]["mime_type"]
                    media_extension = media_extension.split('/')[-1]
                    message_text = "Documento"
                    type_media = "esse documento"

                elif is_user_image(body_json):
                    id_media = body_json["messages"][0]["image"]["id"]
                    media_type = body_json["messages"][0]["type"]
                    media_extension =  body_json["messages"][0]["image"]["mime_type"]
                    media_extension = media_extension.split('/')[-1]

                    caption = None
                    if "messages" in body_json:
                        for message in body_json["messages"]:
                            if message.get("type") == "image" and "image" in message:
                                caption = message["image"].get("caption")
                    if caption:
                        message_text = f"Imagem com legenda: {caption}"
                    else:
                        message_text = "Imagem sem legenda"
                    media_type = "image"
                    type_media = "essa imagem"

                elif is_user_video(body_json):
                    id_media = body_json["messages"][0]["video"]["id"]
                    media_type = body_json["messages"][0]["type"]
                    media_extension =  body_json["messages"][0]["video"]["mime_type"]
                    media_extension = media_extension.split('/')[-1]
                    caption = None
                    if "messages" in body_json:
                        for message in body_json["messages"]:
                            if message.get("type") == "video" and "video" in message:
                                caption = message["video"].get("caption")
                    if caption:
                        message_text = f"Video com legenda: {caption}"
                    else:
                        message_text = "Video sem legenda"

                    type_media = "esse vídeo"

                elif is_user_sticker(body_json):
                    id_media = body_json["messages"][0]["sticker"]["id"]
                    media_type = body_json["messages"][0]["type"]
                    media_extension =  body_json["messages"][0]["sticker"]["mime_type"]
                    media_extension = media_extension.split('/')[-1]
                    message_text = "Figurinha"
                    type_media = "essa figurinha"
                
                    
                elif is_ephemeral_message(body_json):
                    id_media = ''
                    message_text = "Ephemeral"
                    type_media = "essa mensagem"
                
                else:
                    id_media = ''
                    message_text = "Mídia não reconhecida"
                    type_media = "essa mensagem"
                    print(f"WARNING(message.extract_message)-{conversa['TELEFONE'][0]}: mídia não reconhecida: {body_json}")
                

            query = f"""
                SELECT * 
                FROM MENSAGENS_ESTATICAS
                WHERE ID_CAMPANHA = {id_campanha} AND TIPO_ENTRADA = '{tipo_mensagem}' AND (CONTEUDO_ENTRADA = '{message_text}' OR CONTEUDO_ENTRADA = '-')
            """
            print("QUERY BUTTON:",query)
            df = query_to_dataframe(query)
            
            if not df.empty and not df['CONTEUDO_RESPOSTA'].iloc[0] == '-' :
                telefone, cliente, unidade_nome, date, concat_times = gerar_variaveis_mensagem_automatica(conversa, today, http_session)
                string_resposta = df['CONTEUDO_RESPOSTA'].iloc[0]
                if concat_times == '' and '{concat_times}' in string_resposta:
                    string_resposta = f"Desculpe mas para a unidade {unidade_nome} não temos horários disponíveis na nossa agenda, gostaria de ver se tem outra unidade perto de você?"
                
                response2Client['content'] = string_resposta.replace('{type_media}', type_media) \
                                                    .replace('{telefone}', str(telefone)) \
                                                    .replace('{cliente}', cliente) \
                                                    .replace('{unidade_nome}', unidade_nome) \
                                                    .replace('{date}', date) \
                                                    .replace('{concat_times}', concat_times)\
                                                    .replace('{wpp_pes}', telefone_wpp)
                
                if tipo_mensagem == 'LOCATION':

                    response2Client['content'] = response2Client['content'].replace('{nome_unidade_location}', unidade_location.nome)\
                                                    .replace('{endereco_unidade_location}', unidade_location.endereco)\
                                                    .replace('{wpp_pes}', osFunc('TELEFONE_PRODUCAO'))

                response2Client['type'] = df['TIPO_SAIDA'].iloc[0]
                
                id_flag = df['ID_FLAG'].iloc[0]
                acao = df['ACAO'].iloc[0]
    
                if acao:
                    # MUDAR FUNÇÃO DAS AÇÕES
                    match = re.search(r"alterar_campanha\(([^)]+)\)", acao)
                    if match:
                        conteudo = match.group(1)
                        contato = Contato(conversa["TELEFONE"][0])
                        conversa_obj = Conversa(contato, id_campanha)
                        conversa_obj.change_campanha(osFunc(conteudo)) ### CHANGE CAMPANHA NA CAMADA
                        id_campanha = osFunc(conteudo)
                    else:
                        id_flag, id_resultado, resp_change = fazer_acao(acao, conversa, today, http_session)
                        if resp_change:
                            response2Client['content'] = resp_change
            
            elif not df.empty and df['CONTEUDO_RESPOSTA'].iloc[0] == '-': #Mensagem de Avaliação
                response2Client['type'] = None
                response2Client['content'] = ''
                id_flag = osFunc("FINALIZADO")
                
        id_mensagem = ''
        print(f"INFO(message.extract_message)-{conversa['TELEFONE'][0]} message_text: {message_text}, id_mensagem: {id_mensagem}, response2Client: {response2Client}, id_flag: {id_flag}, id_resultado: {id_resultado}, id_campanha: {id_campanha}")
        return message_text, id_mensagem, response2Client, id_flag, id_resultado, id_campanha, id_media, media_type, media_extension
    except Exception as e:
        print(f"ERROR(message.extract_message)-{conversa['TELEFONE'][0]}: Falha ao extrair mensagem {traceback.format_exc()}")
        
def gerar_variaveis_mensagem_automatica(conversa, today, http_session):
    cliente = conversa['NOME_CLIENTE'][0]
    telefone = conversa['TELEFONE'][0] 
    
    if conversa['UNIDADE_PSD'][0]:
        next_weeks = today + timedelta(days=osFunc("BLOQUEIO_AGENDA"))
        datas_disponiveis = GET_vagas_disponiveis(today.strftime("%Y-%m-%d"), 
                            next_weeks.strftime("%Y-%m-%d"), 
                            conversa['UNIDADE_PSD'][0], http_session)
        vagas = json.loads(datas_disponiveis.text)
        # Verifica se a resposta contém a chave 'sucesso'
        if 'resposta' in vagas and 'sucesso' in vagas['resposta']:
            if not vagas['resposta']['sucesso']:
                conversa['UNIDADE_PSD'][0] = None

    if conversa['UNIDADE_PSD'][0] is None:
        sigla = get_sigla_ddd(conversa['TELEFONE'][0])
        sigla = checa_agenda_vazia(sigla)
        conversa['UNIDADE_PSD'][0] = sigla
        
        
    next_weeks = today + timedelta(days=osFunc("BLOQUEIO_AGENDA"))
    datas_disponiveis = GET_vagas_disponiveis(today.strftime("%Y-%m-%d"), 
                        next_weeks.strftime("%Y-%m-%d"), 
                        conversa['UNIDADE_PSD'][0], http_session)
    vagas = json.loads(datas_disponiveis.text)
        
    valid_times = []
    for date, times_list in vagas.items(): 
        try:
            if not len(times_list) == 0:
                valid_times = [time for time, vagasDisponiveis in times_list.items() if vagasDisponiveis['total'] > 0]
                if len(valid_times) > 0:
                    break
        except Exception as e:
            print(f"ERROR(message.gerar_variaveis_mensagem_automatica)-{conversa['TELEFONE'][0]}: Falha ao extrair valid_times {traceback.format_exc()}")

    #date = datetime.strptime(date, '%Y-%m-%d').strftime('%d/%m/%Y')
    date = formatar_data(date)

    valid_times = [s[:-3] for s in valid_times]
    concat_times = '\n- '.join(valid_times)

    unidade = Unidade(conversa['UNIDADE_PSD'][0])
    unidade_nome = unidade.nome
    return telefone, cliente, unidade_nome, date, concat_times


def fazer_acao(acao, conversa, today, http_session):
    id_flag = osFunc("FINALIZADO")
    id_resultado = osFunc("RESPONDIDO")
    resp_change = None

    contato = Contato(conversa['TELEFONE'][0])
    agendamento = contato.agendamento
    if agendamento_passado(agendamento.data_agendamento, agendamento.horario_agendamento, agendamento.unidade_agendamento.sigla):
        if acao == "CONFIRMAR":
            id_flag = osFunc("AGUARDAR_POS")
            id_resultado = osFunc("RESPONDIDO")
            resp_change = "Parece que o horário da sua avaliação já passou, lembrando que oferecemos 15 minutos de tolerância para atrasos. Deu tudo certo ou você gostaria de reagendar?" 
    else:
#################################################################################################################################################################################################################
        agendamento_json = POST_buscar_telefone(conversa['TELEFONE'][0],id_campanha=conversa['ID_CAMPANHA'][0], banco_dados= conversa['BANCO_DADOS'][0], listar_todos=1)
        telefone, cliente, unidade_nome, date, concat_times = gerar_variaveis_mensagem_automatica(conversa, today, http_session)
        # verificando quantos clientes existem para serem confirmados
        if agendamento_json["sucesso"]:
            agendamentos = agendamento_json["agendamentos"]
            n_clientes = len(agendamentos)
        else:
            n_clientes = 1

        # Resposta para mais de uma confirmação
        if n_clientes > 1:
            list_agendamentos = ''
            for agn in agendamentos: 
                dia = agn['atendimento_em'][8:10] + '/' + agn['atendimento_em'][5:7] + '/' +  agn['atendimento_em'][:4]
                horario = agn['atendimento_em'][-8:-3]
                model_unidade = Unidade(agn['unidade_atendimento'])
                unidade = model_unidade.nome
                list_agendamentos += f"\n- {agn['nome_cliente']} às {horario} no dia {dia} na unidade {unidade}"

            if acao == "CONFIRMAR": 
                id_flag = osFunc("AGUARDAR")
                id_resultado = osFunc("RESPONDIDO")
                resp_change = f"Recebi sua confirmação, porém verifiquei que você possui mais de um agendamento em nosso sistema.\n{list_agendamentos}\n\nVocê gostaria de confirmar todas as avaliações?"
            elif acao == "CANCELAR":
                id_flag = osFunc("AGUARDAR")
                id_resultado = osFunc("RESPONDIDO")
                resp_change = f"""Recebi sua solicitação de reagendamento, porém verifiquei que você possui mais de um agendamento em nosso sistema.\n{list_agendamentos}\n\nVocê gostaria de reagendar todas as avaliações?""" 

#################################################################################################################################################################################################################

        if acao == "CONFIRMAR":
            resultado_api = confirma_agendamento(conversa['TELEFONE'][0], id_campanha = conversa["ID_CAMPANHA"][0], banco_dados=conversa["BANCO_DADOS"][0])
            if resultado_api: 
                id_resultado = osFunc('SUCESSO')
                nome = conversa['NOME_CLIENTE'][0]
                nome = nome.split()
                if len(nome) == 1:
                    resp_change = 'Obrigado pela sua resposta! Por favor, lembre-se de trazer um documento de identificação pessoal e os dois calçados que você mais utiliza para a avaliação. Poderia informar seu nome completo para podermos identificá-lo quando chegar?'
            else:
                #tentar agendar
                conversa_obj = Conversa(contato, conversa['ID_CAMPANHA'][0], conversa['DATETIME_CONVERSA'][0])
                datetime_agendamento = [f"{conversa_obj.contato.agendamento.data_agendamento}" + ' ' + f"{conversa_obj.contato.agendamento.horario_agendamento}"] 
                _, resposta_agendamento, _ = registra_agendamentoAPI(conversa_obj, conversa_obj.contato.agendamento.unidade_agendamento, [conversa_obj.contato.nome], datetime_agendamento)
                # print(conversa_obj, conversa_obj.contato.agendamento.unidade_agendamento, [conversa_obj.contato.nome], datetime_agendamento)
                # print(resposta_agendamento)  
                if resposta_agendamento in [1,3]:     
                    resultado_api = confirma_agendamento(conversa['TELEFONE'][0], id_campanha = conversa["ID_CAMPANHA"][0], banco_dados=conversa["BANCO_DADOS"][0])
                    if resultado_api:
                        id_resultado = osFunc('SUCESSO')
                        nome = conversa['NOME_CLIENTE'][0]
                        nome = nome.split()
                        if len(nome) == 1:
                            resp_change = 'Obrigado pela sua resposta! Por favor, lembre-se de trazer um documento de identificação pessoal e os dois calçados que você mais utiliza para a avaliação. Poderia informar seu nome completo para podermos identificá-lo quando chegar?'
                    else:  
                        id_resultado = osFunc('FALHA_AGENDAMENTO')
                else: 
                    #não conseguiu agendar no horário passado (está cheio)
                    #precisa avisar a pessoa
                    telefone, cliente, unidade_nome, date, concat_times = gerar_variaveis_mensagem_automatica(conversa, today, http_session)
                    resp_change = """Parece que o horário da sua avaliação já passou ou não está mais disponível, lembrando que oferecemos 15 minutos de tolerância para atrasos. Seria necessário reagendar para outro momento. Para a unidade {unidade_nome}, a próxima data disponível é {date} nos seguintes horários: 

- {concat_times} 

Qual seria a melhor opção para você?"""
                    resp_change = resp_change.replace('{unidade_nome}', unidade_nome) \
                                             .replace('{date}', date) \
                                             .replace('{concat_times}', concat_times)
                    id_resultado = osFunc('INTERVENCAO') 
        elif acao == "CANCELAR":
            resultado_api = cancela_agendamento(conversa['TELEFONE'][0], id_campanha = conversa["ID_CAMPANHA"][0], banco_dados=conversa["BANCO_DADOS"][0])
            if resultado_api:
                id_resultado = osFunc('RESPONDIDO')
            else:
                id_resultado = osFunc("FALHA_AGENDAMENTO")
    return id_flag, id_resultado, resp_change

def cancela_agendamento(telefone, id_campanha = 0, banco_dados = 0):
    try:
        agendamento_json = POST_buscar_telefone(telefone, id_campanha=id_campanha, banco_dados=banco_dados)
        if agendamento_json["sucesso"]: 
            token_cancelamento = agendamento_json["token_cancelamento"]
            horario_agendamento = agendamento_json["atendimento_em"][11:16]
            data_agendamento = agendamento_json["atendimento_em"][0:10]
            response = POST_cancelar_agendamento(token_cancelamento, id_campanha=id_campanha, banco_dados=banco_dados).text
            print(f"INFO(message.cancela_agendamento)-{telefone}: Cancelando agendamento dia {horario_agendamento} às {data_agendamento}, Response: {response}")
        return True
    except Exception as e:
        print(f"ERROR(message.cancela_agendamento)-{telefone}: Erro ao cancelar o agendamento {traceback.format_exc()}")
        return False
    
def confirma_agendamento(telefone, id_campanha = 0, banco_dados = 0):
    try:
        agendamento_json = POST_buscar_telefone(telefone, id_campanha=id_campanha, banco_dados=banco_dados)
        if agendamento_json["sucesso"]: 
            token_confirmacao = agendamento_json["token_confirmacao"]
            horario_agendamento = agendamento_json["atendimento_em"][11:16]
            data_agendamento = agendamento_json["atendimento_em"][0:10]
            response = POST_confirmar_agendamento(token_confirmacao, id_campanha = id_campanha, banco_dados=banco_dados)
            print(f"INFO(message.confirma_agendamento)-{telefone}: Confirmando agendamento dia {horario_agendamento} às {data_agendamento}, Response: {response}")
            return True
        print(f"WARNING(message.confirma_agendamento)-{telefone}: Agendamento anterior não detectado")
        return False #naõ tem agendamento pra confirmar
    except Exception as e:
        print(f"ERROR(message.confirma_agendamento)-{telefone}: Erro ao confirmar o agendamento {traceback.format_exc()}")
        return False

def is_user_audio(data):
    return (
        'messages' in data and
        len(data['messages']) > 0 and
        'type' in data['messages'][0] and
        data['messages'][0]['type'] == "audio"
    )

def is_user_document(data):
    if "messages" in data:
        for message in data["messages"]:
            if message.get("type") == "document":
                return True
    return False

def is_user_image(data):
    if "messages" in data:
        for message in data["messages"]:
            if message.get("type") == "image":
                return True
    return False

def is_user_video(data):
    if "messages" in data:
        for message in data["messages"]:
            if message.get("type") == "video":
                return True
    return False

def is_user_sticker(data):
    if "messages" in data:
        for message in data["messages"]:
            if message.get("type") == "sticker":
                return True
    return False

def is_user_reaction(data):
    if "messages" in data:
        for message in data["messages"]:
            if message.get("type") == "reaction":
                return True
    return False

def is_user_message(data):
    return (
        'messages' in data and
        len(data['messages']) > 0 and
        'type' in data['messages'][0] and
        data['messages'][0]['type'] == "text"
    )

def is_button_message(data):
    return (
        'messages' in data and
        len(data['messages']) > 0 and
        'type' in data['messages'][0] and
        data['messages'][0]['type'] == "button"
    )

def is_interactive_message(data):
    return (
        'messages' in data and
        len(data['messages']) > 0 and
        'type' in data['messages'][0] and
        data['messages'][0]['type'] == "interactive"
    )

def is_contact_message(data):
    return(
        'messages' in data and
        len(data['messages']) > 0 and
        'type' in data['messages'][0] and
        data['messages'][0]['type'] == "contacts"
    )

def is_user_location(data):
    return(
        'messages' in data and
        len(data['messages']) > 0 and
        'type' in data['messages'][0] and
        data['messages'][0]['type'] == "location"
    )
        
def is_ephemeral_message(data):
    return(
        'messages' in data and
        len(data['messages']) > 0 and
        'type' in data['messages'][0] and
        data['messages'][0]['type'] == "ephemeral"
    )



def get_user_message(data):
    if 'errors' in data['messages'][0].keys():
        return False, False
    return data['messages'][0]['text']['body'], data['messages'][0]['id']

def get_button_message(data):
    return data['messages'][0]['button']['text'], data['messages'][0]['id']

def get_interactive_message(data):
    return data['messages'][0]['interactive']['list_reply']['title'], data['messages'][0]['id']

def get_audio(data):
    try:
        audio_info = data['messages'][0]['audio']
    except Exception as e:
        print(f"ERROR(message.get_audio): erro em pegar audio_info - {traceback.format_exc()}")
            
    audio_id = audio_info['id']
    audio_type = audio_info['mime_type']
    audio_type = audio_type.split(';')[0].split('/')[-1].strip()
    
    return audio_id, audio_type

def get_document(data):
    document_id = data['messages'][0]['document']['id']
    document_type = data['messages'][0]['document']['mime_type']
    document_name = data['messages'][0]['document']['filename']
    return document_id, document_type, document_name

def get_image(data):
    image_id = data['messages'][0]['image']['id'] 
    image_type = data['messages'][0]['image']['mime_type']
    return image_id, image_type

def get_video(data):
    video_id = data['messages'][0]['video']['id']
    video_type = data['messages'][0]['video']['mime_type']
    return video_id, video_type

def get_sticker(data):
    sticker_id = data['messages'][0]['sticker']['id']
    sticker_type = data['messages'][0]['sticker']['mime_type']
    return sticker_id, sticker_type

def get_reaction(data):
    reaction_id = data['messages'][0]['reaction']['message_id']
    reaction_type = data['messages'][0]['reaction']['emoji']
    return reaction_id, reaction_type

