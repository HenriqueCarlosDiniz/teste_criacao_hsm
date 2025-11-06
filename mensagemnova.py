from utils.config import *

import json
import traceback
import pytz
from datetime import datetime, timedelta
import re

from utils.api_communication import GET_vagas_disponiveis, POST_buscar_telefone, POST_cancelar_agendamento, POST_confirmar_agendamento, registra_agendamentoAPI, buscar_unidades
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

def extract_message(body_json, conversa, today, http_session):
    message_text = None
    try:
        response2Client = {'type': None, 'content': None}
        id_flag = osFunc("RESPONDER")
        id_resultado = osFunc("RESPONDIDO")
        
        print(f"INFO(message.extract_message)-{conversa['ID_CONTATO'][0]} message_text: áudio:", is_user_audio(body_json))
        
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
            print(f"INFO(message.extract_message)-{conversa['ID_CONTATO'][0]} message_text: áudio")
            id_mensagem, audio_type = get_audio(body_json)
            audio_path = download_media(id_mensagem, audio_type)
            
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
                    print(f"INFO(message.extract_message)-{conversa['ID_CONTATO'][0]} Campanha não encontrada para transcrição do áudio: {id_campanha}")
                    openai_key = osFunc('OPENAI_TESTE_API')
                
                message_text = transcribe_audio(audio_path, openai_key)
                
            except: 
                print(f"ERROR(message.extract_message)-{conversa['ID_CONTATO'][0]}: NAO ACHEI CAMPANHA - Usando Chave comum homolog {traceback.format_exc()}")
                openai_key = osFunc('OPENAI_TESTE_API')
                message_text = transcribe_audio(audio_path, openai_key)
                
            message_text = "Mensagem de áudio: " + message_text
            
        else: #Outros, pega mensagem estática da base de dados
            type_media = ''
            if is_button_message(body_json): #Botão
                tipo_mensagem = 'BUTTON'
                message_text, id_mensagem = get_button_message(body_json)
            elif is_interactive_message(body_json): #Interativa
                tipo_mensagem = 'INTERACTIVE'
                message_text, id_mensagem = get_interactive_message(body_json)
            elif is_contact_message(body_json):
                tipo_mensagem = 'CONTACT'
                message_text = 'contact'
            else: #Outras midias
                tipo_mensagem = 'MIDIA'
                if is_user_document(body_json):
                    id_mensagem = body_json['messages'][0]['id']  
                    message_text = "Documento"
                    type_media = "esse documento"

                elif is_user_image(body_json):
                    id_mensagem = body_json["messages"][0]["image"]["id"]
                    message_text = "Imagem"
                    type_media = "essa imagem"

                elif is_user_video(body_json):
                    id_mensagem = body_json["messages"][0]["video"]["id"]
                    message_text = "Video"
                    type_media = "esse vídeo"

                elif is_user_sticker(body_json):
                    id_mensagem = body_json["messages"][0]["sticker"]["id"]
                    message_text = "Figurinha"
                    type_media = "essa figurinha"
                
                elif is_user_location(body_json):
                    id_mensagem = body_json["messages"][0]["id"]
                    message_text = "Localização"
                    type_media = "essa localização"
                    
                elif is_ephemeral_message(body_json):
                    id_mensagem = ''
                    message_text = "Ephemeral"
                    type_media = "essa mensagem"
                
                else:
                    id_mensagem = ''
                    message_text = "Mídia não reconhecida"
                    type_media = "essa mensagem"
                    print(f"WARNING(message.extract_message)-{conversa['ID_CONTATO'][0]}: mídia não reconhecida: {body_json}")
                

            query = f"""
                SELECT * 
                FROM MENSAGENS_ESTATICAS
                WHERE ID_CAMPANHA = {id_campanha} AND TIPO_ENTRADA = '{tipo_mensagem}' AND (CONTEUDO_ENTRADA = '{message_text}' OR CONTEUDO_ENTRADA = '-')
            """
            print("QUERY BUTTON:",query)
            df = query_to_dataframe(query)
            
            if not df.empty and not df['CONTEUDO_RESPOSTA'].iloc[0] == '-' :
                string_resposta = df['CONTEUDO_RESPOSTA'].iloc[0]

                response2Client['content'] = string_resposta
                response2Client['type'] = df['TIPO_SAIDA'].iloc[0]
                
                id_flag = df['ID_FLAG'].iloc[0]
            elif not df.empty and df['CONTEUDO_RESPOSTA'].iloc[0] == '-': #Mensagem de Avaliação
                response2Client['type'] = None
                response2Client['content'] = ''
                id_flag = osFunc("FINALIZADO")
                
        id_mensagem = None
        print(f"INFO(message.extract_message)-{conversa['ID_CONTATO'][0]} message_text: {message_text}, id_mensagem: {id_mensagem}, response2Client: {response2Client}, id_flag: {id_flag}, id_resultado: {id_resultado}, id_campanha: {id_campanha}")
        return message_text, id_mensagem, response2Client, id_flag, id_resultado, id_campanha
    except Exception as e:
        print(f"ERROR(message.extract_message)-{conversa['ID_CONTATO'][0]}: Falha ao extrair mensagem {traceback.format_exc()}")

def is_user_message(data):
    return (
        'message' in data and
        'text' in data['message']
    )

def is_user_postback(data):
    return (
        'postback' in data and
        'payload' in data['postback']
    )

def is_user_audio(data):
    return (
        'messages' in data and
        len(data['messages']) > 0 and
        (
        'voice' in data['messages'][0] and
        'id' in data['messages'][0]['voice']
        )
        or
        (
        'audio' in data['messages'][0] and
        'id' in data['messages'][0]['audio']
        )
    )

def is_user_document(data):
    return (
        'messages' in data and
        len(data['messages']) > 0 and
        'document' in data['messages'][0] and
        'id' in data['messages'][0]['document']
    )

def is_user_image(data):
    return (
        'messages' in data and
        len(data['messages']) > 0 and
        'image' in data['messages'][0] and
        'id' in data['messages'][0]['image']
    )

def is_user_video(data):
    return (
        'messages' in data and
        len(data['messages']) > 0 and
        'video' in data['messages'][0] and
        'id' in data['messages'][0]['video']
    )

def is_user_sticker(data):
    return (
        'messages' in data and
        len(data['messages']) > 0 and
        'sticker' in data['messages'][0] and
        'id' in data['messages'][0]['sticker']
    )

def is_button_message(data):
    return (
        'messages' in data and
        len(data['messages']) > 0 and
        'button' in data['messages'][0] and
        'text' in data['messages'][0]['button']
    )

def is_interactive_message(data):
    return (
        'messages' in data and
        len(data['messages']) > 0 and
        'interactive' in data['messages'][0] and
        'list_reply' in data['messages'][0]['interactive']
    )

def is_contact_message(data):
    try:
        if 'messages' in data and len(data['messages']) > 0:
            return data['messages'][0].get('type') == 'contacts'
        return False
    except KeyError:
        return False

def is_user_location(data):
    try:
        if 'messages' in data and len(data['messages']) > 0:
            return data['messages'][0].get('type') == 'location'
        return False
    except KeyError:
        return False
        
def is_ephemeral_message(data):
    try:
        if 'messages' in data and len(data['messages']) > 0:
            return data['messages'][0].get('type') == 'ephemeral'
        return False
    except KeyError:
        return False

def get_user_message(data):
    if 'errors' in data['message'].keys():
        return False, False
    return data['messages']['text'], data['message']['mid']

def get_user_postback(data):
    if 'errors' in data['postback'].keys():
        return False, False
    return data['postback']['payload'], data['postback']['mid']

def get_button_message(data):
    return data['messages'][0]['button']['text'], data['messages'][0]['id']

def get_interactive_message(data):
    return data['messages'][0]['interactive']['list_reply']['title'], data['messages'][0]['id']

def get_audio(data):
    try:
        audio_info = data['messages'][0]['voice']
    except:
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