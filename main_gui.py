from utils.config import *
from Models.contato import Contato
from Models.conversa import Conversa
from utils.connect_db import insert_data_into_db, query_to_dataframe, execute_query_db
import os, requests, datetime, pytz
import traceback
#import utils.config
# from recebeMensagem.lambda_function import  lambda_handler as recebe_mensagem
import json, os
from recebeMensagem.lambda_function import lambda_handler as recebe_mensagem 

CAMPANHAS = query_to_dataframe("SELECT NOME_CAMPANHA FROM CAMPANHA")['NOME_CAMPANHA'].to_list()

def mensagem_wpp(mensagem, telefone):
    timestamp = datetime.datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S.%f")
    event = {
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
                                "wa_id": telefone
                            }
                        ],
                        "messages": [
                            {
                                "from": telefone,
                                "id": "wamid.HBgNNTUxMTkxOTE1ODg5MhUCABIYIDNCNURFQURFQzI4MUY2RkFDREIxOEFGMkRDRjk0Rjk2AA==",
                                "timestamp": timestamp,
                                "text": {
                                    "body": mensagem
                                },
                                "type": "text"
                            }
                        ]
                    },
                    "field": "messages"
                }
            ]
        }
    ]
}
    return event

def button_msg(text, telefone):
    timestamp = datetime.datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S.%f")
    event = {
        "body":
            {
            "post":{
                "contacts": [
                    {
                    "profile": {
                        "name": "Felipe Benatti"
                    },
                    "wa_id": f"{telefone}"
                    }
                ],
                "messages": [
                    {
                    "button": {
                        "text": f"{text}"
                    },
                    "context": {
                        "from": "5511959861043",
                        "id": "gBGHVRGRkViJLwIJnXpO-LSndXTr"
                    },
                    "from": f"{telefone}",
                    "id": "ABGHVRGRkViJLwIQFeGi7N9C1azMWsNRRfR5Uw",
                    "timestamp": "1698178409",
                    "type": "button",
                    "telefone_wpp": "5511959861043"
                    }
                ]
            },

            "timestamp": timestamp
        }
    }
    return event

def interactive_msg(text, telefone):
    timestamp = datetime.datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S.%f")
    event = {
            "body":{
                "post":{
                    "contacts": [
                        {
                        "profile": {
                            "name": "Pedro Gianjoppe 🇧🇷🇮🇹"
                        },
                        "wa_id": f"{telefone}"
                        }
                    ],
                    "messages": [
                        {
                        "context": {
                            "from": "5511998571025",
                            "id": "gBGHVRGRkViJLwIJQd1cHHCm-7ps"
                        },
                        "from": f"{telefone}",
                        "id": "ABGHVRGRkViJLwILPrCgB8MoUfetmWo",
                        "interactive": {
                            "list_reply": {
                            "description": "Saiba mais sobre a avaliação com nosso especialista!",
                            "id": "1",
                            "title": f"{text}"
                            },
                            "type": "list_reply"
                        },
                        "timestamp": "1703778564",
                        "type": "interactive"
                        }
                    ]
                },
                "timestamp": timestamp,
                "telefone_wpp": "5511959861043"
        }
    }
       
    
    return event


import tkinter as tk
from tkinter import scrolledtext, font
from functools import partial
import json

def clear_conversation(text_widget):
    text_widget.config(state=tk.NORMAL)  # Habilita a caixa de texto para edição
    text_widget.delete('1.0', tk.END)    # Limpa o conteúdo da caixa de texto
    text_widget.config(state=tk.DISABLED)  # Desabilita a caixa de texto novamente

# Função para enviar mensagens e receber respostas do bot
def send_message(entry_widget, text_widget, telefone, msg_type = "wpp"):
    user_input = entry_widget.get()
    text_widget.config(state=tk.NORMAL)  # Habilita a caixa de texto para edição
    text_widget.insert(tk.END, "Você: " + user_input + "\n")

    try:
        if msg_type == "wpp":
            msg_event = mensagem_wpp(user_input, telefone)
        elif msg_type == "btn":
            msg_event = button_msg(user_input, telefone)
        elif msg_type == "itv":
            msg_event = interactive_msg(user_input, telefone)
        resp1 = recebe_mensagem(msg_event, {})
        print("RESP1:", resp1)
        bot_response = recebe_mensagem(msg_event, {})['resposta']

        # Garantir que a resposta do bot seja uma string
        if not isinstance(bot_response, str):
            bot_response = str(bot_response)

        text_widget.insert(tk.END, str("BOT: "+ bot_response + "\n"))
    except Exception as e:
        traceback.print_exc()
        text_widget.insert(tk.END, "Erro ao receber resposta do bot: " + str(e) + "\n")

    text_widget.config(state=tk.DISABLED)  # Desabilita a caixa de texto novamente
    entry_widget.delete(0, tk.END)

def on_enter_press(event, entry_widget, conversation_widget, phone, message_type):
    send_message(entry_widget, conversation_widget, phone, message_type)

def read_selection(new_window, selection_var):
    chosen_option = selection_var.get()
    print(f"Opção escolhida: {chosen_option}")  # Aqui você pode manipular a escolha conforme necessário
    new_window.destroy()

def create_new_window():
    new_window = tk.Toplevel()
    new_window.title("Selecionar Opção")

    selection_var = tk.StringVar(new_window)
    selection_var.set("Opção 1")  # valor padrão

    options = CAMPANHAS
    option_menu = tk.OptionMenu(new_window, selection_var, *options)
    option_menu.pack(pady=10, padx=10)

    select_button = tk.Button(new_window, text="Confirmar", command=lambda: read_selection(new_window, selection_var))
    select_button.pack(pady=5)

def main():
    # telefone = #"10026"#"5511919158892"#5527999932733"#"5511955591026"
    telefone = "5511955591026"

    window = tk.Tk()
    window.title(f"Simulador de Conversa com Bot | Telefone = {telefone}")

    # Definindo o estilo
    bg_color = "#282a36"
    text_color = "#f8f8f2"
    button_color = "#44475a"
    entry_bg = "#6272a4"
    entry_fg = "white"

    # Aplicando o estilo na janela principal
    window.configure(bg=bg_color)

    # Definindo as fontes
    fonte_texto = font.Font(family="Helvetica", size=12)
    fonte_botao = font.Font(family="Helvetica", size=10, weight="bold")

    # Caixa de texto para a conversa
    conversation_text = scrolledtext.ScrolledText(window, font=fonte_texto, wrap=tk.WORD, width=80, height=20, bg=entry_bg, fg=entry_fg)
    conversation_text.pack(pady=10, padx=10)

    # Caixa de entrada de texto
    user_entry = tk.Entry(window, font=fonte_texto, width=80, bg=entry_bg, fg=entry_fg)
    user_entry.pack(pady=5, padx=15)

    button_frame = tk.Frame(window, bg=bg_color)
    button_frame.pack(pady=5, fill='x', anchor='center')

    user_entry.bind("<Return>", lambda event: on_enter_press(event, user_entry, conversation_text, telefone, 'wpp'))

    # Botão para enviar mensagens 
    send_button_msg = tk.Button(button_frame, text="Enviar msg", font=fonte_botao, bg=button_color, fg=text_color, command=partial(send_message, user_entry, conversation_text, telefone, 'wpp'))
    send_button_msg.pack(side=tk.LEFT, padx=5, pady=5)

    send_button_btn = tk.Button(button_frame, text="Enviar btn", font=fonte_botao, bg=button_color, fg=text_color, command=partial(send_message, user_entry, conversation_text, telefone, 'btn'))
    send_button_btn.pack(side=tk.LEFT, padx=5, pady=5)

    send_button_itv = tk.Button(button_frame, text="Enviar itv", font=fonte_botao, bg=button_color, fg=text_color, command=partial(send_message, user_entry, conversation_text, telefone, 'itv'))
    send_button_itv.pack(side=tk.LEFT, padx=5, pady=5)

    new_window_button = tk.Button(button_frame, text="Nova Janela", font=fonte_botao, bg=button_color, fg=text_color, command=create_new_window)
    new_window_button.pack(side=tk.LEFT, padx=5, pady=5)

    # Botão para limpar a conversa
    clear_button = tk.Button(button_frame, text="Limpar Conversa", font=fonte_botao, bg=button_color, fg=text_color, command=lambda: clear_conversation(conversation_text))
    clear_button.pack(padx=5, pady=5)

    window.mainloop()

if __name__ == "__main__":
    main()
