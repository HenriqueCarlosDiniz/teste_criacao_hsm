import tkinter as tk
from ttkthemes import ThemedTk
import GUIs.ui_geral as ui_geral
from GUIs.pagina import Pagina
import sys
sys.path.append('../../novo_proj')
from utils.config import *
from utils.connect_db import get_table_columns


# Tabelas
MENSAGEM_ESTATICA = 'MENSAGENS_ESTATICAS'
CONTEXTO = 'CONTEXTO'
STRING_CONTEXTO = 'STRING_CONTEXTO'
DISPARO = 'DISPARO'
USER_STRING = 'USER_STRING'
REP_MSG = 'REP_MSG'
REPESCAGEM = 'REPESCAGEM'
HSM = 'HSM'

KEYS = {CONTEXTO: ['id_campanha', 'nome_contexto'],
        STRING_CONTEXTO:['ID_string_contexto'],
        DISPARO:['ID_CAMPANHA', 'ID_DISPARO'],
        MENSAGEM_ESTATICA:['id_campanha', 'id_mensagem'],
        REP_MSG:['ID'],
        REPESCAGEM:['FLAG', 'ID_CAMPANHA'],
        HSM:['ID_HSM'],
        USER_STRING:['ID_STRING'],
        }


def main():
    # Cria a janela principal
    pags = {}
    for name in KEYS.keys():
        pags.update({name: Pagina(name, KEYS[name])})
    root = tk.Tk()
    # root = ThemedTk(theme = 'arc')
    root.title("Database GUI")
    root.geometry("800x600")  # Largura x Altura em pixels
      

    # Configura e inicializa a interface de usuário
    ui_geral.setup_ui(root, pags)
    # ui.setup_ui(root)

    # Inicia o loop principal do Tkinter
    root.mainloop()

if __name__ == "__main__":
    main() 
