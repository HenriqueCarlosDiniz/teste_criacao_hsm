import tkinter as tk
from tkinter import ttk, messagebox
from GUIs.ui_campanha import setup_ui_campanha
import GUIs.database_operations as database_operations
from GUIs.ui_utils import insert_data, update_data, input_fields_on_tab, update_input_fields, fetch_data


def setup_ui(root, pags):
    # Criação do Notebook (Gerenciador de abas)
    notebook = ttk.Notebook(root)
    notebook.pack(padx=10, pady=10, expand=True, fill='both')

    for pag in pags.values():
        # Cria um frame para a página atual no loop e o adiciona ao notebook
        frame = ttk.Frame(notebook)
        notebook.add(frame, text=pag.name)

        # Configuração dos campos de entrada específicos para busca
        entries, input_fields = setup_tab_with_search_fields(frame, pag)

        # Criação e configuração dos botões de inserção e atualização
        setup_action_buttons(frame, pag, input_fields, entries)

    campanha_frame = ttk.Frame(notebook)
    notebook.add(campanha_frame, text='CAMPANHAS')
    setup_ui_campanha(campanha_frame, pags)

    search_frame = ttk.Frame(notebook)
    notebook.add(search_frame, text='SEARCH')
    setup_dynamic_widgets(search_frame)


### Função para Configurar os Botões de Ação
def setup_action_buttons(tab, pag, input_fields, entries):
    # Botão de Inserção
    insert_button = ttk.Button(tab, text="Inserir Dados", command=lambda: insert_data(pag.name, input_fields))
    insert_button.pack(pady=5)

    # Botão de Atualização
    update_button = ttk.Button(tab, text="Atualizar Dados", command=lambda: update_data(pag, input_fields, entries))
    update_button.pack(pady=5)


def setup_tab_with_search_fields(tab, pag):
    search_frame = ttk.Frame(tab)
    search_frame.pack(padx=10, pady=5, fill='x')

    # Dicionário para campos de busca
    entries = {}
    # Dicionário para campos de entrada que serão preenchidos após a busca
    input_fields = {}

    # Configura os campos de busca
    for key in pag.keys:  # Assumindo que `pag` tem um atributo `search_keys` para campos de busca
        label = ttk.Label(search_frame, text=f"{key}:")
        label.pack(side=tk.LEFT)
        entry = ttk.Entry(search_frame)
        entry.pack(side=tk.LEFT, padx=5)
        entries[key] = entry

    fetch_button = ttk.Button(tab, text="Buscar Dados", command=lambda: fetch_data(pag, input_frame= input_frame, input_fields = input_fields, entries = entries))
    fetch_button.pack(pady=5)

    # Configura os campos de entrada que são adicionados após a busca
    input_frame = ttk.Frame(tab)
    input_frame.pack(padx=10, pady=10)

    # Note que essa chamada agora passa `input_fields` para serem configurados separadamente
    input_fields_on_tab(pag, input_frame, input_fields)

    return entries, input_fields

def setup_dynamic_widgets(root):
    # Frame para conter as linhas dinâmicas
    dynamic_frame = ttk.Frame(root)
    dynamic_frame.grid(column=0, row=1, sticky='nw', padx=10, pady=10)

    # Contador e lista para manter as linhas adicionadas
    line_counter = [0]
    lines = []

    # Função para adicionar uma nova linha de widgets
    def add_line():
        row = line_counter[0]

        # Combobox com opções de exemplo
        combo_var = tk.StringVar()
        combobox = ttk.Combobox(dynamic_frame, textvariable=combo_var, values=["Opção 1", "Opção 2", "Opção 3"], state='readonly')
        combobox.grid(column=0, row=row, padx=10, pady=5, sticky='w')
        combobox.current(0)

        # Entry para entrada de texto
        entry = ttk.Entry(dynamic_frame)
        entry.grid(column=1, row=row, padx=10, pady=5, sticky='w')

        # Botão de lixinho para apagar a linha
        def remove_line():
            for widget in lines[row]:
                widget.destroy()
            lines[row] = []

        delete_button = ttk.Button(dynamic_frame, text="🗑️", command=remove_line)
        delete_button.grid(column=2, row=row, padx=10, pady=5, sticky='w')

        # Adiciona os widgets à lista de controle e incrementa o contador
        lines.append((combo_var, entry, delete_button))
        line_counter[0] += 1

    # Botão "Adicionar" para criar novas linhas, posicionado no topo da janela
    add_button = ttk.Button(root, text="Adicionar", command=add_line)
    add_button.grid(column=0, row=0, padx=10, pady=10)

    # Função para coletar informações de todas as linhas
    def collect_info():
        collected_info = []
        for combo_var, entry, _ in lines:
            if entry.winfo_exists():  # Verifica se o widget ainda existe
                collected_info.append((combo_var.get(), entry.get()))
        print(collected_info)  # Aqui você pode substituir por qualquer operação com as informações coletadas

    # Botão "Coletar Informações" para capturar os dados inseridos
    collect_button = ttk.Button(root, text="Coletar Informações", command=collect_info)
    collect_button.grid(column=0, row=2, padx=10, pady=10, sticky='w')


# def setup_ui_general_search(tab, pag):


