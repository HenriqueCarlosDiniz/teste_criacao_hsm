import tkinter as tk
from tkinter import ttk, messagebox
from GUIs.ui_campanha import setup_ui_campanha
import GUIs.database_operations as database_operations
from GUIs.ui_utils import insert_data, update_data, input_fields_on_tab, update_input_fields, fetch_data
from GUIs.ui_HSM import *

def setup_ui(root, pags):
    # Create a style
    style = ttk.Style(root)
 
    # Set the theme with the theme_use method
    # style.theme_use('alt') 
    # Criação do Notebook (Gerenciador de abas)
    notebook = ttk.Notebook(root)
    notebook.pack(padx=10, pady=10, expand=True, fill='both')

    # Criação da aba 'CAMPANHAS'
    campanha_frame = ttk.Frame(notebook)
    notebook.add(campanha_frame, text='CAMPANHAS')
    setup_ui_campanha(campanha_frame, pags)

    # Criação da aba 'SEARCH'
    search_frame = ttk.Frame(notebook)
    notebook.add(search_frame, text='SEARCH')

    dynamic_frame = ttk.Frame(notebook)
    notebook.add(dynamic_frame, text='DYNAMIC')
    setup_dynamic_widgets(dynamic_frame, pags)

    # Criação da aba 'HSM'
    HSM_frame = ttk.Frame(notebook)
    notebook.add(HSM_frame, text='HSM')
    setup_HSM(HSM_frame)

    # Criação da aba 'TesteHSM'
    TESTE_HSM_frame = ttk.Frame(notebook)
    notebook.add(TESTE_HSM_frame, text='TESTAR HSM')
    setup_teste_HSM(TESTE_HSM_frame)

    HSM_STATUS = ttk.Frame(notebook)
    notebook.add(HSM_STATUS, text='STATUS HSM')
    hsm_status_page(HSM_STATUS)

    # Configuração da aba 'SEARCH' com uma caixa de seleção para as páginas
    setup_search_tab(search_frame, pags)

def setup_search_tab(search_frame, pags):
    # Caixa de seleção com os nomes das páginas
    pag_names = list(pags.keys())
    selected_pag = tk.StringVar()
    select_box = ttk.Combobox(search_frame, textvariable=selected_pag, values=pag_names)
    select_box.pack(pady=10)
    
    # Frame para conter o setup da página selecionada
    page_setup_frame = ttk.Frame(search_frame)
    page_setup_frame.pack(fill='both', expand=True)

    def on_pag_selected(event):
        # Limpa o frame atual antes de adicionar novos widgets
        for widget in page_setup_frame.winfo_children():
            widget.destroy()

        # Obtém a página selecionada e configura os campos de entrada e botões para ela
        pag = pags[selected_pag.get()]
        setup_tab_with_search_fields(page_setup_frame, pag)

    # Chama `on_pag_selected` quando uma nova opção é selecionada
    select_box.bind('<<ComboboxSelected>>', on_pag_selected)


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

    buttons_frame = ttk.Frame(tab)
    buttons_frame.pack(padx=10, pady=10)

    # Criação e configuração dos botões de inserção e atualização
    setup_action_buttons(buttons_frame, pag, input_fields, entries)

    return entries, input_fields

def setup_dynamic_widgets(root, pags):

    select_frame = ttk.Frame(root)
    select_frame.pack(fill='x', padx=10, pady=10)

    pag_names = list(pags.keys())
    selected_pag = tk.StringVar()
    select_box = ttk.Combobox(select_frame, textvariable=selected_pag, values=pag_names)
    select_box.pack(pady=10)

    # Frame para conter as linhas dinâmicas
    dynamic_frame = ttk.Frame(root)
    dynamic_frame.pack(fill='x', padx=10, pady=10)

    # Contador e lista para manter as linhas adicionadas
    line_counter = [0]
    lines = []

    # Função para adicionar uma nova linha de widgets
    def add_line(selected_pag, pags):
        table_name = selected_pag.get()
        pag = pags[table_name]
    
        row = line_counter[0]
        line_frame = ttk.Frame(dynamic_frame)  # Frame para cada linha
        line_frame.pack(fill='x', padx=10, pady=5)

        # Combobox com opções de exemplo
        combo_var = tk.StringVar()
        combobox = ttk.Combobox(line_frame, textvariable=combo_var, values=pag.columns, state='readonly')
        combobox.pack(side='left', padx=5)
        combobox.current(0)

        # Entry para entrada de texto
        entry = ttk.Entry(line_frame)
        entry.pack(side='left', padx=5)

        # Botão de lixinho para apagar a linha
        def remove_line():
            line_frame.pack_forget()
            line_frame.destroy()
            lines[row] = []

        delete_button = ttk.Button(line_frame, text="🗑️", command=remove_line, width=5)
        delete_button.pack(side='left', padx=5)

        # Adiciona os widgets à lista de controle e incrementa o contador
        lines.append((combo_var, entry, delete_button, line_frame))
        line_counter[0] += 1

    # Botão "Adicionar" para criar novas linhas, posicionado no topo da janela
    add_button = ttk.Button(root, text="Adicionar", command=lambda: add_line(selected_pag, pags))
    add_button.pack(pady=10)

    def show_dataframe_in_new_window(df):
        # Cria uma nova janela
        new_window = tk.Toplevel(root)
        new_window.title("Resultados da Pesquisa")

        # Cria um frame para o Treeview na nova janela
        tree_frame = ttk.Frame(new_window)
        tree_frame.pack(fill='both', expand=True)

        # Cria e configura o Treeview
        tree_view = ttk.Treeview(tree_frame, columns=list(df.columns), show="headings")
        for col in df.columns:
            tree_view.heading(col, text=col)
            tree_view.column(col, width=100, anchor='center')
        tree_view.pack(fill='both', expand=True)

        # Preenche o Treeview com os dados do DataFrame
        for index, row in df.iterrows():
            tree_view.insert("", tk.END, values=list(row))

    # Função para coletar informações de todas as linhas
    def collect_info(table_name):
        collected_info = {}
        for combo_var, entry, _, _ in lines:
            if entry.winfo_exists():  # Verifica se o widget ainda existe
                collected_info[combo_var.get()] = entry.get()
        # Supondo que este é o método para obter o DataFrame baseado nas informações coletadas
        query = database_operations.search_query(collected_info, table_name)
        df = database_operations.query_to_dataframe(query)

        # Exibe o DataFrame em uma nova janela
        show_dataframe_in_new_window(df)

    # Botão "Coletar Informações" para capturar os dados inseridos
    collect_button = ttk.Button(root, text="Coletar Informações", command= lambda:collect_info(selected_pag.get()))
    collect_button.pack(pady=10)




