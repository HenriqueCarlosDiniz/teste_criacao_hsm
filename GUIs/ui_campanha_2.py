import tkinter as tk
from tkinter import ttk
from GUIs.database_operations import query_to_dataframe, get_table_columns, get_next_id
from GUIs.pagina import Pagina
from GUIs.ui_utils import input_fields_on_tab, fetch_data
import sys
sys.path.append('../novo_proj')
from GUIs.config import osFunc

SAME_FIELDS = {"ID_CAMPANHA":["ID_CAMPANHA", 'id_campanha']}
DIFF_TYPES = {'string_contexto':'text'}
DEFAULT_FIELDS = {'CONTEXTO':{'nome_contexto':'ATENDENTE', 'id_user_contexto':0, 'ID_string_contexto':get_next_id('STRING_CONTEXTO','ID_string_contexto')}}
PAGS_CAMPANHA = ["CONTEXTO", "STRING_CONTEXTO"]
PAGS_TO_COPY = ['REPESCAGEM', "CONTEXTO_EXTRA", "DISPARO"]
# DIFF_BUT_SAME_NAME = {"ID_STRING":[("USER_STRING","id_user_contexto", "CONTEXTO"),("STRING_CONTEXTO","ID_string_contexto", "CONTEXTO")]}

def get_status_page(nome_campanha):
    status = 'Complete'
    ATIVO_CHECK = ['ATENDENTE', 'CHECAR_ENCERRAMENTO', 'EXTRACT_DATA', 
                   'CLASSIFICADOR_REPESCAGEM', 'CLASSIFICADOR_FAQ', 
                   'GET_RESPOSTA_FAQ', 'AUX_FAQ', 'FUNCTION_FNU', 
                   'FUNCTION_ATU', 'CLASSIFICADOR_AGENDAMENTO_MULTIPLO', 
                   'CLASSIFICADOR_INTERVENCAO']
    id_campanha = osFunc(nome_campanha)
    print("CAMPAING:", nome_campanha, osFunc(nome_campanha))
    query = f"SELECT nome_contexto FROM CONTEXTO where ID_CAMPANHA = {id_campanha}"
    df = query_to_dataframe(query)
    if df.empty:
        return "Status: Inexistente"
    elif df is not None:
        set_nome_contexto = set(df['nome_contexto'].to_list())
        missing = list(set(ATIVO_CHECK) - set_nome_contexto)
        if len(missing)>0:
            if len(list(set(missing) - set(ATIVO_CHECK))) == 0:
                print("funciona")
                return "Status: Inexistente"
            status = f'Missing: {missing[0]}'
            for i in range(1,len(missing)):
                status += f', {missing[i]}'

    return f"Status: {status}"

def setup_ui_campanha(root, pags):
    # Busca os dados da tabela CAMPANHA
    pag = Pagina('CAMPANHA')
    queryString = "SELECT ID_CAMPANHA, NOME_CAMPANHA FROM CAMPANHA"
    df = query_to_dataframe(queryString)
    pags = {"CAMPANHA": pag, **pags}
    del pags["USER_STRING"]
    
    if df is not None:
        for index, row in df.iterrows():
            # Cria um container para cada campanha
            campaign_frame = ttk.Frame(root)
            campaign_frame.pack(fill='x', expand=True)
            
            # Cria e exibe o nome da campanha
            campaign_name = ttk.Label(campaign_frame, text=row['NOME_CAMPANHA'])
            campaign_name.pack(side='left', padx=5)
            
            # Cria o botão de editar
            edit_button = ttk.Button(campaign_frame, text='Editar', command=lambda id=row['ID_CAMPANHA']: edit_campaign(id))
            edit_button.pack(side='right', padx=5)

    else:
        print("Falha ao buscar dados da campanha")
    
    # Adicionar botão para criar nova campanha no final
    add_campaign_button = ttk.Button(root, text="Criar Nova Campanha", command=lambda: create_new_campaign(pag, pags))
    add_campaign_button.pack(pady=10)

def create_new_campaign(pag, pags):
    input_values = {}  # Dicionário para armazenar valores de entrada compartilhados

    new_campaign_window = tk.Toplevel()
    new_campaign_window.title("Nova Campanha")
    new_campaign_window.geometry("800x600")

    notebook = ttk.Notebook(new_campaign_window)
    notebook.pack(expand=True, fill='both')

    def on_field_change(key, value):
        """Atualiza o dicionário compartilhado e sincroniza os campos de entrada."""
        input_values[key] = value
        # Aqui você pode adicionar lógica para atualizar os campos em todas as páginas, se necessário

    def on_tab_changed(event):
        """Atualiza os campos de entrada com os valores compartilhados quando uma nova aba é selecionada."""
        current_tab = notebook.nametowidget(notebook.select())
        for field in current_tab.input_fields:
            if field in input_values:
                current_tab.input_fields[field].set(input_values[field])

    for pag in pags.values():
        frame = ttk.Frame(notebook)
        frame.input_fields = {}  # Armazena os campos de entrada para esta página
        notebook.add(frame, text=pag.name)
        setup_tab_with_search_fields(frame, pag, on_field_change)

    notebook.bind("<<NotebookTabChanged>>", on_tab_changed)

    save_button = ttk.Button(new_campaign_window, text="Salvar", command=lambda: save_campaign(frame.input_fields))
    save_button.pack(pady=10)

def setup_tab_with_search_fields(tab, pag, on_field_change):
    input_frame = ttk.Frame(tab)
    input_frame.pack(padx=10, pady=10, expand=True, fill='both')

    # Verifica se existem valores padrão para a página atual
    default_values = DEFAULT_FIELDS.get(pag.name, {})

    for field_name in pag.columns:
        canonical_field_name = get_canonical_field_name(field_name, pag.name)

        field_frame = ttk.Frame(input_frame)
        field_frame.pack(fill='x', pady=5)

        label = ttk.Label(field_frame, text=canonical_field_name)
        label.pack(side=tk.LEFT, padx=5)

        if canonical_field_name not in tab.input_fields:
            entry_var = tk.StringVar()
            # Define o valor padrão do campo, se aplicável
            default_value = default_values.get(canonical_field_name)
            if default_value is not None:
                entry_var.set(default_value)
            tab.input_fields[canonical_field_name] = entry_var
        else:
            entry_var = tab.input_fields[canonical_field_name]

        entry = ttk.Entry(field_frame, textvariable=entry_var)
        entry.pack(side=tk.LEFT, expand=True, fill='x', padx=5)

        entry_var.trace_add("write", lambda name, index, mode, var=entry_var, key=canonical_field_name: on_field_change(key, var.get()))

def save_campaign(input_fields):
    print("Salvando nova campanha:", input_fields)
    for i in input_fields:
        print(input_fields[i].get())

def edit_campaign(campaign_id):
    # Função chamada quando o botão Editar é pressionado
    print(f"Editar campanha {campaign_id}")

def get_canonical_field_name(field_name, pag_name):
    # Primeiro verifica SAME_FIELDS para mapeamentos diretos
    for canonical_name, synonyms in SAME_FIELDS.items():
        if field_name in synonyms:
            return canonical_name

    # # Segundo, verifica DIFF_BUT_SAME_NAME para mapeamentos condicionais
    # if field_name in DIFF_BUT_SAME_NAME:
    #     for mapping in DIFF_BUT_SAME_NAME[field_name]:
    #         if pag_name == mapping[0]:  # Se o nome da página corresponde
    #             return mapping[1]  # Retorna o nome canônico para esse contexto

    return field_name  # Retorna o nome original se não houver mapeamentos
