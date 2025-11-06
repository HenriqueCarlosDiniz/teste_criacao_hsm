import tkinter as tk
from tkinter import ttk
from GUIs.database_operations import query_to_dataframe, get_table_columns, get_next_id, get_campaign_data, save_campaign, make_copy
from GUIs.pagina import Pagina
from GUIs.ui_utils import input_fields_on_tab, fetch_data
import sys
sys.path.append('../novo_proj')
from GUIs.config import osFunc

SAME_FIELDS = {"ID_CAMPANHA":["ID_CAMPANHA", 'id_campanha']}
DIFF_TYPES = {'string_contexto':'text'}
DEFAULT_FIELDS = {'CONTEXTO':{'nome_contexto':'ATENDENTE', 'id_user_contexto':0, 'ID_string_contexto':get_next_id('STRING_CONTEXTO','ID_string_contexto')}}
PAGS_CAMPANHA = ["CONTEXTO", "STRING_CONTEXTO"]
PAGS_TO_COPY = ['REPESCAGEM', "DISPARO"]
CONTEXT_EXTRAS = ['CHECAR_ENCERRAMENTO', 'EXTRACT_DATA', 
                'CLASSIFICADOR_REPESCAGEM', 'CLASSIFICADOR_FAQ', 
                'GET_RESPOSTA_FAQ', 'AUX_FAQ', 'FUNCTION_FNU', 
                'FUNCTION_ATU', 'CLASSIFICADOR_AGENDAMENTO_MULTIPLO', 
                'CLASSIFICADOR_INTERVENCAO','CADASTRO_RECEPTIVO']

# Supondo que query_to_dataframe("SELECT NOME_CAMPANHA FROM CAMPANHA") retorna um DataFrame
CAMPANHAS = query_to_dataframe("SELECT NOME_CAMPANHA FROM CAMPANHA")['NOME_CAMPANHA'].to_list()

# DIFF_BUT_SAME_NAME = {"ID_STRING":[("USER_STRING","id_user_contexto", "CONTEXTO"),("STRING_CONTEXTO","ID_string_contexto", "CONTEXTO")]}

def setup_ui_campanha(root, pags):
    # Busca os dados da tabela CAMPANHA
    pag = Pagina('CAMPANHA', ['ID_CAMPANHA'])
    queryString = "SELECT ID_CAMPANHA, NOME_CAMPANHA FROM CAMPANHA"
    df = query_to_dataframe(queryString)
    pags = {name: pags[name] for name in PAGS_CAMPANHA}
    pags = {"CAMPANHA": pag, **pags}
    
    if df is not None:
        for index, row in df.iterrows():
            # Cria um container para cada campanha
            campaign_frame = ttk.Frame(root)
            campaign_frame.pack(fill='x', expand=True)
            
            # Cria e exibe o nome da campanha
            campaign_name = ttk.Label(campaign_frame, text=row['NOME_CAMPANHA'])
            campaign_name.pack(side='left', padx=5)
            
            # Cria o botão de editar
            edit_button = ttk.Button(campaign_frame, text='Editar', command=lambda id=row['ID_CAMPANHA']: edit_campaign(id, pags))
            edit_button.pack(side='right', padx=5)

    else:
        print("Falha ao buscar dados da campanha")
    
    # Adicionar botão para criar nova campanha no final
    add_campaign_button = ttk.Button(root, text="Criar Nova Campanha", command=lambda: create_new_campaign(pag, pags, get_next_id("CAMPANHA", "ID_CAMPANHA")))
    add_campaign_button.pack(pady=10)

def create_new_campaign(pag, pags, id_new_campaing):
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

    frames = {}
    for pag in pags.values():
        frame = ttk.Frame(notebook)
        frame.input_fields = {}  # Armazena os campos de entrada para esta página
        notebook.add(frame, text=pag.name)
        setup_tab_with_search_fields(frame, pag, on_field_change, last_id_campaing=id_new_campaing)
        frames[pag.name] = (frame)
    notebook.bind("<<NotebookTabChanged>>", on_tab_changed)

    save_button = ttk.Button(new_campaign_window, text="Salvar", command=lambda: save_campaign(frames, pags))
    save_button.pack(pady=10)

def setup_tab_with_search_fields(tab, pag, on_field_change, existing_data = None, last_id_campaing = None):
    input_frame = ttk.Frame(tab)
    input_frame.pack(padx=10, pady=10, expand=True, fill='both')

    default_values = DEFAULT_FIELDS.get(pag.name, {})

    if last_id_campaing:
        print("ENTROU")
        default_values = {"ID_CAMPANHA": last_id_campaing, **default_values}

    for field_name in pag.columns:
        canonical_field_name = get_canonical_field_name(field_name, pag.name)

        field_frame = ttk.Frame(input_frame)
        field_frame.pack(fill='x', pady=5)

        label = ttk.Label(field_frame, text=canonical_field_name)
        label.pack(side=tk.LEFT, padx=5)

        # Tratamento especial para o campo 'string_contexto'
        if canonical_field_name == 'string_contexto':
            entry_var = tk.StringVar()
            text_widget = tk.Text(input_frame, height=20, width=100)
            scrollbar = ttk.Scrollbar(input_frame, command=text_widget.yview)
            text_widget.config(yscrollcommand=scrollbar.set)
            text_widget.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            if existing_data and canonical_field_name in existing_data:
                text_widget.insert("1.0", existing_data[canonical_field_name])
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            entry_var.set(text_widget.get("1.0", "end-1c"))
            tab.input_fields[canonical_field_name] = text_widget
        else:
            if canonical_field_name not in tab.input_fields:
                entry_var = tk.StringVar()
                default_value = default_values.get(canonical_field_name)
                if default_value is not None:
                    entry_var.set(default_value)
                tab.input_fields[canonical_field_name] = entry_var
            else:
                entry_var = tab.input_fields[canonical_field_name]

            entry = ttk.Entry(field_frame, textvariable=entry_var)
            entry.pack(side=tk.LEFT, expand=True, fill='x', padx=5)
            entry_var.trace_add("write", lambda name, index, mode, var=entry_var, key=canonical_field_name: on_field_change(key, var.get()))
        if existing_data and canonical_field_name in existing_data:
            entry_var.set(existing_data[canonical_field_name])


def edit_campaign(campaign_id, pags):
    campaign_data = get_campaign_data(campaign_id, pags, DEFAULT_FIELDS)  # Busca os dados da campanha
    existing_data = {field: value for field, value in campaign_data.items()}
    edit_window = tk.Toplevel()
    edit_window.title("Editar Campanha")
    edit_window.geometry("800x600")

    notebook = ttk.Notebook(edit_window)
    notebook.pack(expand=True, fill='both')

    input_values = {}  # Dicionário para armazenar valores de entrada compartilhados

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

    frames = {}
    for pag in pags.values():
        frame = ttk.Frame(notebook)
        frame.input_fields = {}  # Armazena os campos de entrada para esta página
        notebook.add(frame, text=pag.name)
        setup_tab_with_search_fields(frame, pag, on_field_change, existing_data)
        frames[pag.name] = (frame)
    
    frame_copy = ttk.Frame(notebook)
    frame_copy.input_fields = {}
    notebook.add(frame_copy, text="EXTRAS")
    setup_pag_copy_info(frame_copy, campaign_id)

    notebook.bind("<<NotebookTabChanged>>", on_tab_changed)

    save_button = ttk.Button(edit_window, text="Salvar", command=lambda: save_campaign(frames, pags))
    save_button.pack(pady=10)

def get_canonical_field_name(field_name, pag_name):
    # Primeiro verifica SAME_FIELDS para mapeamentos diretos
    for canonical_name, synonyms in SAME_FIELDS.items():
        if field_name in synonyms:
            return canonical_name

    return field_name  # Retorna o nome original se não houver mapeamentos

def setup_pag_copy_info(tab, id_campanha):

    # Dicionários para armazenar as variáveis de controle dos Checkbuttons
    selection_vars_pags = {item: tk.BooleanVar() for item in PAGS_TO_COPY}
    selection_vars_context = {item: tk.BooleanVar() for item in CONTEXT_EXTRAS}
    combo_var = tk.StringVar()
    options = CAMPANHAS
    combobox = ttk.Combobox(tab, textvariable=combo_var, values=options, state='readonly') 

    # Função para criar os Checkbuttons e adicioná-los ao tab
    def create_checkbuttons(options, vars_dict, start_row):
        for i, item in enumerate(options, start_row):
            check = ttk.Checkbutton(tab, text=item, variable=vars_dict[item])
            check.grid(column=0, row=i, sticky=tk.W, padx=10, pady=2)

    combobox.grid(column=0, row=0, padx=10, pady=10)
    combobox.current(0)
    # Criação dos Checkbuttons para cada lista
    create_checkbuttons(PAGS_TO_COPY, selection_vars_pags, 1)
    create_checkbuttons(CONTEXT_EXTRAS, selection_vars_context, len(PAGS_TO_COPY)+1)

    # Função para ler os estados dos Checkbuttons e imprimir os resultados
    def read_selections(id_campanha):
        results_pags = {}
        results_context = {}
        for key, var in selection_vars_pags.items():
            results_pags[key] = var.get()
        for key, var in selection_vars_context.items():
            results_context[key] = var.get()
        nome_campanha = combo_var.get()
        id_campanha_source = osFunc(nome_campanha)
        make_copy(results_pags, results_context, id_campanha_source, id_campanha)
        results = {**results_pags, **results_context}
        return results, nome_campanha  # Retorna o dicionário com os resultados

    # Botão para ler a seleção dos Checkbuttons
    read_button = ttk.Button(tab, text="Ler Seleções", command= lambda:read_selections(id_campanha))
    read_button.grid(column=0, row=len(PAGS_TO_COPY) + len(CONTEXT_EXTRAS) + 1, padx=10, pady=10)

    



