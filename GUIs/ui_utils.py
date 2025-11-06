import tkinter as tk
from tkinter import ttk, messagebox
import GUIs.database_operations as database_operations

def input_fields_on_tab(pag, input_frame, input_fields):
    for widget in input_frame.winfo_children():
        widget.destroy()
    
    fields = pag.columns

    for field in fields:
        label = ttk.Label(input_frame, text=field)
        label.pack(anchor='w')

        if field in ["string_contexto", "STRING", "CONTEUDO"]:
            # Criação do campo de texto com scrollbar para STRING_CONTEXTO
            text_widget = tk.Text(input_frame, height=20, width=100)
            scrollbar = ttk.Scrollbar(input_frame, command=text_widget.yview)
            text_widget.config(yscrollcommand=scrollbar.set)
            text_widget.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            input_fields[field] = text_widget

        else:
            # Criação de um campo de entrada padrão para outros campos
            entry = ttk.Entry(input_frame, width=50)
            entry.pack(anchor='w')
            input_fields[field] = entry


def update_input_fields(pag, input_frame, input_fields, data):
    for widget in input_frame.winfo_children():
        widget.destroy()
    
    fields = pag.columns
    
    for field in fields:
        label = ttk.Label(input_frame, text=field)
        label.pack(anchor='w')

        if field in ['string_contexto',"STRING", "CONTEUDO"]:
            # Criação do campo de texto com scrollbar para STRING_CONTEXTO
            text_widget = tk.Text(input_frame, height=20, width=100)
            scrollbar = ttk.Scrollbar(input_frame, command=text_widget.yview)
            text_widget.config(yscrollcommand=scrollbar.set)
            text_widget.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            input_fields[field] = text_widget

        else:
            # Criação de um campo de entrada padrão para outros campos
            entry = ttk.Entry(input_frame, width=50)
            entry.pack(anchor='w')
            input_fields[field] = entry

    for field, widget in input_fields.items():
        try:
            if field in data:
                value = data[field]
                if value is not None:
                    if isinstance(widget, tk.Text):
                        widget.delete("1.0", tk.END)
                        widget.insert("1.0", value)
                    else:
                        widget.delete(0, tk.END)
                        widget.insert(0, value)
        except:
            print("######### ERROR:", value, widget, data[field])

def fetch_data(pag, input_frame = None, input_fields = None, entries = None): 
    entries_val = {}
    for i in entries.keys():
        entries_val.update({i:entries[i].get()})
    data_df = database_operations.fetch_data(pag, input_fields=input_fields, entries = entries_val)

    # Após buscar os dados, atualize os campos de entrada
    if not data_df.empty:
        update_input_fields(pag, input_frame, input_fields, data_df.iloc[0])
    else:
        messagebox.showinfo("Resultado", "Nenhum dado encontrado.")

def update_data(pag, input_fields = None, entries = None):
    message = database_operations.update_data(pag, input_fields= input_fields, entries = entries)
    messagebox.showinfo("Resultado", message)

def insert_data(table_name, input_fields):
    data = {field: widget.get("1.0", "end-1c") if isinstance(widget, tk.Text) else widget.get() 
            for field, widget in input_fields.items()}
    message = database_operations.insert_data_into_db_gui(data, table_name)
    messagebox.showinfo("Resultado", message)

# def get_status_page(nome_campanha):
#     status = 'Complete'
#     ATIVO_CHECK = ['ATENDENTE', 'CHECAR_ENCERRAMENTO', 'EXTRACT_DATA', 
#                    'CLASSIFICADOR_REPESCAGEM', 'CLASSIFICADOR_FAQ', 
#                    'GET_RESPOSTA_FAQ', 'AUX_FAQ', 'FUNCTION_FNU', 
#                    'FUNCTION_ATU', 'CLASSIFICADOR_AGENDAMENTO_MULTIPLO', 
#                    'CLASSIFICADOR_INTERVENCAO']
#     id_campanha = osFunc(nome_campanha)
#     print("CAMPAING:", nome_campanha, osFunc(nome_campanha))
#     query = f"SELECT nome_contexto FROM CONTEXTO where ID_CAMPANHA = {id_campanha}"
#     df = query_to_dataframe(query)
#     if df.empty:
#         return "Status: Inexistente"
#     elif df is not None:
#         set_nome_contexto = set(df['nome_contexto'].to_list())
#         missing = list(set(ATIVO_CHECK) - set_nome_contexto)
#         if len(missing)>0:
#             if len(list(set(missing) - set(ATIVO_CHECK))) == 0:
#                 print("funciona")
#                 return "Status: Inexistente"
#             status = f'Missing: {missing[0]}'
#             for i in range(1,len(missing)):
#                 status += f', {missing[i]}'

#     return f"Status: {status}"