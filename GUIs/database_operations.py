import tkinter as tk
import pandas as pd
import sys
sys.path.append('../novo_proj')
from GUIs.connect_db import query_to_dataframe, execute_query_db, insert_data_into_db, get_table_columns

def insert_data_into_db_gui(data_dict, table_name):
    try:
        print("\n INSERT:", table_name, "\n", data_dict)
        data_df = pd.DataFrame([data_dict])
        sucesso = insert_data_into_db(data_df, table_name)
        if sucesso:
            return "Dados inseridos com sucesso!"
        else: 
            return f"Erro ao inserir dados"
    except Exception as e:
        return f"Erro ao inserir dados: {e}"
    
def build_sql_where_query(search_keys, entries):
    # Começa a construir a string da consulta
    query = f" WHERE "
    conditions = []
    for key in search_keys:
        condition = f"{key} = '{entries[key]}'"
        conditions.append(condition)
    query += " AND ".join(conditions)
    query += " LIMIT 1"
    return query

def query_to_dataframe_dbo(query):
    return query_to_dataframe(query)

def fetch_data(pag, id_value = None, input_fields = None, nome_contexto = None, id_campanha = None, id_mensagem = None, entries = None):
    table_name = pag.name
    search_keys = pag.keys
    query = f"SELECT * FROM {table_name}"
    query += build_sql_where_query(search_keys, entries)
    print(query)
    data_df = query_to_dataframe(query)
    if data_df is not None and not data_df.empty:
        data = data_df.iloc[0].to_dict()
        for field, value in data.items():
            print(value)
            try:
                print("INPUTFIELD:",input_fields[field], value)
                if isinstance(input_fields[field], tk.Text):
                    print("ENTROU")
                    input_fields[field].delete("1.0", tk.END)
                    input_fields[field].insert("1.0", value)
                else:
                    print("NOT_TEXT:", value)
                    input_fields[field].delete(0, tk.END)
                    input_fields[field].insert(0, value)
            except Exception as e:
                print("######################### \n ERROR:", input_fields[field],  "DATA:", data,  value)

    return data_df

def update_data(pag, input_fields = None, entries = None):
    data = {}
    table_name = pag.name
    entries_val = {}
    for i in entries.keys():
        entries_val.update({i:entries[i].get()})
    for field, widget in input_fields.items():
        if isinstance(widget, tk.Text):
            data[field] = widget.get("1.0", "end-1c")
        else:
            data[field] = widget.get()
    set_clause = ', '.join([f'''{field} = "{data[field]}"''' for field in data if field not in pag.keys])
    
    update_query = f"UPDATE {table_name} SET {set_clause} " + build_sql_where_query(pag.keys, entries_val)
    print("UPDATE QUERY",update_query)
    try:
        execute_query_db(update_query)
        return "Dados atualizados com sucesso!"
    except Exception as e:
        return f"Erro ao atualizar dados: {e}"


def get_next_id(table_name, id_name):
    # Modifique a string de consulta para usar o nome da coluna do ID fornecido
    query_string = f"SELECT MAX({id_name}) AS max_id FROM {table_name}"
    df = query_to_dataframe(query_string)
    # print(df)

    # Verifique se o DataFrame não está vazio e tem o resultado esperado
    if df is not None and not df.empty:
        # Verifique se o valor retornado é None (pode acontecer se a tabela estiver vazia)
        if pd.isnull(df['max_id'].iloc[0]):
            next_id = 1  # Se a tabela estiver vazia, começamos os IDs a partir de 1
        else:
            next_id = df['max_id'].iloc[0] + 1  # Calcule o próximo ID
        return next_id
    else:
        print(f"Não foi possível recuperar o máximo ID atual da tabela {table_name}.")
        return "ERROR"
    
def get_campaign_data(id_campanha, pags, default_values = {}):
    missing_info = list(pags.keys())
    info_known = {"id_campanha": id_campanha, "ID_CAMPANHA": id_campanha}
    for key in default_values:
        info_for_key = default_values[key]
        info_known = {**info_known, **info_for_key}
    for index, pag_name in enumerate(missing_info):
        print("PAG",pag_name)
        pag = pags[pag_name]
        if all([item in list(info_known.keys()) for item in pag.keys]):
            query = f"SELECT * FROM {pag_name}" + build_sql_where_query(pag.keys, info_known)
            df = query_to_dataframe(query)
            if df is not None and not df.empty:
                dict_records = df.to_dict(orient='records')[0]
                print(dict_records)
                info_known = {**info_known, **dict_records}
            # pag.pop(index)
    print("DIC:", info_known)
    return info_known

def data_exists(table_name, check_fields):
    """
    Verifica se os dados já existem no banco de dados com base em campos específicos.
    Retorna True se existir, False caso contrário.
    """
    where_clause = " AND ".join([f"{field} = '{value.get()}'" for field, value in check_fields.items()])
    query = f"SELECT IF(EXISTS(SELECT 1 FROM {table_name} WHERE {where_clause}), 1, 0) AS exists_flag;"
    # query = f"SELECT EXISTS(SELECT 1 FROM {table_name} WHERE {where_clause}, 1, 0) AS exists;"
    print(f"\n {query} \n")
    df = query_to_dataframe(query)
    if df is not None and not df.empty and df.iloc[0]['exists_flag']:
        return True
    return False

def save_campaign(frames, pags):
    # del pags["CAMPANHA"]
    input_fields = {}
    for frame in frames.values():
        input_fields = {**input_fields, **frame.input_fields}
    # Cria um dicionário apenas com os campos chave e seus valores para verificar a existência dos dados
    for pag in pags.values():
        print("PAG:",pag.name, pag.keys)
        check_fields = {}
        for key in pag.keys:
            if key == 'id_campanha':
                input_fields[key] = input_fields["ID_CAMPANHA"]
            check_fields[key] = input_fields[key]
        if data_exists(pag.name, check_fields):
            # Atualiza os dados existentes
            input_fields_pag = {key : input_fields[key] for key in pag.columns}
            result_message = update_data(pag, input_fields=input_fields_pag, entries=check_fields)
        else:
            # Insere novos dados
            input_fields_pag = {key : input_fields[key] for key in pag.columns}
            data_dict = {}
            for field, widget in input_fields_pag.items():
                if isinstance(widget, tk.Text):
                    data_dict[field] = widget.get("1.0", "end-1c")
                else:
                    data_dict[field] = widget.get()
            result_message = insert_data_into_db_gui(data_dict=data_dict, table_name=pag.name)
        
        print(result_message)

def make_copy(results_pags, results_context, id_campanha_source, id_campanha_target):
    for key, val in results_pags.items():
        if val:
            columns = get_table_columns(key)
            # print(columns)
            if "ID_CAMPANHA" in columns:
                columns.remove("ID_CAMPANHA")
            if "ID_DISPARO" in columns:
                columns.remove("ID_DISPARO")
            if "LIMIT" in columns:
                columns.remove("LIMIT")
                columns.append("`LIMIT`")
            str_columns = ", ".join(columns)
            query = f"""INSERT INTO {key} ({str_columns}, ID_CAMPANHA)
            SELECT {str_columns}, {id_campanha_target} as ID_CAMPANHA
            FROM {key}
            WHERE ID_CAMPANHA = {id_campanha_source};"""
            print(f"\n QUERY: {query}\n")
            execute_query_db(query)

    columns = get_table_columns("CONTEXTO")
    print(f"\n COLUMNS: {columns}")
    if "id_campanha" in columns:
            columns.remove("id_campanha")
    for key, val in results_context.items():
        if val:
            str_columns = ", ".join(columns)
            query = f"""INSERT INTO CONTEXTO ({str_columns}, id_campanha)
            SELECT {str_columns}, {id_campanha_target} as id_campanha
            FROM CONTEXTO
            WHERE id_campanha = {id_campanha_source} AND nome_contexto = \'{key}\';"""
            print(f"\n QUERY: {query}\n")
            execute_query_db(query)

def search_query(dic_search, table_name):
    query = f"SELECT * FROM {table_name} WHERE "
    conditions = []
    for key, val in dic_search.items():
        condition = f"{key} = \'{val}\'"
        conditions.append(condition)
    query += " AND ".join(conditions)
    print(query)
    return query
    