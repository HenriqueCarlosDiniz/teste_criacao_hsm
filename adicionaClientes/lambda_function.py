from utils.config import * 

import json
import pytz
from datetime import datetime
import random
import time
import requests
import re

from utils.connect_db import query_to_dataframe, execute_query_db, DatabaseConnection
from utils.config import osFunc
from Models.contato import *
from Models.agente import *
from Models.conversa import *
from utils.text import remover_acentos
from utils.api_communication import GET_unidades_ativas

if osFunc('Ambiente_Lambda'):
    from invoke import invokeFunction
else: 
    from adicionaClientesBatch.lambda_function import lambda_handler as adicionaClientesBatch

def get_gender(telefone, nome, campanha, df_nomes):

    try:
        nome = nome.split()[0].upper()
        
        genero = df_nomes[df_nomes['NOME'] == remover_acentos(nome)]['GENERO'].values
        if genero:
            genero = genero[0]     
            
        if not genero:
            # contexto = """
            # Será passado um nome e sobrenome para você, quero que você classifique o gênero do nome: Masculino ou Feminino ou Indefinido. O output deve ser uma das seguintes opções exclusivamente: 'M' ou 'F' ou 'I'
            # Retorne um arquivo json no formato {'label': 'F'} ou {'label': 'M'} ou {'label': 'I'}
            # """
            # contato = Contato(telefone, nome_cliente = nome, buscar_agendamento = False)
            # agente = Agent(contexto, model="gpt-3.5-turbo-1106", json_mode = True)
            # resp = agente.get_response_simples([{"role": "system", "content": f"{contexto}"}, {"role": "user", "content": f"{nome}"}], conversa = Conversa(contato, campanha))
            # genero = resp.choices[0].message.content
            # genero = json.loads(genero)
            # genero = genero['label']
            genero = 'null' 
    except:
        genero = 'null'

    return genero


def get_all_units(http_session):
    data_all = GET_unidades_ativas(http_session)
    data_all = json.loads(data_all.text)
    retorno = []
    for value in data_all:
        retorno.append(value['grupoFranquia'])
    return retorno
 

def generate_random_array(values, length):
    n = len(values)
    # Repeat the values to fill n elements
    repeated_values = values * (length//n)

    random.shuffle(repeated_values)
    
    # If n is not a multiple of the length of values, add the remaining values
    remaining_values = values[:length % n]
    random_array = repeated_values + remaining_values

    return random_array

def processa_telefone(telefone):
    telefone_str = str(telefone)
    telefone_str = "55" + telefone_str
    if len(telefone_str) == 12: 
        telefone_modificado = telefone_str[:4] + '9' + telefone_str[4:]
        telefone = int(telefone_modificado)
    else:            
        telefone = int(telefone_str)
    return telefone

def processa_nome(name):
    name = name.replace("", "ã")
    name = name.replace("'", " ")
    name = re.sub('[^\w\s]', '', name)
    words = name.split()

    processed_words = [] 
    for word in words:
        if word.isupper():
            processed_words.append(word[0] + word[1:].lower())
        else:
            processed_words.append(word)

    processed_name = ' '.join(processed_words)

    return processed_name

    
def cria_dicionario():
    http_session = requests.Session()
    unidades = get_all_units(http_session)
    unidades_correcao = {unidade[-3:]:unidade for unidade in unidades}
    return unidades_correcao

def lambda_handler(event, context): 
    correcao_unidades = cria_dicionario() 
    ############################################################################
    ########################## EXTRAÇÃO DOS DADOS ##############################
    ############################################################################
    start_time = time.time()

    # Recuperando data atual (para identificação dos clientes adicionados nessa execução)
    current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')) 
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

    # Resgatando os números de wpp disponíveis
    df_telefone_wpp = query_to_dataframe('SELECT ID_CAMPANHA, BANCO_DADOS, TELEFONE_WPP FROM DISPARO')

    # Seleciona os contatos que ainda não foram utilizados 
    df = query_to_dataframe('select * from tb_log_envio_contatos where utilizado = 0')
    df_nomes = query_to_dataframe(f"SELECT * FROM NOME_GENERO")  
    for _, row in df.iterrows():
        dic_df = row.to_dict()
        lembrete_bool = False
        
        if isinstance(json.loads(dic_df['contatos']), dict):
            dic_contatos = json.loads(dic_df['contatos'])
            print('dict')
        elif isinstance(json.loads(dic_df['contatos']), list):
            print('list')
            dic_contatos = {str(index): data for index, data in enumerate(json.loads(dic_df['contatos']))}
        else:
            print('ERROR: não veio lista nem dicionário')
            continue
        
        print('dic_contatos: ', dic_contatos)

        # Corrigindo o nome da campanha
        campanha = dic_df['campanha']
        banco_de_dados = 0
        preco = 0
        ativo = 0
        id_remarketing = None

        if campanha == 'no_show':
            id_campanha = osFunc("NOSHOW")
        elif campanha == 'no_show_reciclado':
            id_campanha = osFunc("NOSHOW")
            banco_de_dados = 1
        elif campanha == 'cancelados':
            id_campanha = osFunc("CANCELADOS")
        elif campanha == 'cancelados_reciclado':
            id_campanha = osFunc("CANCELADOS")
            banco_de_dados = 1
        elif campanha == 'confirmacao_2h':
            id_campanha = osFunc("CONFIRMACAO_2H")
            lembrete_bool = True
            id_flag = osFunc("ENVIAR_HSM")
        elif campanha == 'confirmacao':
            id_campanha = osFunc("CONFIRMACAO")
        elif campanha == 'confirmacao_1h':
            id_campanha = osFunc("CONFIRMACAO_2H")
            id_flag = osFunc('LEMBRETE_1H')
            lembrete_bool = True
        elif campanha == 'indique_amigos':
            id_campanha = osFunc("INDIQUE_AMIGOS")
        elif campanha == 'leads_whatsapp':
            id_campanha = osFunc("LEADS_WHATSAPP")
        elif campanha == 'leads_meta':
            id_campanha = osFunc("LEADS_META")
        ######## REMARKETING #################
        
        elif campanha == 'remarketing_2h':
            id_campanha = osFunc("REMARKETING") 
            id_remarketing = 0
        elif campanha == 'remarketing_7d':
            id_campanha = osFunc("REMARKETING") 
            id_remarketing = 1
        elif campanha == 'remarketing_15d':
            id_campanha = osFunc("REMARKETING") 
            id_remarketing = 2
        elif campanha == 'remarketing_23d':
            id_campanha = osFunc("REMARKETING") 
            id_remarketing = 3
        elif campanha == 'remarketing_31d':
            id_campanha = osFunc("REMARKETING") 
            id_remarketing = 4
        elif campanha == 'remarketing_38d':
            id_campanha = osFunc("REMARKETING") 
            id_remarketing = 5
        print("CAMPANHA: ", campanha ," , ID_CAMPANHA: ", id_campanha)

        # Definindo resultado e flag após adição

        id_resultado = osFunc("ADICIONADO")
        if not lembrete_bool:
            id_flag = osFunc("ENVIAR_HSM")

        ############################################################################
        ########################## RANDOMIZAÇÃO DO HSM #############################
        ############################################################################ 
        if id_campanha == osFunc('REMARKETING'):
            df_hsm  = query_to_dataframe(f'SELECT ID_HSM, VETOR_ID_HSM_SECUNDARIO FROM HSM WHERE ATIVO = 1 AND STATUS = "approved" AND SECUNDARIO = 0 AND ID_CAMPANHA = {id_campanha} AND ID_REMARKETING = {id_remarketing}')
        else:
            df_hsm  = query_to_dataframe(f'SELECT ID_HSM, VETOR_ID_HSM_SECUNDARIO FROM HSM WHERE ATIVO = 1 AND STATUS = "approved" AND SECUNDARIO = 0 AND ID_CAMPANHA = {id_campanha} AND BANCO_DADOS = {banco_de_dados}')
        print(df_hsm)
        hsm_list  = df_hsm['ID_HSM'].tolist()
        random_hsm = generate_random_array(hsm_list, len(dic_contatos.keys()))
         
        random_hsm_secundario = []
        if 'VETOR_ID_HSM_SECUNDARIO' in df_hsm.columns:
            for id_hsm in random_hsm:
                id_hsm_secundario_str = df_hsm[df_hsm['ID_HSM'] == id_hsm]['VETOR_ID_HSM_SECUNDARIO'].iloc[0]
                if id_hsm_secundario_str is not None:
                    id_hsm_secundario_list = id_hsm_secundario_str.split(',')
                else:
                    id_hsm_secundario_list = [None]
                random_id_hsm_secundario = random.choice(id_hsm_secundario_list)
                random_hsm_secundario.append(random_id_hsm_secundario)
        else:
            random_hsm_secundario = [None] * len(random_hsm)

        ############################################################################
        ########################## RESGATANDO O TELEFONE A SER ENVIADO #############
        ############################################################################
        telefone_wpp = df_telefone_wpp[(df_telefone_wpp['ID_CAMPANHA'] == id_campanha) & (df_telefone_wpp['BANCO_DADOS'] == banco_de_dados)]['TELEFONE_WPP'].iloc[0]

        ############################################################################
        #################### ADIÇÃO DOS CLIENTES EM BATCHES#########################
        ############################################################################
        # Definição do número de clientes por batch
        numero_clientes = len(dic_contatos.keys())
        print("Número Clientes: ", numero_clientes)

        active_connections = int(DatabaseConnection.get_active_connections())
        max_connections = int(osFunc('MAX_CONNECTIONS'))
        available_connections = (max_connections - active_connections)
        print("Active_connections: ", active_connections)
        print("Available Connections: ",available_connections)

        batch = numero_clientes//(available_connections*0.5) + 1
        print("Número de clientes por batch: ", batch) 
        print("Batches: ", numero_clientes//batch+1)        

        # Inicialização dos vetores
        telefone_vec = []
        nome_vec = []
        horario_consulta_vec = [] 
        data_consulta_vec = []
        unidade_consulta_vec = [] 
        key_vec = []
        formatted_datetime_vec = []
        id_campanha_vec = []
        id_resultado_vec = []
        banco_de_dados_vec = []
        preco_vec = []
        hsm_vec =  []
        id_flag_vec = []  
        genero_vec = []
        indicado_vec = [] 
        forms_link_vec = []
        hsm_secundario_vec =  []
        id_remarketing_vec = [] ### REMARKETING ###
        nome_especialista_vec = []
        dor_principal_vec = [] 
        token_cancelamento_vec = []
        token_confirmacao_vec = []
        unidade_federativa_vec = []
        cidade_vec = []
        ativo_vec = []
 


        # Iterar sobre todos os clientes
        for index, key in enumerate(dic_contatos.keys()):
            ############################################################################
            ########################## EXTRAÇÃO DOS DADOS ##############################
            ############################################################################
            start_time_cliente = time.time()
            try:
                print('INDEX: ', index, " / ", len(dic_contatos.keys()))
                data = dic_contatos[key]
                print("DATA:", data)
                if id_campanha == osFunc("LEADS_WHATSAPP"):
                    nome = "leads_whatsapp"
                else:
                    nome = data.get("NOME", data.get("NOME_INDICADO", data.get("NOME_CLIENTE", '')))
                    if nome: 
                        nome = nome.strip()
                    else:
                        nome = "leads_whatsapp"
     
                horario_consulta = data.get("HORARIO", '')
                telefone = data.get("TELEFONE", data.get("TELEFONE_INDICADO", ''))
                data_consulta = data.get("DATA_CONSULTA", '')
                unidade_consulta = data.get("UNIDADE_SIGLA", '')
                nome_especialista = data.get("NOME_ESPECIALISTA", ' ').strip()
                dor_principal = data.get("DOR_PRINCIPAL", ' ').strip()
                token_cancelamento = data.get("TOKEN_CANCELAMENTO",'')
                token_confirmacao = data.get("TOKEN_CONFIRMACAO",'')
                unidade_federativa = data.get("ESTADO",'')
                cidade = data.get("CIDADE",'')
                
                if unidade_consulta[-3:] in correcao_unidades.keys():
                    unidade_consulta = correcao_unidades[unidade_consulta[-3:]]
                    
                ################ BLOCKLIST DE UNIDADE (QUANDO SOLICITADO) ########
                # if unidade_consulta in ['FR-POA','FR-MCL']:
                #     print(f"WARNING-{telefone}, {nome}: Pulando pois a unidade é {unidade_consulta}...")
                #     continue   
                
                nome_amigo = data.get("NOME_INDICADOR", '')  # Se data["NOME_AMIGO"] não existir, nome_amigo = None
               
                ############## Lembrete 1H forms ###################################
                if lembrete_bool:
                    id_forms = str(data["QUESTIONARIO"])
                else:
                    id_forms = None
                ############### Tratar Telefone e Nome #############################
                telefone = processa_telefone(telefone)
                
                # ################### BLOCKLIST DE TELEFONE ########################
                if telefone in [5592985566028, 5592992085985, 5592985298288, 5592991791630, 5592985833714, 5592992417540, 5591986041367, 5592992417540, 5592999698925, 5592982454894, 5592992233271, 5512997168883]:
                    print(f"WARNING-{telefone}, {nome}: Pulando pois telefone {telefone} está na blocklist...")
                    continue 
                
                nome = processa_nome(nome)
                nome_amigo = processa_nome(nome_amigo)
                
                if cidade:
                    cidade = processa_nome(cidade)
                if unidade_federativa:
                    unidade_federativa = processa_nome(unidade_federativa)
                
                ############### Recuperar genero ###################################
                genero = get_gender(telefone, nome, campanha, df_nomes)
    
                ############### Recuperar genero #######################
                telefone_vec.append(str(telefone))
                nome_vec.append(str(nome))
                horario_consulta_vec.append(str(horario_consulta))
                data_consulta_vec.append(str(data_consulta))
                unidade_consulta_vec.append(str(unidade_consulta))
                key_vec.append(str(key))
                formatted_datetime_vec.append(str(formatted_datetime))
                id_campanha_vec.append(str(id_campanha))
                banco_de_dados_vec.append(str(banco_de_dados))
                id_resultado_vec.append(str(id_resultado))
                preco_vec.append(str(preco))
                hsm_vec.append(str(random_hsm[index]))
                id_flag_vec.append(str(id_flag))
                genero_vec.append(str(genero))
                indicado_vec.append(str(nome_amigo))
                hsm_secundario_vec.append(str(random_hsm_secundario[index]))
                forms_link_vec.append(id_forms)
                nome_especialista_vec.append(str(nome_especialista))
                dor_principal_vec.append(str(dor_principal))
                id_remarketing_vec.append(str(id_remarketing))
                token_cancelamento_vec.append(token_cancelamento)
                token_confirmacao_vec.append(token_confirmacao)
                unidade_federativa_vec.append(unidade_federativa)
                cidade_vec.append(cidade)
                ativo_vec.append(ativo)
                
            except Exception as e:
                print(f"ERROR-{telefone}, {nome}:- Erro inesperado ao processar cliente: {e}")
                continue
                
    
            ############################################################################
            ####################### INSERÇÃO DOS DADOS EM BATCHES ######################
            ############################################################################
            if ((index + 1) % batch == 0) or (len(dic_contatos.keys()) - index == 1 ):
                
                input_adicionaClientesBatch = {
                                "telefone_vec": telefone_vec,
                                "nome_vec": nome_vec,
                                "horario_consulta_vec": horario_consulta_vec,
                                "data_consulta_vec": data_consulta_vec,
                                "unidade_consulta_vec": unidade_consulta_vec,
                                "key_vec": key_vec,
                                "formatted_datetime_vec": formatted_datetime_vec,
                                "id_campanha_vec": id_campanha_vec,
                                "banco_de_dados_vec": banco_de_dados_vec,
                                "id_resultado_vec": id_resultado_vec,
                                "preco_vec": preco_vec,
                                "hsm_vec": hsm_vec,
                                "id_flag_vec": id_flag_vec,
                                "genero_vec": genero_vec,
                                "indicado_vec": indicado_vec,
                                "hsm_secundario_vec": hsm_secundario_vec,
                                "telefone_wpp": str(telefone_wpp),
                                "forms_link_vec": forms_link_vec,
                                "id_remarketing_vec": id_remarketing_vec,
                                "nome_especialista_vec": nome_especialista_vec,
                                "dor_principal_vec": dor_principal_vec,
                                'token_cancelamento_vec': token_cancelamento_vec,
                                'token_confirmacao_vec': token_confirmacao_vec,
                                "unidade_federativa_vec": unidade_federativa_vec,
                                'cidade_vec': cidade_vec,
                                'ativo_vec': ativo_vec
                                }
                print("input_adicionaClientesBatch: ", input_adicionaClientesBatch) 

                if osFunc('Ambiente_Lambda'): 
                    if 'producao' in os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
                        invokeFunction(input_adicionaClientesBatch, "arn:aws:lambda:sa-east-1:191246610649:function:producao_adicionaClientesBatch")
                    else:
                        invokeFunction(input_adicionaClientesBatch, "arn:aws:lambda:sa-east-1:191246610649:function:homolog_adicionaClientesBatch")
                else: adicionaClientesBatch(input_adicionaClientesBatch,{})

                # Re-inicialização dos vetores para começar novo batch
                telefone_vec = []
                nome_vec = []
                horario_consulta_vec = []
                data_consulta_vec = []
                unidade_consulta_vec = []
                key_vec = []
                formatted_datetime_vec = []
                id_campanha_vec = []
                id_resultado_vec = []
                banco_de_dados_vec = []
                preco_vec = []
                hsm_vec =  []
                id_flag_vec = []
                genero_vec = []
                indicado_vec = []
                hsm_secundario_vec =  []
                forms_link_vec = []
                id_remarketing_vec = []  ### REMARKETING ###
                nome_especialista_vec = []
                dor_principal_vec = []
                token_cancelamento_vec = []
                token_confirmacao_vec = []
                unidade_federativa_vec = []
                cidade_vec = []
                ativo_vec = []
                end_time_cliente = time.time()
                print(f"Tempo para adicionar batch de {batch} clientes: {end_time_cliente - start_time_cliente} s")
            
        ############################################################################
        #################### FINALIZAÇÃO E UPDATE DO UTILIZADO #####################
        ############################################################################
        update_query = f"UPDATE tb_log_envio_contatos SET utilizado = 1 WHERE id = {dic_df['id']}"
        execute_query_db(update_query)

    end_time = time.time()
    print(f"Tempo total: {end_time - start_time} s")

    return{
        'statusCode': 200
    }

