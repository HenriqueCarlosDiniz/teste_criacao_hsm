from utils.config import *

import os, json, pytz, time
from datetime import datetime

from utils.connect_db import  query_to_dataframe, DatabaseConnection, execute_query_db, execute_queries_db
from utils.api_communication import verificar_agendamento, verificar_confirmacao

if osFunc('Ambiente_Lambda'): 
    from invoke_lambda import invokeFunction
else: 
    from enviaRepescagem.lambda_function import lambda_handler as envia_repescagem


def lambda_handler(event, context): 
    if osFunc('Ambiente_Lambda'):
        if 'producao' in os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            invokeFunction({}, "arn:aws:lambda:sa-east-1:191246610649:function:homolog_repescagem")
    start_time = time.time()
    ############################################################################
    ####### VERIFICAÇÃO AGENDAMENTO PELO TELEMARKETING (ÀS 8H / 18H) ###########
    ############################################################################
    # Horário atual no fuso-horário 'America/Sao_Paulo'
    saopaulo_timezone = pytz.timezone('America/Sao_Paulo')
    horario_atual = datetime.now(saopaulo_timezone)

    # Períodos para verificação geral do agendamento pelo telemarketing
    evening_start = saopaulo_timezone.localize(datetime(horario_atual.year, horario_atual.month, horario_atual.day, 20, 45, 0))
    evening_end = saopaulo_timezone.localize(datetime(horario_atual.year, horario_atual.month, horario_atual.day, 20, 55, 0))

    # Verificação do período
<<<<<<< HEAD
    if morning_start <= horario_atual <= morning_end or evening_start <= horario_atual <= evening_end:
=======
    if evening_start <= horario_atual <= evening_end:
>>>>>>> 4aba98fc251f32f53c0dbf6082a658ef00e61949
        print(f"INFO(lambda_function.lambda_handler): Verificação geral de agendamento pelo telemarketing")

        checar_telemarketing(start_time)

        return {
        'statusCode': 200,
        'body': json.dumps('Repescagem finalizada.')
        }

    ############################################################################
    ################## REPESCAGEM EM BATCHES (10 / 10 min) #####################
    ############################################################################ 
    print(f"INFO(lambda_function.lambda_handler): Repescagem")
    
    repescagem()

    return {
        'statusCode': 200,
        'body': json.dumps('Repescagem finalizada.')
    }


def repescagem():

    df_contatos = get_contatos_repescagem()

    if df_contatos is not None:

        max_connections = int(os.environ['MAX_CONNECTIONS'])
        active_connections = int(DatabaseConnection.get_active_connections())
        available_connections = (max_connections - active_connections)
        n_batch = available_connections // int(os.environ['PORCENTAGEM_BATCHES'])

        numero_clientes = df_contatos.shape[0]  
        count_max = numero_clientes//n_batch

        print(f"INFO(lambda_function.repescagem): Número Clientes = {numero_clientes}")
        print(f"INFO(lambda_function.repescagem): Active_connections = {active_connections}")
        print(f"INFO(lambda_function.repescagem): Available Connections = {available_connections}")
        print(f"INFO(lambda_function.repescagem): Batchs = {n_batch}")

        if available_connections < n_batch + available_connections*float(os.environ['MARGEM_SEGURANCA_CONEXOES']):
            print(f"ERROR(lambda_function.repescagem): AVAILABLE CONNECTIONS < {n_batch + available_connections*float(os.environ['MARGEM_SEGURANCA_CONEXOES'])}")
            return {
                'statusCode': 200,
                'body': json.dumps('Hello from Lambda!')
            }
        
        processa_batch_repescagem(df_contatos, count_max, numero_clientes)


def get_contatos_repescagem():
    queryRepescagem = '''
        SELECT * FROM FILA_REPESCAGEM
        ;
    '''
    return query_to_dataframe(queryRepescagem)


def processa_batch_repescagem(df_contatos, count_max, numero_clientes):

    batch_id_contato = []
    batch_id_campanha = []
    batch_datetime_conversa = []
    count = 0

    for index, row in df_contatos.iterrows():

        id_contato = row['TELEFONE']
        id_campanha = row['ID_CAMPANHA']
        datetime_conversa = row['DATETIME_CONVERSA']
        resultado = row['ID_RESULTADO']

        if not resultado in [osFunc('SUCESSO'),osFunc('FALHA_AGENDAMENTO'),osFunc('INTERVENCAO')] and not id_campanha in [osFunc('CONFIRMACAO'),osFunc("CONFIRMACAO_2H")]:
            verificar_agendamento(id_contato, id_campanha, datetime_conversa)

        batch_id_contato.append(str(id_contato))
        batch_id_campanha.append(id_campanha)
        batch_datetime_conversa.append(datetime_conversa.strftime('%Y-%m-%d %H:%M:%S'))
        count+=1

        if count == count_max or index == numero_clientes - 1:
            input_envio = {
                            "id_contato": batch_id_contato,
                            "id_campanha": batch_id_campanha,
                            "datetime_conversa": batch_datetime_conversa
                            }
            
            if osFunc('Ambiente_Lambda'):
                if 'producao' in os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
                    invokeFunction(input_envio, "arn:aws:lambda:sa-east-1:191246610649:function:producao_enviaRepescagem")
                else:
                    invokeFunction(input_envio, "arn:aws:lambda:sa-east-1:191246610649:function:homolog_enviaRepescagem")
            else: 
                envia_repescagem(input_envio, {})

            batch_id_contato = []
            batch_id_campanha = []
            batch_datetime_conversa = []
            count = 0


def checar_telemarketing(start_time):

    df_contatos = get_contatos_telemarketing()

    if df_contatos is not None:

        tempo_maximo = 60*14 #14 minutos
        batch = 200 #chute: ~20s por batch de 200 pessoas. Consegue processar qse 80 batches (8000 pessoas) antes de timeout
        current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')) 
        formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        id_resultado = osFunc("TELEMARKETING")
        id_flag = osFunc("FINALIZADO")
        num_contatos = len(df_contatos.index)

        print("Número de clientes por batch: ", batch) 
        print("Número Contatos: ", num_contatos)

        processa_batch_telemarketing(df_contatos, start_time, tempo_maximo, batch, formatted_datetime, id_flag, id_resultado, num_contatos)


def get_contatos_telemarketing():
    # Query para pescar todas as conversas nos últimos 1 dias (pois na segunda-feira tem que incluir os de sexta-feira)
    queryTelemarketing = '''
        SELECT 
            CONVERSA.DATETIME_CONVERSA,
            CONVERSA.TELEFONE,
            CONVERSA.ID_CAMPANHA,
            CONVERSA.ID_RESULTADO
        FROM
            CONVERSA
        JOIN (
            SELECT TELEFONE, MAX(DATETIME_CONVERSA) AS MAX_DATETIME_CONVERSA
            FROM CONVERSA
            GROUP BY TELEFONE
        ) AS MAX_CONVERSA ON CONVERSA.TELEFONE = MAX_CONVERSA.TELEFONE AND CONVERSA.DATETIME_CONVERSA = MAX_CONVERSA.MAX_DATETIME_CONVERSA
        HAVING 
            CONVERSA.DATETIME_CONVERSA >= DATE_SUB(CURDATE(), INTERVAL 1 DAY) #Considerar últimos 1 dias
            AND ID_CAMPANHA NOT IN (14)
            AND ID_RESULTADO NOT IN (3, 4, 8, 9)
        ;
    '''
    return query_to_dataframe(queryTelemarketing)


def processa_batch_telemarketing(df_contatos, start_time, tempo_maximo, batch, formatted_datetime, id_flag, id_resultado, num_contatos):
    datetime_conversa_vec, formatted_datetime_vec, telefone_vec, id_campanha_vec, id_flag_vec, id_resultado_vec = [[] for _ in range(6)]
    total_telemarketing = 0
    
    for row in df_contatos.itertuples():
        t = time.time()
        if (t-start_time)> tempo_maximo: 
            print(f"WARNING(lambda_function.processa_batch_telemarketing): Tempo máximo da repescagem atingido: {t-start_time} / {tempo_maximo} s. Contatos atualizados: {batch*(index//batch)}")
            break

        id_contato = row.TELEFONE
        id_campanha = row.ID_CAMPANHA
        datetime_conversa = row.DATETIME_CONVERSA

        resp = verificar_confirmacao(id_contato)
        if (id_campanha == osFunc("CONFIRMACAO") and resp['confirmado']) or (id_campanha not in [osFunc("CONFIRMACAO"),osFunc("CONFIRMACAO_2H")] and resp['agendado']):
            datetime_conversa_vec.append(str(datetime_conversa))
            formatted_datetime_vec.append(str(formatted_datetime))
            telefone_vec.append(str(id_contato))
            id_campanha_vec.append(str(id_campanha))
            id_flag_vec.append(str(id_flag))
            id_resultado_vec.append(str(id_resultado)) 

        if ((row.Index + 1) % batch == 0) or (row.Index == df_contatos.index[-1]):
            total_telemarketing += len(telefone_vec)
            atualiza_db_telemarketing(datetime_conversa_vec, formatted_datetime_vec, telefone_vec, id_campanha_vec, id_resultado_vec, id_flag_vec)
            datetime_conversa_vec, formatted_datetime_vec, telefone_vec, id_campanha_vec, id_flag_vec, id_resultado_vec = [[] for _ in range(6)]
    
    print(f"INFO(lambda_function.processa_batch_telemarketing): Contatos mudados para TELEMARKETING: {total_telemarketing}. Contatos processados: {len(df_contatos.index)}")
    
def atualiza_db_telemarketing(datetime_conversa_vec, formatted_datetime_vec, telefone_vec, id_campanha_vec, id_resultado_vec, id_flag_vec):
    
    if len(telefone_vec) > 0:
        queries = []

        data_vetores = {"DATETIME_CONVERSA":datetime_conversa_vec, "DATETIME_FLAG": formatted_datetime_vec, "TELEFONE":telefone_vec, "ID_CAMPANHA": id_campanha_vec, "ID_RESULTADO":id_resultado_vec, "ID_LAST_FLAG": id_flag_vec}
        print(f"INFO(lambda_function.atualiza_db_telemarketing): CLientes telemarketing a serem atualizados no db: {data_vetores}")
        
        ############## Insert HIST FLAG ####################
        values_list = ", ".join([f"('{datetime_conversa}', '{telefone}', '{id_campanha}', '{formatted_datetime}', '{id_flag}')" for datetime_conversa, formatted_datetime, telefone, id_campanha, id_flag in zip(datetime_conversa_vec, formatted_datetime_vec, telefone_vec, id_campanha_vec, id_flag_vec)])
        query_flag_historico = f"""
            INSERT INTO FLAG_HISTORICO (DATETIME_CONVERSA, TELEFONE, ID_CAMPANHA, DATETIME_FLAG, ID_FLAG) 
            VALUES {values_list}
            ON DUPLICATE KEY UPDATE DATETIME_CONVERSA = VALUES(DATETIME_CONVERSA), TELEFONE = VALUES(TELEFONE), ID_CAMPANHA = VALUES(ID_CAMPANHA), DATETIME_FLAG = VALUES(DATETIME_FLAG), ID_FLAG = VALUES(ID_FLAG);
        """    
        queries.append(query_flag_historico)
        
        ############## Insert CONVERSA #####################
        values_list = ", ".join([f"('{datetime_conversa}', '{telefone}', '{id_campanha}', '{id_resultado}', '{id_flag}')" for datetime_conversa, telefone, id_campanha, id_resultado, id_flag in zip(datetime_conversa_vec, telefone_vec, id_campanha_vec, id_resultado_vec, id_flag_vec)])
        query_conversa = f"""
            INSERT INTO CONVERSA (DATETIME_CONVERSA, TELEFONE, ID_CAMPANHA, ID_RESULTADO, ID_LAST_FLAG) 
            VALUES {values_list}
            ON DUPLICATE KEY UPDATE DATETIME_CONVERSA = VALUES(DATETIME_CONVERSA), TELEFONE = VALUES(TELEFONE), ID_CAMPANHA = VALUES(ID_CAMPANHA), ID_RESULTADO = VALUES(ID_RESULTADO), ID_LAST_FLAG = VALUES(ID_LAST_FLAG);
        """
        queries.append(query_conversa)

        execute_queries_db(queries)