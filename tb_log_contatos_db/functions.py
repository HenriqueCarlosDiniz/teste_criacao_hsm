from datetime import datetime, timedelta 
import pandas as pd
import numpy as np
from utils.config import *
from utils.connect_db import *


def get_formatted_date(delta):
    today = datetime.now()
    tomorrow = today + timedelta(days=delta)
    formatted_date = tomorrow.strftime('%Y-%m-%d')
    return formatted_date

def adicionar_somas(df): 
    # resgatando apenas a informação de agenda
    df = df.loc['agenda']
    
    # dias para o resgate de informação
    dias = []
    for i in range(1,2): 
        dias.append(get_formatted_date(delta = i)) 

    first_value = True
    for unidade, value in df.items(): 
        
        # transformando o json em dataframe
        df_value = pd.DataFrame.from_dict(value, orient='index')

        # resgatando os valores de D1
        df_value = df_value.loc[list(set(df_value.index) & set(dias))]

        # criando uma linha para o nome da unidade
        df_value['unidade_psd'] = unidade

        # criando o dataframe de resultado ou adicionando os novos valores
        if first_value:
            first_value = False
            df_result = df_value
        else:
            df_result = pd.concat([df_result, df_value]).reset_index(drop=True)



    df_result['lotacao_c_overbooking'] = df_result['total_agendamentos'] / (df_result['total_vagas'] + df_result['total_disponivel_para_overbooking'])
    df_result['lotacao_c_overbooking'] = df_result['lotacao_c_overbooking'].astype(float)
    df_result['lotacao_c_overbooking'] = np.around(df_result['lotacao_c_overbooking'],2)

    df_result = df_result[df_result['total_vagas'] > 0]

    return df_result



def buscar_conversao_unidade():
    df = query_to_dataframe('''
                            SELECT 
                            CONVERSA.ID_CAMPANHA AS id_campanha,
                            CAMP_AGENDAMENTO.UNIDADE_PSD AS unidade_psd,
                            SUM(CASE WHEN CONVERSA.ID_RESULTADO = 3 THEN 1 ELSE 0 END)/ COUNT(CONVERSA.ID_RESULTADO) AS conversao,
                            COUNT(ID_RESULTADO) AS qt_clientes
                            FROM chatbot_V2.CONVERSA 
                            LEFT JOIN chatbot_V2.CAMP_AGENDAMENTO USING(TELEFONE)
                            WHERE 
                            CONVERSA.BANCO_DADOS = 1 AND
                            CONVERSA.ID_RESULTADO NOT IN (0,1,8) 
                            GROUP BY CAMP_AGENDAMENTO.UNIDADE_PSD
                            ''')
    
    # df = df.rename(columns = {'CONVERSAO': "%CONVERSAO"})

    return df 

def calcular_limite(row):
    if row['conversao'] > 0:
        value =  int(row['total_vagas_a_preencher'] / row['conversao'])
    else:
        value = int(row['total_vagas_a_preencher'] / 0.02)

    return min(value, 400)

def buscar_vip():
    df = query_to_dataframe('''
                            SELECT 
                            UNIDADE_SIGLA AS unidade_psd,
                            VIP AS vip
                            FROM 
                            UNIDADES_VIP
                            
                            ''')
    
    return df
