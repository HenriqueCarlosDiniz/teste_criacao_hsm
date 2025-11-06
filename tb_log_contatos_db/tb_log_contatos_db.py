from utils.config import *
from utils.connect_db import *

import numpy as np
import requests
import pandas as pd 

from tb_log_contatos_db.functions import *

def lambda_handler(event, context):
    
    URL_unidades = 'https://psdsaopaulo.net.br/psd2/laravel/public/api/acesso-dados-unidades/token/'
    token_API_lotacao = '6990aba8b6966e98be0de64d3bc80dcaa1abde8d788714f58329eaca9b3ad78c'
    url = URL_unidades + token_API_lotacao
    response = requests.get(url)
    
    if response.status_code == 200:
        # A resposta da API está no formato JSON, então podemos acessar os dados assim:
        data = response.json() 
        df = pd.DataFrame(data)
    else:
        print('Falha ao fazer a solicitação:', response.status_code)
    
    
    novo_df = adicionar_somas(df) 
     
    df_conversao = buscar_conversao_unidade()
    df_vip = buscar_vip()
    
    
    print(df_conversao)
    
    for i, camp in enumerate(df_conversao['id_campanha'].unique()):
        new_df = novo_df.merge(df_conversao[df_conversao['id_campanha'] == camp], how = 'left')
        new_df['id_campanha'] = camp
        new_df.fillna(0, inplace = True )
        if i == 0:
            df_score = new_df 
        else:
            df_score = pd.concat([df_score, new_df], ignore_index=True) 
    
    df_score = df_score.merge(df_vip, how = 'left')
    
    df_score.to_clipboard()

    df_score = df_score[df_score['total_vagas_a_preencher'] > 0]
    df_score = df_score[df_score['vip'] == 1]
    
    code_string = " (df_score['conversao'] * 1.01 + (1 - df_score['lotacao_c_overbooking']) * 0.1) + 100 * df_score['vip']"
    df_score['score'] = np.around(eval(code_string), 2)
    
    df_score.drop(columns=['vip'], inplace=True) 
    
    df_score['limite'] = df_score.apply(calcular_limite, axis=1)  
    
    df_score.sort_values(by = ['score'], ascending=False, inplace = True)

    

    for id_campanha in df_conversao['id_campanha'].unique():
        result = {'limite_total': str(2000),
              'limite_unidades': []}
        
        for i, row in df_score[df_score['id_campanha'] == id_campanha].iterrows():
            result['limite_unidades'].append({"unidade": row['unidade_psd'], "limite": row['limite']})
    
        result = str(result).replace("'",'"')
    
        if id_campanha == 0:
            nome_campanha = 'no_show_reciclado'
        elif id_campanha == 2:
            nome_campanha = 'cancelados_reciclado'

        print(result)
        execute_query_db(f'''INSERT INTO 
                         chatbot_V2.tb_log_contatos_bd (ranking_unidades, campanha) 
                         VALUES
                         ('{result}' , "{nome_campanha}")''')
        
    
