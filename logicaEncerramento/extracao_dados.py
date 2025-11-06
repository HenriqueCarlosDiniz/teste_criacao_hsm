from utils.config import *

import traceback

from utils.locations import get_all_units
from utils.text import similar

if osFunc('Ambiente_Lambda'):  from invoke_lambda import invokeFunction
else: from geraResposta.lambda_function import lambda_handler as gera_resposta


# def extrai_dados_receptivo(resultado_dados):
#     nome_completo = "False"
#     email = "False"
#     try:
#         nome_completo = resultado_dados['Nome']
#         email = resultado_dados['Email']
#     except:
#         print(f"ERROR(utils_lambda.extrai_dados_receptivo): Erro na extração de dados {traceback.format_exc()}")

#     return nome_completo, email
    
def extrair_dados(id_conversa, tarefa):
    input_gera_resposta = {
        'id_conversa': [id_conversa[0], str(id_conversa[1]), id_conversa[2]],
        'tarefa': 'EXTRACT_DATA_REPESCAGEM' if tarefa == 'REPESCAGEM' else 'EXTRACT_DATA'
    }

    if osFunc('Ambiente_Lambda'):  
        if 'producao' in os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            resultado_dados = invokeFunction(input_gera_resposta, "arn:aws:lambda:sa-east-1:191246610649:function:producao_geraResposta")
        else:
            resultado_dados = invokeFunction(input_gera_resposta, "arn:aws:lambda:sa-east-1:191246610649:function:homolog_geraResposta")
    else: 
        resultado_dados = gera_resposta(input_gera_resposta, {})
    print(f"INFO(utils_lambda.extrair_dados): resultado_dados = {resultado_dados}")
    
    try:
        resposta = resultado_dados['resposta']
        custo = resultado_dados['custo']
        error = resultado_dados['error']

        # Continue processing the extracted values as needed
    except:
        print(f"ERROR(utils_lambda.extrair_dados): Erro na extração de dados {traceback.format_exc()}")

    return resposta

def get_unidade_sigla(resultado_dados, http_session):
    unidade_extraida = resultado_dados['Unidade']
    unidades = get_all_units(http_session)
    unidades_siglas = list(unidades.keys())
    unidades_nomes = [value['Nome'] for value in unidades.values()] 

    if isinstance(unidade_extraida, bool):
        return False, False
    
    if unidade_extraida in unidades_siglas:  
        unidade_sigla = unidade_extraida
        resultado_sigla = True 
    else:
        max_similarity = 0.8
        closest_unit_name = None
        for name in unidades_nomes:
            similarity = similar(unidade_extraida, name) 
            if similarity > max_similarity:
                max_similarity = similarity
                closest_unit_name = name 
                if similarity > 0.98:
                    break 
        if closest_unit_name is not None:   
            resultado_sigla = True    
            idx = unidades_nomes.index(closest_unit_name)
            unidade_sigla = unidades_siglas[idx]
        else:
            resultado_sigla = False
            unidade_sigla = {} 
    return unidade_sigla, resultado_sigla