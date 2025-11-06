import os 
import re

if 'AWS_LAMBDA_FUNCTION_NAME' in os.environ:
    if 'producao' in os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
        os.environ['DB_DATABASE'] = 'homolog_chatbot_db'
        os.environ['DB_USERNAME'] = 'user_chatbot'
    else:
        os.environ['DB_DATABASE'] = 'homolog_chatbot_db'
        # os.environ['DB_USERNAME'] = 'homolog_chatbot'
        aws_lambda_function_name = os.environ.get('AWS_LAMBDA_FUNCTION_NAME', '')
        print(aws_lambda_function_name)
        if re.search('recebeMensagem', aws_lambda_function_name):
            os.environ['DB_USERNAME'] = 'producao_recebeMensagem'  
        elif re.search('processaMensagem', aws_lambda_function_name):
            os.environ['DB_USERNAME'] = 'producao_processaMensagem'  
        elif re.search('disparo' , aws_lambda_function_name, re.IGNORECASE):
            os.environ['DB_USERNAME'] = 'producao_disparo' 
        elif re.search('geraResposta', aws_lambda_function_name, re.IGNORECASE):
            os.environ['DB_USERNAME'] = 'producao_geraResposta'
        elif re.search('logicaEncerramento', aws_lambda_function_name, re.IGNORECASE):
            os.environ['DB_USERNAME'] = 'producao_logicaEncerramento'
        else:
            os.environ['DB_USERNAME'] = 'homolog_chatbot'

else: 
    # os.environ['DB_DATABASE'] = 'chatbot_V2'
    os.environ['DB_DATABASE'] = 'chatbot_V2'
    os.environ['DB_USERNAME'] = 'user_chatbot'


os.environ['DB_HOST'] = 'ls-403f490e9b5b5655c80c0d5d8061bd0baad36d11.ckr1nz432eyx.us-east-2.rds.amazonaws.com'
os.environ['DB_PASSWORD'] = 'q`Gd6:=cSB:M[CLdY)S|hf&oelMa8,;!'

if os.environ['DB_DATABASE'] == 'homolog_chatbot_db':
    os.environ['AMBIENTE_HOMOLOG'] = str(True)
else: 
    os.environ['AMBIENTE_HOMOLOG'] = str(False)



from utils.connect_db import query_to_dataframe

df = query_to_dataframe('''
    SELECT 
    VARIAVEL,
    VALOR,
    NULL AS NOME,
    NULL AS CUSTO_INPUT,
    NULL AS CUSTO_OUTPUT
    FROM CONFIG
    
    UNION
    
    SELECT 
    FLAG_NOME AS VARIAVEL,
    ID_FLAG AS VALOR,
    NULL,
    NULL,
    NULL
    FROM FLAG
    
    UNION
    
    SELECT
    NOME_CAMPANHA AS VARIAVEL,
    ID_CAMPANHA AS VALOR,
    NULL,
    NULL,
    NULL
    FROM CAMPANHA
    
    UNION
    
    SELECT 
    NOME_RESULTADO AS VARIAVEL,
    ID_RESULTADO AS VALOR,
    NULL,
    NULL,
    NULL
    FROM RESULTADO
    
    UNION
    
    SELECT 
    NULL, 
    NULL,
    NOME,
    CUSTO_INPUT,
    CUSTO_OUTPUT
    FROM MODELOS_GPT
''')

for index, row in df.iterrows():
    if row['NOME'] is not None and row['CUSTO_INPUT'] is not None and row['CUSTO_OUTPUT'] is not None:
        os.environ[f"{row['NOME']}_INPUT"] = str(row['CUSTO_INPUT'])
        os.environ[f"{row['NOME']}_OUTPUT"] = str(row['CUSTO_OUTPUT'])
    else:
        os.environ[row['VARIAVEL']] = row['VALOR']
    # 'gpt-4-0125-preview_INPUT' 'gpt-4-1106-preview_OUTPUT'

def osFunc(VarName):  

    Ambiente_Lambda = False
    
    if 'AWS_LAMBDA_FUNCTION_NAME' in os.environ: # Ambiente Lambda 
        Ambiente_Lambda = True
        chave_api = 'OPENAI_API_KEY'             # Chave da API OpenAI pro Lambda 
    else:
        chave_api = 'OPENAI_API_KEY_LOCAL'       # Chave da API OpenAI pro Local 

    if VarName == 'API_OPENAI':
        return os.environ[chave_api]
    
    if VarName == 'Ambiente_Lambda':
        return Ambiente_Lambda
    
    if VarName == 'AMBIENTE_HOMOLOG' and VarName in os.environ:
        return  True if os.environ[VarName] == 'True' else False

    try:
        if int(os.environ[VarName]) == float(os.environ[VarName]):
            return  int(os.environ[VarName])
        else: return  float(os.environ[VarName])
    except: return os.environ[VarName]
 
 
