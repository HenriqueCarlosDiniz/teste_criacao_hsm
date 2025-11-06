import os 

# os.environ['DB_DATABASE'] = 'homolog_chatbot_db'
# os.environ['DB_DATABASE'] = 'homolog_chatbot_db'
from utils.connect_db import query_to_dataframe
os.environ['DB_DATABASE'] = 'chatbot_V2'
os.environ['DB_HOST'] = 'ls-403f490e9b5b5655c80c0d5d8061bd0baad36d11.ckr1nz432eyx.us-east-2.rds.amazonaws.com'
os.environ['DB_PASSWORD'] = 'q`Gd6:=cSB:M[CLdY)S|hf&oelMa8,;!'
os.environ['DB_USERNAME'] = 'user_chatbot'



df = query_to_dataframe('''
                        SELECT * FROM CONFIG
                        
                        UNION
                        
                        SELECT 
                        FLAG_NOME AS VARIAVEL,
                        ID_FLAG AS VALOR
                        FROM FLAG

                        UNION

                        SELECT
                        NOME_CAMPANHA AS VARIAVEL,
                        ID_CAMPANHA AS VALOR 
                        FROM CAMPANHA

                        UNION

                        SELECT 
                        NOME_RESULTADO AS VARIAVEL,
                        ID_RESULTADO AS VALOR
                        FROM RESULTADO
                        ''')

for index, row in df.iterrows():
    os.environ[row['VARIAVEL']] = row['VALOR']


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

    try:
        if int(os.environ[VarName]) == float(os.environ[VarName]):
            return  int(os.environ[VarName])
        else: return  float(os.environ[VarName])
    except: return os.environ[VarName]
 
 
