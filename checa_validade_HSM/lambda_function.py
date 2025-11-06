import requests
import json
import os 

from utils.config import *
from utils.connect_db import execute_query_db, query_to_dataframe
import re
from utils.send_wpp import enviar_wpp
# import urllib.parse

from datetime import datetime
import pytz
from Models.contato import *
from Models.agente import *

base = osFunc('URL_DIALOG_TEMPLATE')
URL_DIALOG_MESSAGE = osFunc('URL_DIALOG_MESSAGE')

HEADERS = {
    'D360-Api-Key': os.environ['API_DIALOG'],
    'Content-Type': "application/json",
}

def GPT(text):
    contexto = """
    Será passado um texto, altere o texto sem alterar o sentido do mesmo.
    As palavras entre {} devem permanecer inalteradas. 
    Evite alterar muito texto
    """  

    agente = Agent(contexto, model="gpt-4-0125-preview", json_mode = True)
    agente.json_mode = False
    resp = agente.get_response_sem_conversa([{"role": "system", "content": f"{contexto}"}, {"role": "user", "content": f"{text}"}])
    result = resp.choices[0].message.content

    return result
def new_name(id_hsm, last_name):
    if 'refurbished' in last_name: 
        last_name = last_name.split('refurbished')
        number = int(last_name[-1])
        number +=1
        last_name[-1] = number
        nomeHSM = id_hsm + '_refurbished' + str(number)  
    else:
        nomeHSM = id_hsm + '_refurbished1'
    return nomeHSM


def createPayload(new_name,URL_DIALOG_TEMPLATE, HEADERS, useGPT = False, payload = None):
    response = requests.get(URL_DIALOG_TEMPLATE, headers=HEADERS)
    response = response.text.encode('utf-8')
        
    # Parse the cleaned JSON
    response_json = json.loads(response)
    print('template no waba - ',response_json)
    response_json = response_json['waba_templates'][0]

    if useGPT:
        for i in range(10): 
            if 'text' in response_json['components'][i].keys():
                response_json['components'][i]['text'] = GPT(response_json['components'][i]['text'])
                break
            if len(response_json['components']) == i + 1:
                break 

    print(response_json)

    # Specify the keys you want to select
    selected_keys = ["category", "components", "language"]

    # Create a new dictionary with only the selected keys
    selected_data = {key: response_json[key] for key in selected_keys}
    selected_data["allow_category_change"] = "true" 
    selected_data['name'] = new_name

    
    stop = False
    for i in range(10, 0, -1):
        variable = str("{{") + str(i) + str("}}")  
        
        
        for j in range(10):
            try:
                if 'text' in selected_data['components'][j].keys():
                    if variable in selected_data['components'][j]['text']:
                        body_text = [['a' for n in range(i)]]
                        selected_data['components'][j]['example'] = {'body_text': body_text}
                        stop = True
                        break
            except:
                pass
            
            if  j + 1 == len(selected_data['components']):
                break 
            
        if stop:
            break
            

    _, text2upload = HSM_text_transformation( response_json['components'][j]['text'],payload )
    
    return selected_data, text2upload

def get_template_status():
    # filters = {"status": status}

    # Codificar o objeto JSON para que possa ser incluído na URL
    # encoded_filters = urllib.parse.quote(json.dumps(filters))

    # URL_DIALOG_TEMPLATE = os.environ['URL_DIALOG_TEMPLATE'] + '/limit:10000'
    URL_DIALOG_TEMPLATE  = 'https://waba.360dialog.io/v1/configs/templates' #+ f'?filters={encoded_filters}'

    # Perform a GET request
    response = requests.get(URL_DIALOG_TEMPLATE, headers=HEADERS)
    
    if response.status_code == 200:
        cleaned_response_text = response.text.encode('utf-8')
        response_json = json.loads(cleaned_response_text)


        result, category_dictionary = {}, {}
        for dictionary in response_json['waba_templates']:
            category_dictionary[dictionary['name']] = dictionary['category']
            result[dictionary['name']] = dictionary['status']
            
        return result, category_dictionary
    else:
        return None, None

def HSM_text_transformation(texto, payload):
    # Usando expressão regular para encontrar palavras entre chaves
    palavras_entre_chaves = re.findall(r'"text":\s\{([^}]+)\}', payload) 

    for i, t in enumerate(palavras_entre_chaves):
        texto = texto.replace( "{{"+ str(i+1) +"}}", "{"+ t +"}")
        print(texto, '\n\n')
    
    return palavras_entre_chaves, texto

def lambda_handler(event, context):
    current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo')).replace(tzinfo=None) 
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

    df = query_to_dataframe(f'SELECT * FROM HSM WHERE ATIVO NOT IN (-1,0)')
    status, category = get_template_status()  
    
    update_rows = []

    for i, row in df.iterrows():
        id_hsm = row['ID_HSM']
        payload = row['MESSAGE_PAYLOAD'] 
        id_campanha = row['ID_CAMPANHA']
        status_db = row['STATUS'] 
        category_db = row['TIPOLOGIA']

        match = re.search(fr'''{id_hsm}(.*?)"''', payload)   
        
        if not match is None:
            last_name = id_hsm + match.group(1) 
            
            # modificando o status, caso esteja desatualizado
            if status_db != status[last_name] or category_db != category[last_name]:
                update_rows.append(f"('{id_hsm}', '{id_campanha}', '{status[last_name]}', '{formatted_datetime}', '{category[last_name]}')")
                last_update = current_datetime
            else:
                last_update = row['ATUALIZADO_EM']
            
            # caculando o deltaT desde a última modificação 
            delta_time = current_datetime - last_update
            
            print('time delta - ', id_hsm, '- status - ', status_db, delta_time.total_seconds())
            
            # caso HSM esteja desativado e já passou por mais de uma hora, cria-se um novo HSM com GPT
            if (status[last_name] == 'disabled' or status[last_name] == 'rejected') and delta_time.total_seconds() > 3600:
                print("caso HSM esteja desativado e já passou por mais de uma hora, cria-se um novo HSM com GPT")
                new_HSM(id_hsm, last_name, payload, useGPT=True)
            # caso o HSM esteja desativado, cria-se um novo HSM com o mesmo texto
            if (status[last_name] == 'disabled' or status[last_name] == 'rejected') and status_db != status[last_name]:
                print("caso o HSM esteja desativado, cria-se um novo HSM com o mesmo texto")
                new_HSM(id_hsm, last_name, payload)
            # caso o HSM esteja pausado por um período maior que 6h, cria-se um novo HSM com o mesmo texto
            elif delta_time.total_seconds() > 3600 * 6 and status[last_name] == 'paused':
                print("caso o HSM esteja pausado por um período maior que 6h, cria-se um novo HSM com o mesmo texto")
                new_HSM(id_hsm,  last_name, payload)
            

    # atualizando o banco de dados
    values = ','.join(update_rows).replace('"(',"(").replace(')"',')"')
    print(f'INSERT INTO HSM (ID_HSM, ID_CAMPANHA, STATUS, ATUALIZADO_EM, TIPOLOGIA) VALUES ' + values + " ON DUPLICATE KEY UPDATE STATUS = VALUES(STATUS), ATUALIZADO_EM =  VALUES(ATUALIZADO_EM), TIPOLOGIA = VALUES(TIPOLOGIA);")
    if update_rows:
        execute_query_db(f'INSERT INTO HSM (ID_HSM, ID_CAMPANHA, STATUS, ATUALIZADO_EM, TIPOLOGIA) VALUES ' + values + " ON DUPLICATE KEY UPDATE STATUS = VALUES(STATUS), ATUALIZADO_EM =  VALUES(ATUALIZADO_EM), TIPOLOGIA = VALUES(TIPOLOGIA);")
        print(f'INSERT INTO HSM (ID_HSM, ID_CAMPANHA, STATUS, ATUALIZADO_EM, TIPOLOGIA) VALUES ' + values + " ON DUPLICATE KEY UPDATE STATUS = VALUES(STATUS), ATUALIZADO_EM =  VALUES(ATUALIZADO_EM), TIPOLOGIA = VALUES(TIPOLOGIA);")



    
def new_HSM(id_hsm, last_name, payload, useGPT = False):     
    
    nomeHSM = new_name(id_hsm, last_name)
    
    # Perform a GET request
    URL_DIALOG_TEMPLATE = base + f"""?filters=%7B%22business_templates.name%22%3A%22{id_hsm}%22%7D"""

    Uploadpayload, text2upload = createPayload(nomeHSM, URL_DIALOG_TEMPLATE, HEADERS, useGPT, payload)
    Uploadpayload = json.dumps(Uploadpayload, ensure_ascii=False, indent=None).encode('utf-8')
    
    response = requests.post(URL_DIALOG_TEMPLATE, data=Uploadpayload, headers=HEADERS)
    response_json = json.loads(response.text)
    
    print(Uploadpayload)
    print(response_json)
    
    
    if 'status' in response_json.keys():
        payload = payload.replace(last_name, nomeHSM) 

        execute_query_db(f"UPDATE HSM SET MESSAGE_PAYLOAD = '{payload}' WHERE ID_HSM = '{id_hsm}'")
    
        if useGPT:
            execute_query_db(f"UPDATE HSM SET CONTEUDO = '{text2upload}' WHERE ID_HSM = '{id_hsm}'")

        response = requests.delete(f'https://waba.360dialog.io/v1/configs/templates/{last_name}', headers=HEADERS)



