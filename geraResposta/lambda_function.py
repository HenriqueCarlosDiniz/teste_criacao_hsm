from utils.config import * 

import json
import requests

from Models.contato import Contato
from Models.conversa import Conversa
from utils.connect_db import DatabaseConnection

if osFunc('Ambiente_Lambda'):
    from gera_resposta import gera_resposta  
else: 
    from geraResposta.gera_resposta import gera_resposta

def lambda_handler(event, context):  
    print('EVENT ', json.dumps(event))
    http_session = requests.Session()
    id_conversa = event['id_conversa']
    id_contato, campanha, datetime_conversa = id_conversa
    tarefa = event['tarefa']
    contato = Contato(id_contato)
    conversa = Conversa(contato, campanha, datetime_conversa)
    resposta, ret, custo = gera_resposta(tarefa, conversa, http_session)
    print(f"INFO(lambda_handler:resposta) - {str(id_contato)}: {resposta}")
    
    http_session.close()
    DatabaseConnection.dispose_engine()
    
    if osFunc('Ambiente_Lambda'):
        return {
            'body': json.dumps({ 'resposta': resposta, 'custo': custo, 'error': ret})
        }
    else:
        return{
        'resposta': resposta,
        'custo': custo,
        'error': ret
    }
