from utils.config import *
import json 
from utils.api_communication import GET_vagas_disponiveis
from utils.text import similar
from utils.send_wpp import enviar_wpp
from utils.text import formatar_data
from datetime import datetime, timedelta
from Models.unidade import *

if osFunc('Ambiente_Lambda'): 
    #from extracao_dados import extrai_dados_receptivo 
    from invoke_lambda import invokeFunction
else:  
    from geraResposta.lambda_function import lambda_handler as gera_resposta
    #from logicaEncerramento.extracao_dados import extrai_dados_receptivo



# def completar_cadastro(conversa, resultado_dados):
#     if conversa.completar_cadastro:
#         nome_completo, email = extrai_dados_receptivo(resultado_dados)

#         if nome_completo != 'False':    
#             conversa.contato.nome = nome_completo
#         if email != 'False':
#             conversa.contato.email = email            
        
#         conversa.set_cadastro(nome_completo, email)
        
#         return nome_completo, email
#     else:
#         return None, None


def vagas_disponiveis_multiplo_agendamento(qtd_clientes,data_agendada, horario,unidade, http_session):
    horario = int(horario[:2])

    datas_disponiveis = GET_vagas_disponiveis(data_agendada, 
                            data_agendada, 
                            unidade, http_session)
    
    vagas = json.loads(datas_disponiveis.text)

    horarios_com_vagas = []

    for date, times in vagas.items():
        for time, info in times.items():
            if int(time[:2]) == horario:
                if "vagas" in info and info["vagas"] > 0:
                    horarios_com_vagas.append(time)


def get_msg_horario_indisponivel(conversa, unidade_agendamento, hrr_resultados, http_session):

    # unidade_agendamento = conversa.contato.agendamento.unidade_agendamento
    datetime_estudado = hrr_resultados['hrr_indisponiveis'][0]
    data_agendamento = datetime_estudado[:-6]
    horario_agendamento = datetime_estudado[-5:]

    data = datetime.strptime(data_agendamento, "%Y-%m-%d")
    data_mais_14 = data + timedelta(days=14)
    datas_disponiveis = GET_vagas_disponiveis(data.strftime("%Y-%m-%d"), 
                        data_mais_14.strftime("%Y-%m-%d"), 
                        unidade_agendamento, http_session)
    vagas = json.loads(datas_disponiveis.text)
    
    proxima_data_encontrada = False
    horarios_disponiveis = []
    data_mesmo_horario = False
    # busca dos horarios da data disponível mais próxima e data mais proxima com o mesmo horario
    for data, times_list in vagas.items(): 
        try:
            valid_times = [time for time, vagasDisponiveis in times_list.items() if vagasDisponiveis['total'] > 0 and not( str(data) + ' ' + str(time)  in hrr_resultados['disponibilidade_hrr'])]
            if conversa.agendamento_multiplo > 1:
                hours = [int(x[:2]) for x in valid_times ]
                available_time = []
                for i in range(len(hours) - conversa.agendamento_multiplo + 1): 
                    if hours[i+conversa.agendamento_multiplo-1] - hours[i] + 1 == conversa.agendamento_multiplo:
                         available_time.append(valid_times[i])
            else:
                available_time = valid_times
            if len(available_time) > 0 and not proxima_data_encontrada:
                proxima_data_encontrada = True
                proxima_data = data
                proxima_data_formatada = formatar_data(data)
                horarios_disponiveis = available_time
            if horario_agendamento +':00' in available_time:
                data_mesmo_horario = data
                data_mesmo_horario_formatada = formatar_data(data)
                break
        except: pass 
    

    horarios_disponiveis = [s[:-3] for s in horarios_disponiveis]
    concat_times = '\n- '.join(horarios_disponiveis) 

    unidade = Unidade(unidade_agendamento)
    unidade_nome = unidade.nome  


    hrr_resultados['disponibilidade_hrr'] = [datetime.strptime(x, "%Y-%m-%d %H:%M").strftime("%H:%M no dia %d/%m/%Y") for x in hrr_resultados['disponibilidade_hrr']]
    hrr_resultados['hrr_indisponiveis'] = [datetime.strptime(x, "%Y-%m-%d %H:%M").strftime("%H:%M no dia %d/%m/%Y") for x in hrr_resultados['hrr_indisponiveis']]
    
    string_hrr_disponiveis = ''
    if len(hrr_resultados['disponibilidade_hrr'])> 1:
        string_hrr_disponiveis = f""" os horários {','.join(hrr_resultados['disponibilidade_hrr'])} estão disponíveis, mas"""
    elif len(hrr_resultados['disponibilidade_hrr']) == 1:
        string_hrr_disponiveis = f""" o horário {','.join(hrr_resultados['disponibilidade_hrr'])} está disponível, mas"""
  
    if len(hrr_resultados['hrr_indisponiveis'])> 1:
        string_hrr_indisponiveis = f""" os horários {','.join(hrr_resultados['hrr_indisponiveis'])} não estão mais disponíveis"""
    elif len(hrr_resultados['hrr_indisponiveis']) == 1:
        string_hrr_indisponiveis = f""" o horário {','.join(hrr_resultados['hrr_indisponiveis'])} não está mais disponível"""
    
    
    if (data_mesmo_horario) and (proxima_data != data_mesmo_horario): 
        msg = f'''{conversa.contato.nome}, verifiquei no sistema que {string_hrr_disponiveis}{string_hrr_indisponiveis}. Mas não se preocupe, ainda temos ótimas opções! Para a unidade {unidade_nome} {proxima_data_formatada} temos os seguintes horários disponíveis: \n\n- {concat_times}.\n\n Também temos o mesmo horário das {horario_agendamento} {data_mesmo_horario_formatada}. Algum deles fica bom, ou posso verificar a disponibilidade em outro dia?'''
    elif len(horarios_disponiveis)>0:
        msg = f'''{conversa.contato.nome}, verifiquei no sistema que {string_hrr_disponiveis}{string_hrr_indisponiveis}. Mas não se preocupe, ainda temos ótimas opções! Para a unidade {unidade_nome} {proxima_data_formatada} temos os seguintes horários disponíveis: \n\n- {concat_times}.\n\n Algum deles fica bom, ou posso verificar a disponibilidade em outro dia?'''
    elif len(horarios_disponiveis)==0:
        msg = f'''{conversa.contato.nome}, verifiquei no sistema que {string_hrr_disponiveis}{string_hrr_indisponiveis}. Infelizmente para essa unidade não temos mais disponibilidade nas próximas duas semanas, posso verificar outra unidade?'''
    else: 
        print(f"ERROR(get_msg_horario_indisponivel) - Erro na logicaEncerramento") 
        msg = ''

    if len(horarios_disponiveis)==0 and msg != '' and conversa.agendamento_multiplo > 2:
        msg += '\nPor favor, escolha um horário para cada pessoa que realizará a consulta.'
    
    print(f"INFO(get_msg_horario_indisponivel) horarios disponiveis: {horarios_disponiveis}") 

    return msg    



def buscar_palavras_chave(string, palavras_chave):
    """
    Busca por palavras-chave em uma string.

    Args:
        string (str): A string na qual procurar por palavras-chave.
        palavras_chave (list): Lista de palavras-chave a serem procuradas.

    Returns:
        tuple: Uma tupla contendo a palavra-chave encontrada (ou None se nenhuma for encontrada)
               e um booleano indicando se pelo menos uma palavra-chave foi encontrada.
    """
    # Procura por palavras-chave na string, ignorando maiúsculas e minúsculas
    palavra_encontrada = next((palavra for palavra in palavras_chave if palavra in string.lower()), None)

    # Verifica se pelo menos uma palavra-chave foi encontrada
    if palavra_encontrada:
        # Se encontrou, retorna a palavra-chave e True
        return palavra_encontrada, True
    else:
        # Se não encontrou, retorna None e False
        return None, False


def agendamento_multiplo_classificador(conversa):
    input_gera_resposta = {
            'id_conversa': [str(conversa.id_conversa[0]), str(conversa.id_conversa[1]), str(conversa.id_conversa[2])],
            'tarefa': 'CLASSIFICADOR_AGENDAMENTO_MULTIPLO'
    }
    
    if osFunc('Ambiente_Lambda'):  
        if 'producao' in os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            resultado_agendamento_multiplo = invokeFunction(input_gera_resposta, "arn:aws:lambda:sa-east-1:191246610649:function:producao_geraResposta")
        else:
            resultado_agendamento_multiplo = invokeFunction(input_gera_resposta, "arn:aws:lambda:sa-east-1:191246610649:function:homolog_geraResposta")
    else: 
        resultado_agendamento_multiplo = gera_resposta(input_gera_resposta, {})

    print(f"INFO(lambda_function.lambda_handler)-{conversa.id_conversa[0]}: resultado_agendamento_multiplo = {resultado_agendamento_multiplo}")
    
    return resultado_agendamento_multiplo['resposta']

def multiplos_nomes(qtd_agendamentos, nomes_agendamento, conversa):

    # transformando todos valores da lista em string
    nomes_agendamento = [str(element) for element in nomes_agendamento]
    
    print(f"INFO(lambda_function.lambda_handler) - multiplos_nomes {nomes_agendamento}")
    
    if qtd_agendamentos == "False" or qtd_agendamentos == False:
        qtd_agendamentos = 1
        nomes_agendamento = []
    
    if nomes_agendamento == []:
        nomes_agendamento.append(conversa.contato.nome)

    # deletando palavras proibidas
    index2delete = []
    for i, nome in enumerate(nomes_agendamento):
        for forbidden_word in ['mãe de', 'pai de', 'filho de', 'filha de', 'irmão de', 'irmã de', 'primo de', 'prima de','tio de','tia de','avô de','avó de','você de', 'cunhad de']:
            similarity = similar(nome, forbidden_word)  
            if 0.5 < similarity:
                index2delete.append(i)
                break
    
    nomes_agendamento = [value for index, value in enumerate(nomes_agendamento) if index not in index2delete]
                
    # identificando se o nome do cliente principal já está contido na lista de nomes
    # caso não, o nome do cliente principal é adicionado na lista
    if len(nomes_agendamento) != qtd_agendamentos or (
        'False' in nomes_agendamento or 
        'false' in nomes_agendamento or 
        False in nomes_agendamento
    ):
        for i, name in enumerate(nomes_agendamento):
            similarity = similar(name, conversa.contato.nome)  
            print(f"aaaaaaaaa - {name} in {['False', 'false', False]}")
            if 0.5 < similarity or name in ['False', 'false', False]:   
                nomes_agendamento[i] = conversa.contato.nome 

                while False in nomes_agendamento:
                    nomes_agendamento.remove(False) 
                while 'False' in nomes_agendamento:
                    nomes_agendamento.remove('False')
                break
            
            elif i == len(nomes_agendamento) - 1:
                nomes_agendamento.append(conversa.contato.nome)
                nomes_agendamento = nomes_agendamento[::-1] 

    print(f"INFO(lambda_function.lambda_handler) - multiplos_nomes tratados {nomes_agendamento}")
    
    if conversa.agendamento_multiplo != qtd_agendamentos:
        conversa.atualiza_agendamento_multiplo(qtd_agendamentos)

    return qtd_agendamentos, nomes_agendamento, conversa
    
def get_msg_falha_agendamento(conversa, datetime_agendamento):
    msg = f'{conversa.contato.nome}, seu agendamento ficou marcado para\n'
    
    for datetime in datetime_agendamento: 
        msg += f' - {formatar_data(datetime[:-6])} às {datetime[-5:]}\n'
    msg += 'poderia confirmar se está correto?'
    
    return msg
        
def encerramento(http_session, conversa, formatted_datetime, id_encerramento, unidade = None, datetime_agendamento = None, hrr_resultados = None):
    print(f"INFO(lambda_function.lambda_handler) - encerramento")
    df_encerramento = query_to_dataframe(f"SELECT * FROM LOGICA_ENCERRAMENTO WHERE ID_ENCERRAMENTO = {id_encerramento}")
    id_flag = df_encerramento['ID_FLAG'].iloc[0]
    id_resultado = df_encerramento['ID_RESULTADO'].iloc[0]
    
    
    print(f"INFO(lambda_function.lambda_handler) - id resultado {id_resultado}")
    conversa.set_flag(id_flag, formatted_datetime)
    if not id_resultado is None:
        conversa.set_resultado(id_resultado)      
    

    msg = df_encerramento['MENSAGEM'].iloc[0] 
    
    if not msg is None:
        msg = msg.replace('{cliente}', conversa.contato.nome)
        
        if not unidade is None:
            msg = msg.replace('{unidade_agendamento}', unidade.nome)
        if not datetime_agendamento is None:
            msg = msg.replace('{data_agendamento}', datetime_agendamento[0])
    
 
    if msg is None:
        return False, None
    elif msg == "get_msg_falha_agendamento" and not datetime_agendamento is None:
        msg = get_msg_falha_agendamento(conversa, datetime_agendamento)
    elif msg == "get_msg_horario_indisponivel":
        msg = get_msg_horario_indisponivel(conversa, unidade.sigla, hrr_resultados, http_session)

     
    return True, msg


def call_geraResposta(inputGeraResposta):
    if osFunc('Ambiente_Lambda'):  
        if 'producao' in os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            resultado = invokeFunction(inputGeraResposta, "arn:aws:lambda:sa-east-1:191246610649:function:producao_geraResposta")
        else:
            resultado = invokeFunction(inputGeraResposta, "arn:aws:lambda:sa-east-1:191246610649:function:homolog_geraResposta")
    else: 
        resultado = gera_resposta(inputGeraResposta,{})
    return resultado

def check_horario_ultima_msg(conversa):
    # filtro para checar se há horarios na ultima mensagem
    padroes = ['7:', '8:', '9:', '10:', '11:', '12:', '13:', '14:', '15:', '16:', '17:', '18:','7h','8h', '9h', '10h', '11h', '12h', '13h', '14h', '15h', '16h', '17h', '18h']
    aparicoes = 0
    ultima_msg_bot =  conversa.get_ultima_msg(autor=0)
    for substring in padroes: 
        if substring in ultima_msg_bot:
            aparicoes += 1
    return aparicoes
            
    # a aparição de horario deve ser única para ser considera como agendamento (ou 2 em agendamentos múltiplos)


def retorno_separacao_ambientes(data):
    if osFunc('Ambiente_Lambda'):
        return {
            'body': json.dumps(data),
        }
    else:
        return data
    

