from utils.config import *

import re
import json
import pytz
import requests
import traceback
from datetime import datetime, timedelta

from utils.api_communication import registra_agendamentoAPI
from utils.connect_db import DatabaseConnection
from utils.locations import agendamento_passado 
from Models.conversa import Conversa
from Models.contato import Contato
from Models.unidade import Unidade

if osFunc('Ambiente_Lambda'): 
    from invoke_lambda import invokeFunction 
    from functions import *
    from extracao_dados import * 
else: 
    from geraResposta.lambda_function import lambda_handler as gera_resposta
    from logicaEncerramento.functions import *
    from logicaEncerramento.extracao_dados import * 

def lambda_handler(event, context):
    print("INFO(lambda_function.lambda_handler): EVENT ", json.dumps(event))
    current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo'))
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    
    http_session = requests.Session()
    tarefa = event['tarefa'] #"ENCERRAMENTO", "REPESCAGEM", "INTERVENCAO"
    id_conversa = event['id_conversa']
    id_contato = id_conversa[0]
    campanha = int(id_conversa[1])
    datetime_conversa = id_conversa[2]
    contato = Contato(id_contato)
    conversa = Conversa(contato, campanha, datetime_conversa)
    
    ret, msg = False,None
    
    if tarefa == 'INTERVENCAO':
        data = {'success': False,'limite_msg': False, 'palavras_bloqueadas': False, 'classificador_intervencao': False,}
        ############################################################################
        ############ DETECÇÃO FALHA (NECESSIDADE DE INTERVENÇÃO HUMANA) ############
        ############################################################################                      
        #################### LIMITAÇÃO DO NÚMERO DE MENSAGENS #######################
        if conversa.get_num_msgs() > osFunc('LIMITE_MENSAGENS'):
            print(f"WARNING(lambda_function.lambda_handler)-{id_contato}: Limite máximo de mensagens atingido")
            conversa.set_flag('ON_HOLD', formatted_datetime)
            conversa.set_resultado('INTERVENCAO')
            data['limite_msg'] = True
        else:
            ###################### FILTROS #########################
            # Obtém a última mensagem do cliente
            ultima_msg_cliente = conversa.get_ultima_msg(autor=1)
            # Obtém a última mensagem do bot
            ultima_msg_bot = conversa.get_ultima_msg(autor=0)
            ###################### DETECÇÃO DE PALAVRAS-PROIBIDAS #########################
            # Obtém as palavras proibidas do banco de dados
            df_palavras_proibidas = query_to_dataframe("SELECT * FROM PALAVRAS_BLOQUEADAS;")
            palavras_proibidas = df_palavras_proibidas['TEXTO'].tolist() #Lista de strings proibidas
            palavras_proibidas = [' ' + s + ' ' for s in palavras_proibidas] #Concatenar " " antes e depois de cada string proibida

            # Verifica palavras proibidas na última mensagem do cliente
            palavra_cliente, bool_palavra_cliente = buscar_palavras_chave(ultima_msg_cliente, palavras_proibidas)
            # Verifica palavras proibidas na última mensagem do bot
            palavra_bot, bool_palavra_bot = buscar_palavras_chave(ultima_msg_bot, palavras_proibidas)
            # Se uma palavra proibida for encontrada em qualquer uma das conversas, realiza a ação necessária
            if bool_palavra_cliente or bool_palavra_bot:
                if bool_palavra_cliente:
                    print(f"INFO(lambda_function.lambda_handler)-{id_contato}: Palavra proibida detectada na última mensagem do cliente = {palavra_cliente}")
                if bool_palavra_bot:
                    print(f"INFO(lambda_function.lambda_handler)-{id_contato}: Palavra proibida detectada na última mensagem do bot = {palavra_bot}")
                conversa.set_flag('ON_HOLD', formatted_datetime)
                conversa.set_resultado('INTERVENCAO')
                data['palavras_bloqueadas'] = True
            else:  
                ############################# NECESSIDADE DE INTERVENCAO HUMANA? ##############################
                input_gera_resposta = {
                        'id_conversa': [str(conversa.id_conversa[0]), str(conversa.id_conversa[1]), str(conversa.id_conversa[2])],
                        'tarefa': 'CLASSIFICADOR_INTERVENCAO'
                }
                resultado_intervencao = call_geraResposta(input_gera_resposta)
                if resultado_intervencao['error'] == False:
                    print(f"ERROR(lambda_function.lambda_handler)-{id_contato}: ERRO NO GPT - resultado_intervencao = {resultado_intervencao}")
                    current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo'))
                    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
                    conversa.set_flag("AGUARDAR", formatted_datetime)
                    data['success'] = False
                    return {
                            'body': json.dumps(data)
                        }
                    
                print(f"INFO(lambda_function.lambda_handler)-{id_contato}: resultado_intervencao = {resultado_intervencao}")
                if resultado_intervencao['resposta'] == "Muito insatisfeito":
                    conversa.set_flag('ON_HOLD', formatted_datetime)
                    conversa.set_resultado('INTERVENCAO')
                    data['classificador_intervencao'] = True
    
    elif tarefa in ['ENCERRAMENTO','REPESCAGEM']:
        data = {'success': False, 'deteccao_agendamento': False, 'post_agendamento': False, "encerramento_ret": False, "encerramento_msg": None}
        ############################################################################
        ###################### DETECÇÃO ENCERRAMENTO (SUCESSO) #####################
        ############################################################################ 
        if conversa.get_flag() not in [osFunc('NONE'), osFunc('ENVIAR_HSM'), osFunc('HSM_ENVIADO'), osFunc('ON_HOLD'), osFunc('HSM_LIDO'), osFunc('HSM_RECEBIDO')]:
            aparicoes = check_horario_ultima_msg(conversa)
            # a aparição de horario deve ser única para ser considera como agendamento (ou 2 em agendamentos múltiplos)
            if aparicoes > 0:
                inputGeraResposta = {
                    'id_conversa': [conversa.id_conversa[0], str(conversa.id_conversa[1]), conversa.id_conversa[2]],
                    'tarefa': 'CHECAR_ENCERRAMENTO_REPESCAGEM' if tarefa == 'REPESCAGEM' else 'CHECAR_ENCERRAMENTO'
                }
                
                DatabaseConnection.dispose_engine()
                resultado_encerramento = call_geraResposta(inputGeraResposta)
                
                if resultado_encerramento['error'] == False:
                    print(f"ERROR(lambda_function.lambda_handler)-{id_contato}: ERRO NO GPT - resultado_encerramento = {resultado_encerramento}")
                    current_datetime = datetime.now(pytz.timezone('America/Sao_Paulo'))
                    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
                    conversa.set_flag("AGUARDAR", formatted_datetime)
                    return retorno_separacao_ambientes(data)
                resultado_encerramento = resultado_encerramento['resposta']
            else:
                resultado_encerramento = 'Não confirmou agendamento'
            
            print(f"INFO(lambda_function.lambda_handler)-{id_contato}: resultado_encerramento = {resultado_encerramento}")

            if resultado_encerramento == 'Confirmou agendamento': 
                data['deteccao_agendamento'] = True
                resultado_dados = extrair_dados(id_conversa, tarefa)

                try:
                    # resgatando os dados extraídos
                    unidade_agendamento = resultado_dados['Unidade'] 
                    datetime_agendamento = resultado_dados['DATETIME'] 
                    qtd_agendamentos = resultado_dados['Quantidade clientes'] 
                    nomes_agendamento = resultado_dados['Nome']
                    
                    unidade_sigla, resultado_sigla = get_unidade_sigla(resultado_dados, http_session)
   
                    
                    if datetime_agendamento not in [False, 'False'] and False not in datetime_agendamento and 'False' not in datetime_agendamento and nomes_agendamento not in [False, 'False']:
 
                        # completando o cadastro de cliente
                        #nome_completo, email = completar_cadastro(conversa, resultado_dados)
                        
                        # extração da quantidade de agendamentos a serem realizados
                        qtd_agendamentos, nomes_agendamento, conversa = multiplos_nomes(qtd_agendamentos, nomes_agendamento, conversa)
                        print(f"INFO(lambda_function.lambda_handler)-{id_contato}: multiplos_nomes = {qtd_agendamentos}, {nomes_agendamento}")                                
                        
                        # checando se o horario requerido esteja 14 dias adiante
                        if pytz.timezone('America/Sao_Paulo').localize(datetime.strptime(datetime_agendamento[0], '%Y-%m-%d %H:%M')) > (datetime.now(pytz.timezone('America/Sao_Paulo'))  + timedelta(days=14)):
                            resultado_agendamento = 8
                            ret, msg = encerramento(http_session, conversa, formatted_datetime, resultado_agendamento) 
                        # caso o horário já tenha passado
                        elif agendamento_passado(datetime_agendamento[0][:-6], datetime_agendamento[0][-5:], unidade_sigla):
                            resultado_agendamento = 7     
                            ret, msg = encerramento(http_session, conversa, formatted_datetime, resultado_agendamento) 
                        elif qtd_agendamentos != len(nomes_agendamento):
                            resultado_agendamento = 5
                            ret, msg = encerramento(http_session, conversa, formatted_datetime, resultado_agendamento) 
                        else:
                            unidade_agendamento = unidade_sigla
                            
                            # checando se os horarios estao iguais para todos os clientes em agendamento multiplo
                            # em caso afirmativo, modifica-se para horarios seguidos
                            datetime_agendamento = [x for i, x in enumerate(datetime_agendamento) if datetime_agendamento.index(x) == i]
                            
                            if len(datetime_agendamento) != qtd_agendamentos:
                                for i in range(qtd_agendamentos - len(datetime_agendamento)):
                                    horario = int(datetime_agendamento[-1][-5:-3]) + i + 1
                                    if horario < 10:
                                        horario = "0" + str(horario)
                                    datetime_agendamento.append(datetime_agendamento[-1][:-5] + str(horario) + ':00' )

                            print(f"INFO(lambda_function.lambda_handler)-{id_contato}: datetime_agendamento = {datetime_agendamento}")
                            
                            if 'receptivo' in nomes_agendamento or 'leads_whatsapp' in nomes_agendamento or contato.nome in ['receptivo','leads_whatsapp']:
                                resultado_agendamento = 5
                                ret, msg = encerramento(http_session, conversa, formatted_datetime, resultado_agendamento)
                                http_session.close() 
                                if osFunc('Ambiente_Lambda'):
                                    data.update({'success': True})
                                    return {
                                        'body': json.dumps(data),
                                    }
                                else:
                                    return data

                            # para a quantidade de pessoas a serem agendadas
                            unidade = Unidade(unidade_agendamento)
                            unidade.get_agenda(unidade.sigla, n_clientes = conversa.agendamento_multiplo)

                            _, resultado_agendamento, hrr_resultados  = registra_agendamentoAPI(conversa, unidade, nomes_agendamento, datetime_agendamento)
                            
                            print(f"INFO(lambda_function.lambda_handler)-{id_contato}: resultados de horario = {hrr_resultados}")

                            ret, msg = encerramento(http_session,conversa, formatted_datetime, resultado_agendamento, unidade, datetime_agendamento, hrr_resultados)     
                              
                    else:
                        #########COMENTAR ESSE TRECHO DEPOIS (DEIXAR POR ENQUANTO PARA ACOMPANHAR)
                        resultado_agendamento = 'Unidade, data ou horário não encontrados'
                        # ret, msg = encerramento(http_session, conversa, formatted_datetime, resultado_agendamento) 
                        
                        print(f"INFO(lambda_function.lambda_handler)-{id_contato}: A unidade não existe ou não foram encontrados dados suficientes para agendamento")
                except Exception as e:
                    resultado_agendamento = 'Erro na logicaEncerramento'
                    conversa.set_flag('AGUARDAR', formatted_datetime)
                    conversa.set_resultado('INTERVENCAO')
                    print(f"ERROR(lambda_function.lambda_handler)-{id_contato}: Erro no agendamento {traceback.format_exc()}")
 

                print(f'INFO(lambda_function.lambda_handler)-{id_contato}: Resultado AGENDAMENTO - ', resultado_agendamento)

    data['encerramento_ret'] = ret
    data['encerramento_msg'] = msg
    print(f'INFO(lambda_function.lambda_handler)-{id_contato}: encerramento_ret - ', ret)
    print(f'INFO(lambda_function.lambda_handler)-{id_contato}: encerramento_msg - ', msg)
    
    http_session.close() 
    data['success'] = True
    return retorno_separacao_ambientes(data)
    



