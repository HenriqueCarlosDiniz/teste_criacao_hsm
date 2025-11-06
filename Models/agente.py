import openai, os
from utils.config import osFunc
from utils.connect_db import execute_query_db


class Agent:
    def __init__(self, context = '', temperature = 0.3, detailed = False, model = "gpt-3.5-turbo-1106", functions = None, func_temperature = 0.25, json_mode = False, bool_user_string = False) -> None:
        self.context = context
        self.temperature = temperature
        self.model = model
        self.functions = functions
        self.func_temperature = func_temperature 
        self.detailed = detailed
        self.json_mode = json_mode
        self.bool_user_string = bool_user_string
    
    def get_response_sem_conversa(self, discussion, use_functions = False):
        openai.api_key = str(osFunc('OPENAI_API_KEY'))
  
        if use_functions:
            if self.json_mode:
                functions = self.functions
                completion = openai.ChatCompletion.create(
                model = self.model,
                response_format={"type": "json_object"}, 
                messages = discussion, 
                temperature = self.temperature,
                functions = functions)
            else:
                functions = self.functions
                completion = openai.ChatCompletion.create(
                model = self.model,
                messages = discussion, 
                temperature = self.temperature,
                functions = functions)

        else: 
            if self.json_mode:
                completion = openai.ChatCompletion.create(
                model = self.model, 
                response_format={ "type": "json_object" },
                messages = discussion, 
                temperature = self.temperature)
            else:
                completion = openai.ChatCompletion.create(
                model = self.model, 
                messages = discussion, 
                temperature = self.temperature)
             
        
        return completion
    
    def get_response_simples(self, discussion, conversa, use_functions = False):
        
        # Mapeamento de campanhas para suas chaves API e mensagens específicas
        campanhas_config = {
            str(osFunc("INDIQUE_AMIGOS")): ("OPENAI_INDIQUE_AMIGOS", "INDIQUE_AMIGOS"),
            str(osFunc("NOSHOW")): ("OPENAI_NO_SHOW", "NOSHOW"),
            str(osFunc("CONFIRMACAO")): ("OPENAI_CONFIRMACAO", "CONFIRMACAO"),
            str(osFunc("CANCELADOS")): ("OPENAI_CANCELADOS", "CANCELADOS"),
            str(osFunc("RECEPTIVO")): ("OPENAI_RECEPTIVO", "RECEPTIVO"),
            str(osFunc("SAPATOS_SITE")): ("OPENAI_SAPATOS_SITE", "SAPATOS_SITE"),
            str(osFunc("RECEPTIVO_GOOGLE")): ("OPENAI_AGENDAMENTO_SITES", "RECEPTIVO_GOOGLE"),
            str(osFunc("AGENDAMENTO_SITES")): ("OPENAI_AGENDAMENTO_SITES", "AGENDAMENTO_SITES"),
            str(osFunc("CONFIRMACAO_2H")): ("OPENAI_CONFIRMACAO_2H", "CONFIRMACAO_2H"),
            str(osFunc("LEADS_WHATSAPP")): ("OPENAI_LEADS_WHATSAPP", "LEADS_WHATSAPP"),
            
            str(osFunc("NOSHOW")) + "_DB": ("OPENAI_NO_SHOW_BD", "NOSHOW"),
            str(osFunc("CANCELADOS")) + "_DB": ("OPENAI_CANCELADOS_BD", "CANCELADOS"),
            str(osFunc("INDIQUE_AMIGOS")) + "_DB": ("OPENAI_INDIQUE_AMIGOS_BD", "INDIQUE_AMIGOS"),
        }

        try:
            # Converter conversa.campanha para string uma única vez
            campanha_str = str(conversa.campanha)
            
            # Verificar se a campanha existe no mapeamento
            if campanha_str in campanhas_config:
                # Obter configurações da campanha
                if conversa.banco_dados == 1:
                    campanha_str += "_DB"
                chave_api, mensagem_campanha = campanhas_config[campanha_str]
                openai.api_key = str(osFunc(chave_api))
                
                # Imprimir informação da campanha
                print(f"INFO(agente.get_response_simples)-{conversa.contato.id_contato}: CAMPANHA {campanha_str} - {mensagem_campanha}")
            else:
                # Campanha não encontrada
                print(f"WARNING(agente.get_response_simples)-{conversa.contato.id_contato}: Campanha {campanha_str} não encontrada")
                openai.api_key = str(osFunc('OPENAI_API_KEY'))
        except:
            print(f"ERROR(agente.get_response_simples)-{conversa.contato.id_contato}: Campanha não encontrada, usando chave comum")
            openai.api_key = str(osFunc('OPENAI_API_KEY'))
        if use_functions:
            if self.json_mode:
                functions = self.functions
                completion = openai.ChatCompletion.create(
                model = self.model,
                response_format={"type": "json_object"}, 
                messages = discussion, 
                temperature = self.temperature,
                functions = functions)
            else:
                functions = self.functions
                completion = openai.ChatCompletion.create(
                model = self.model,
                messages = discussion, 
                temperature = self.temperature,
                functions = functions)

        else: 
            if self.json_mode:
                completion = openai.ChatCompletion.create(
                model = self.model, 
                response_format={ "type": "json_object" },
                messages = discussion, 
                temperature = self.temperature)
            else:
                completion = openai.ChatCompletion.create(
                model = self.model, 
                messages = discussion, 
                temperature = self.temperature)
                

        calcula_precos(completion, self, conversa)
        
        return completion
    
def calcula_precos(completion, agente, conversa):
        # Update de preços        
        tokens_input = completion['usage']['prompt_tokens']
        tokens_output = completion['usage']['completion_tokens']

        # Busca os preços nas variáveis de ambiente e os converte para float
        preco_input = float(os.getenv(f"{agente.model}_INPUT", 0))
        preco_output = float(os.getenv(f"{agente.model}_OUTPUT", 0))

        # Calcula o preço total
        preco = (tokens_input * preco_input + tokens_output * preco_output)
            
        query = f'''
        UPDATE CONVERSA
        SET PRECO = PRECO + {preco}
        WHERE TELEFONE = '{conversa.contato.id_contato}' AND DATETIME_CONVERSA = STR_TO_DATE('{conversa.datetime_conversa}', '%Y-%m-%d %H:%i:%s')
        '''
        
        execute_query_db(query)