from utils import connect_db
from Models.agendamento import Agendamento


class Contato():
    def __init__(self, id_contato, 
                 nome_cliente = None,  
                 email ='',
                 buscar_agendamento = True):
        
        self.id_contato = id_contato
        
        if nome_cliente is None:
            query = f"SELECT * FROM CONTATO WHERE TELEFONE = {id_contato}"
            df = connect_db.query_to_dataframe(query)
            data = df.iloc[0]
            self.telefone = data['TELEFONE']
            self.nome = data['NOME_CLIENTE']
        else: 
            self.telefone = id_contato
            self.nome = nome_cliente
        
        if buscar_agendamento:
            self.agendamento = Agendamento(id_contato)
        else:
            self.agendamento = None
            
        self.email = email
        
    def update_nome_contato(self,nome):
        connect_db.execute_query_db(f"UPDATE CONTATO SET NOME_CLIENTE = \'{nome}\' WHERE TELEFONE = \'{self.telefone}\';")
        self.nome = nome

