from sqlalchemy import create_engine, pool, text
import pandas as pd
import os


class DatabaseConnection:
    _instance = None
    _last_db_name = None  # Adiciona um controle para o nome do banco de dados

    def __new__(cls):
        db_name = os.environ.get('DB_DATABASE')  # Pega o nome atual do banco de dados
        if cls._instance is None or cls._last_db_name != db_name:
            cls._instance = super(DatabaseConnection, cls).__new__(cls)
            cls._last_db_name = db_name  # Atualiza o controle para o nome do banco
            connection_string = f"mysql+pymysql://{os.environ.get('DB_USERNAME')}:{os.environ.get('DB_PASSWORD')}@{os.environ.get('DB_HOST')}/{db_name}"
            cls._instance.engine = create_engine(connection_string, poolclass=pool.NullPool)
        return cls._instance

    @staticmethod
    def get_engine():
        return DatabaseConnection()._instance.engine

    @staticmethod
    def get_active_connections():
        engine = DatabaseConnection.get_engine()
        with engine.connect() as connection:
            result = connection.execute(text("SHOW STATUS WHERE variable_name = 'Threads_connected';"))
            row = result.fetchone()
            if row:
                return int(row[1])
            else:
                return None
            
    @staticmethod
    def dispose_engine():
        if DatabaseConnection._instance is not None:
            DatabaseConnection._instance.engine.dispose()
            DatabaseConnection._instance = None  # Reseta a instância para forçar a reconexão
            DatabaseConnection._last_db_name = None  # Reseta o controle do nome do banco


def query_to_dataframe(queryString) -> pd.DataFrame:
    try:
        #initial_connections = DatabaseConnection.get_active_connections()
        engine = DatabaseConnection.get_engine()
        with engine.connect() as conn:
            df = pd.read_sql_query(queryString, con=conn)
            #final_connections = DatabaseConnection.get_active_connections()
            return df
    except Exception as e:
        DatabaseConnection.dispose_engine()
        print(f"ERROR(connect_db.query_to_dataframe) - Error query_to_dataframe: {e}")
        return None


def insert_data_into_db(data, table_name,return_last_id=False):
    try:
        #initial_connections = DatabaseConnection.get_active_connections()
        engine = DatabaseConnection.get_engine()
        with engine.connect() as conn:
            if isinstance(data, pd.DataFrame):
                data.to_sql(name=table_name, con=conn, if_exists='append', index=False)
            else:
                print("ERROR(connect_db.query_to_dataframe): Data type not supported. 'data' must be a DataFrame")
            #final_connections = DatabaseConnection.get_active_connections()

            if return_last_id:
                result = conn.execute(text("SELECT LAST_INSERT_ID();"))
                last_id = result.scalar()
                return last_id
            
    except Exception as e:
        DatabaseConnection.dispose_engine()
        print(f"ERROR(connect_db.query_to_dataframe): Error insert_data_into_db: {e}")

def insert_multiple_data_into_db(data_list):
    """
    Insert multiple DataFrames into their respective tables in the database.
    
    Parameters:
    data_list (list of tuples): A list where each tuple contains a DataFrame and the corresponding table name.
    
    Example of data_list:
    [
        (DataFrame1, 'table_name1'),
        (DataFrame2, 'table_name2'),
        ...
    ]
    """
    try:
        engine = DatabaseConnection.get_engine()
        with engine.connect() as conn:
            for data, table_name in data_list:
                if isinstance(data, pd.DataFrame):
                    data.to_sql(name=table_name, con=conn, if_exists='append', index=False)
                else:
                    print("ERROR: Data type not supported. 'data' must be a DataFrame.")
    except Exception as e:
        DatabaseConnection.dispose_engine()
        print(f"ERROR: Error in insert_multiple_data_into_db: {e}")

def execute_query_db(query):
    try:
        engine = DatabaseConnection.get_engine()
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text(query))
    except Exception as e:
        DatabaseConnection.dispose_engine()
        print(f"ERROR(connect_db.query_to_dataframe): Erro ao executar a consulta: {e}")

def execute_queries_db(queries):
    try:
        engine = DatabaseConnection.get_engine()
        with engine.connect() as conn:
            with conn.begin() as trans:
                for query in queries:
                    conn.execute(text(query))
                trans.commit()
    except Exception as e:
        DatabaseConnection.dispose_engine()
        print(f"ERROR(connect_db.query_to_dataframe): Erro ao executar a consulta: {e}")

def get_table_columns(table_name):
    try:
        engine = DatabaseConnection.get_engine()
        with engine.connect() as conn:
            query = f"SHOW COLUMNS FROM {table_name};"
            result = conn.execute(text(query))
            # Extrai os nomes das colunas
            columns = [row[0] for row in result]
            return columns
    except Exception as e:
        DatabaseConnection.dispose_engine()
        print(f"Error get_table_columns: {e}")
        return None

def get_last_insert_id():
    try:
        engine = DatabaseConnection.get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT LAST_INSERT_ID();"))
            last_id = result.scalar()
            return last_id
    except Exception as e:
        DatabaseConnection.dispose_engine()
        print(f"ERROR: Error getting last insert ID: {e}")
        return None 
    
def get_db_mmigration(schema_name):
    try:
        engine = DatabaseConnection.get_engine()
        with engine.connect() as conn:
            # Buscar todas as tabelas do schema
            query_tables = f"""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema_name}';
            """
            tables_result = conn.execute(text(query_tables))
            tables = [row[0] for row in tables_result]

            # Dicionário para armazenar os resultados
            schema_info = {}

            # Buscar detalhes das colunas para cada tabela
            for table_name in tables:
                query_columns = f"""
                SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_KEY
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}';
                """
                columns_result = conn.execute(text(query_columns))
                table_info = {}

                for row in columns_result:
                    column_name, column_type, column_key = row
                    is_primary_key = column_key == 'PRI'
                    is_foreign_key = column_key == 'MUL'  # Pode exigir verificação adicional para precisão

                    # Verificação adicional para chaves estrangeiras (opcional)
                    # Este passo pode ser adicionado para confirmar se 'MUL' realmente indica uma FK

                    table_info[column_name] = (column_type, is_primary_key, is_foreign_key)

                schema_info[table_name] = table_info

            return schema_info
    except Exception as e:
        DatabaseConnection.dispose_engine()
        print(f"Error getting schema table columns: {e}")
        return None