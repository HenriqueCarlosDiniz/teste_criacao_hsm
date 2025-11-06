from GUIs.database_operations import get_table_columns

class Pagina():
    def __init__(self, name, keys = None):
        self.name = name
        self.columns = get_table_columns(name)
        self.keys = keys