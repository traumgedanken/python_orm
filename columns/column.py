class Column:
    __literal__ = 'NONE'

    def __init__(self, primary_key=False, nullable=True):
        self.col_name = None
        self.table_name = None
        self.primary_key = primary_key
        self.nullable = False

    def __str__(self):
        res = self.__class__.__literal__
        if self.primary_key:
            res = f'{res} PRIMARY KEY'
        if not self.nullable:
            res = f'{res} NOT NULL'
        return res

    def update_info(self, col_name, table_name):
        self.col_name = col_name
        self.table_name = self.table_name


class IntegerColumn(Column):
    __literal__ = 'INTEGER'
    pass
