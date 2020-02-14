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
        elif not self.nullable:
            res = f'{res} NOT NULL'
        return res

    def __get__(self, obj, obj_type=None):
        if self.primary_key:
            return obj.obj_id

        pkey_name = obj_type.get_primary_key_column().col_name
        query = f'SELECT {self.col_name} from "{self.table_name}" WHERE {pkey_name}=\'{obj.obj_id}\';'
        return obj_type.execute(query)[0][0]

    def __set__(self, obj, value):
        pkey_name = obj.__class__.get_primary_key_column().col_name
        query = f'UPDATE "{self.table_name}" SET {self.col_name} = \'{value}\' WHERE {pkey_name}=\'{obj.obj_id}\''
        obj.__class__.execute(query)

        if self.primary_key:
            obj.obj_id = value

    def update_info(self, col_name, table_name):
        self.col_name = col_name
        self.table_name = table_name


class IntegerColumn(Column):
    __literal__ = 'INTEGER'

    def __str__(self):
        res = 'SERIAL' if self.primary_key else IntegerColumn.__literal__
        if not self.nullable:
            res = f'{res} NOT NULL'
        return res


class StringColumn(Column):
    __literal__ = 'VARCHAR(20)'
