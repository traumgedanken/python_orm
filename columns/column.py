import abc

from columns.expression import Expression


class Column(metaclass=abc.ABCMeta):
    """Class to represent table column in ORM class object"""
    
    def __init__(self, primary_key=False, nullable=True):
        self.type = None
        self.col_name = None
        self.table_name = None
        self.primary_key = primary_key
        self.nullable = nullable

    def __str__(self):
        """Is used in table creation scripts"""
        res = f'{self.col_name} {self.type}'
        if self.primary_key:
            res = f'{res} PRIMARY KEY'
        elif not self.nullable:
            res = f'{res} NOT NULL'
        return res

    def __get__(self, obj, obj_type=None):
        if not obj:
            # if we get from cls object
            return self

        if self.primary_key:
            # if we get primary key column value we don't need to make SQL query
            return obj.obj_id

        pkey_col = obj_type.get_primary_key_column()
        pkey_name = pkey_col.col_name
        obj_id = pkey_col.sanitize_value(obj.obj_id)
        tname = self.table_name
        cname = self.col_name
        query = f'SELECT {cname} FROM "{tname}" WHERE "{tname}".{pkey_name}={obj_id};'
        return obj_type.execute(query)[0][0]

    def __set__(self, obj, value):
        pkey_col = obj.__class__.get_primary_key_column()
        pkey_name = pkey_col.col_name
        obj_id = pkey_col.sanitize_value(obj.obj_id)
        tname = self.table_name
        cname = self.col_name
        value = self.sanitize_value(value)
        query = f'UPDATE "{tname}" SET {cname} = {value} WHERE "{tname}".{pkey_name}={obj_id};'
        obj.__class__.execute(query)

        if self.primary_key:
            obj.obj_id = value

    def update_info(self, col_name, table_name):
        self.col_name = col_name
        self.table_name = table_name

    def __eq__(self, other):
        return Expression(self, '=', other)

    def __ne__(self, other):
        return Expression(self, '<>', other)

    def __lt__(self, other):
        return Expression(self, '<', other)

    def __gt__(self, other):
        return Expression(self, '>', other)

    def __le__(self, other):
        return Expression(self, '<=', other)

    def __ge__(self, other):
        return Expression(self, '>=', other)

    @abc.abstractmethod
    def sanitize_value(self, obj):
        """This methods transform obj value to appropriate SQL value"""
        pass


class IntegerColumn(Column):
    def __init__(self, length=20, primary_key=False, nullable=True):
        super().__init__(primary_key, nullable)
        self.type = 'INTEGER'

    def __str__(self):
        res = 'SERIAL' if self.primary_key else self.type
        res = f"{self.col_name} {res}"
        if not self.nullable:
            print(self.nullable, self.col_name)
            input()
            res = f'{res} NOT NULL'
        return res

    def sanitize_value(self, obj):
        return f'{int(obj)}'


class StringColumn(Column):
    def __init__(self, primary_key=False, nullable=True, length=20):
        super().__init__(primary_key, nullable)
        self.type = f'VARCHAR({length})'

    def sanitize_value(self, obj):
        value = str(obj).replace("'", "\\'")
        return f"'{value}'"
