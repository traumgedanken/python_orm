class Expression:
    """Class to create valid SQL expressions"""

    def __init__(self, column=None, op_literal=None, value=None, string=None):
        if string:
            self.string = string
        else:
            tname = column.table_name
            cname = column.col_name
            val = column.sanitize_value(value)
            self.string = f'"{tname}".{cname}{op_literal}{val}'

    def __str__(self):
        return self.string

    @staticmethod
    def or_(first, second):
        return Expression(string=f'({first}) OR ({second})')

    @staticmethod
    def and_(first, second):
        return Expression(string=f'({first}) AND ({second})')

    @staticmethod
    def not_(first):
        return Expression(string=f'NOT ({first})')
