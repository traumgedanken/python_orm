class PrimaryKeyError(AttributeError):
    pass


class TableNameError(ValueError):
    pass


class SQLExecutionError(ValueError):
    pass
