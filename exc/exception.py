class PrimaryKeyError(AttributeError):
    pass

class TableNameError(ValueError):
    pass

class UniqueViolationError(ValueError):
    pass
