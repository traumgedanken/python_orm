import re

from exc import PrimaryKeyError, TableNameError


class ModelMeta(type):
    __tname_regexp = re.compile('[a-zA-Z_]+')

    def __new__(mcs, name, bases, attrs):
        cls_obj = super().__new__(mcs, name, bases, attrs)
        if name == 'Model':
            return cls_obj

        ModelMeta.__check_table_name(cls_obj, name)
        ModelMeta.__check_primary_key(cls_obj, name)
        ModelMeta.__update_columns_info(cls_obj)

        return cls_obj

    @staticmethod
    def __check_primary_key(cls_obj, name):
        """Check if primary key is specified"""
        primary_key = cls_obj.get_primary_key_column()
        if not primary_key:
            raise PrimaryKeyError(f'No primary key is specified for class `{name}`')

    @staticmethod
    def __check_table_name(cls_obj, name):
        """Validate specified table name"""
        if not hasattr(cls_obj, '__tablename__'):
            raise TableNameError(f'No table name is specified for class `{name}`')

        if not ModelMeta.__tname_regexp.match(cls_obj.__tablename__):
            raise TableNameError(f'Invalid table name is specified for class `{name}`')

    @staticmethod
    def __update_columns_info(cls_obj):
        """Update column objects to ad into then info about column and table names"""
        table_name = cls_obj.__tablename__
        columns = cls_obj.get_all_columns()
        for col_name, col_obj in columns.items():
            col_obj.update_info(col_name, table_name)
