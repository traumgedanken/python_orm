import os

import dotenv
import psycopg2

import exc
from models.model_meta import ModelMeta
from columns import Column


def _create_table(func):
    def wrapper(cls, *args, **kwargs):
        if cls._table_created:
            return func(cls, *args, **kwargs)

        cols = cls.get_all_columns()
        cols_str = ', '.join([f'{name} {ops}' for name, ops in cols.items()])
        create_query = f'CREATE TABLE "{cls.__tablename__}" ({cols_str});'
        try:
            cls.execute(create_query)
            cls.commit()
        except psycopg2.ProgrammingError as e:
            cls.rollback()

        cls._table_created = True
        return func(cls, *args, **kwargs)

    return wrapper


class Model(metaclass=ModelMeta):
    __connection = None
    _table_created = False

    def __init__(self, obj_id):
        self.__obj_id = obj_id

    def __repr__(self):
        return f'<{self.__class__.__name__}> id: {self.__obj_id}'

    @staticmethod
    def execute(query):
        cursor = Model.__get_connection().cursor()
        cursor.execute(query)

    @staticmethod
    def rollback():
        Model.__get_connection().rollback()

    @staticmethod
    def commit():
        Model.__get_connection().commit()

    @classmethod
    @_create_table
    def insert(cls, **kwargs):
        keys = ', '.join([f'{key}' for key in kwargs.keys()])
        values = ', '.join([f"'{value}'" for value in kwargs.values()])
        pkey_name = cls.get_primary_key_column().col_name
        query = f'INSERT INTO "{cls.__tablename__}"({keys}) VALUES ({values}) RETURNING {pkey_name};'
        try:
            cursor = Model.__get_connection().cursor()
            cursor.execute(query)
            return cls(cursor.fetchall()[0][0])
        except psycopg2.DatabaseError as e:
            Model.rollback()
            raise exc.UniqueViolationError(str(e))

    @classmethod
    def get_primary_key_column(cls):
        primary_keys = [c for c in cls.__dict__.values()
                        if isinstance(c, Column) and c.primary_key]
        if primary_keys:
            return primary_keys[0]
        return None

    @classmethod
    def get_all_columns(cls):
        return {name: col for name, col in cls.__dict__.items()
                if isinstance(col, Column)}

    @staticmethod
    def __get_connection():
        if Model.__connection:
            return Model.__connection

        dotenv.load_dotenv()
        db_url = os.getenv('DB_URL')
        if not db_url:
            raise ValueError('No database url specified in .env')

        Model.__connection = psycopg2.connect(db_url)
        return Model.__connection
