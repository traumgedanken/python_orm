import logging
import os

import dotenv
import psycopg2

import exc
from models.model_meta import ModelMeta
from columns import Column


def _create_table(func):
    """If table for ORM class is not created it will be created"""
    def wrapper(cls, *args, **kwargs):
        if cls._table_created:
            return func(cls, *args, **kwargs)

        cols = cls.get_all_columns().values()
        cols_str = ', '.join([str(c) for c in cols])
        create_query = f'CREATE TABLE "{cls.__tablename__}" ({cols_str});'
        try:
            cls.execute(create_query)
            logging.debug(f'Table `{cls.__tablename__}` was created successfully')
        except psycopg2.ProgrammingError as e:
            # If given table already exists
            cls.rollback()
            logging.debug(f'Table `{cls.__tablename__}` was not created because it already exists')

        cls._table_created = True
        return func(cls, *args, **kwargs)

    return wrapper


def _no_model_class(func):
    """Wrapper to ensure that method will not be executed in Model class"""
    def wrapper(cls, *args, **kwargs):
        if cls is Model:
            raise NotImplementedError(f'Method `{func.__name__}` is not allowed for Model class')
        return func(cls, *args, **kwargs)
    return wrapper


class Model(metaclass=ModelMeta):
    __connection = None
    _table_created = False

    def __init__(self, obj_id):
        # Every object is represented only by it's primary key
        self.obj_id = obj_id

    def __repr__(self):
        # Connecting to DB to get every column value
        keys = self.__class__.get_all_columns().keys()
        keys_str = ', '.join([f'{key}: {getattr(self, key)}' for key in keys])
        return f'<{self.__class__.__name__}> {keys_str}'

    @staticmethod
    def execute(query):
        """
        Execute given SQL query
        @param query: SQL query
        @return: result with returned rows, None if no rows were returned
        """
        logging.debug(f'Trying to execute SQL query `{query}`')

        cursor = Model.__get_connection().cursor()
        cursor.execute(query)

        try:
            return cursor.fetchall()
        except psycopg2.ProgrammingError as e:
            return None

    @staticmethod
    def rollback():
        Model.__get_connection().rollback()

    @staticmethod
    def commit():
        Model.__get_connection().commit()

    @classmethod
    @_create_table
    @_no_model_class
    def insert(cls, **kwargs):
        """
        @param kwargs: columns values for new object
        @return: created cls object
        """
        cols = cls.get_all_columns()
        keys = ', '.join([f'{key}' for key in kwargs.keys()])
        values = ', '.join([cols[k].sanitize_value(v) for k, v in kwargs.items()])
        pkey_name = cls.get_primary_key_column().col_name
        query = f'INSERT INTO "{cls.__tablename__}"({keys}) VALUES ({values}) RETURNING {pkey_name};'
        try:
            result = Model.execute(query)
            return cls(result[0][0])
        except psycopg2.DatabaseError as e:
            Model.rollback()
            raise exc.SQLExecutionError(str(e))

    @classmethod
    @_create_table
    @_no_model_class
    def get_many(cls, expression=None):
        """
        @param expression: Expression to filter
        @return: generator object with all cls objects
        """
        pkey_name = cls.get_primary_key_column().col_name
        query = f'SELECT {pkey_name} FROM "{cls.__tablename__}"'
        if expression:
            query = f'{query} WHERE {expression};'
        else:
            query = f'{query};'
        rows = cls.execute(query)

        def _generator():
            for row in rows:
                yield cls(row[0])
        return _generator()

    @classmethod
    @_create_table
    @_no_model_class
    def get_one(cls, expression):
        """
        @param expression: Expression to filter
        @return: return cls object that corresponds to first row in response
        """
        pkey_name = cls.get_primary_key_column().col_name
        query = f'SELECT {pkey_name} FROM "{cls.__tablename__}" WHERE {expression} LIMIT 1;'
        rows = cls.execute(query)
        if rows:
            return cls(rows[0][0])
        return None

    @classmethod
    @_create_table
    @_no_model_class
    def delete(cls, expression):
        """
        Delete all rows in table that corresponds to given expression
        @param expression: Expression to filter
        """
        query = f'DELETE FROM "{cls.__tablename__}" WHERE {expression};'
        cls.execute(query)

    @classmethod
    def get_primary_key_column(cls):
        """
        @return: return primary ey column of cls, None if it doesn't exist
        """
        primary_keys = [c for c in cls.__dict__.values()
                        if isinstance(c, Column) and c.primary_key]
        if primary_keys:
            return primary_keys[0]
        return None

    @classmethod
    def get_all_columns(cls):
        """
        @return: dict of column properties of cls
        """
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
