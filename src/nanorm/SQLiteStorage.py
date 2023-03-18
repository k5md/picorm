import sqlite3
from collections import OrderedDict
import typing

from nanorm.Storage import Storage
from nanorm.utils import quote

class SQLiteStorage(Storage):
    # name, field type in db, transform function
    types = { 
        'int': ('int', 'integer', str), 
        'str': ('str', 'text', quote),
    }

    def _transform(self, name, entry: OrderedDict):
        return OrderedDict((k, self.schemata[name][k][2](v)) for k, v in entry.items())

    def __init__(self, db_path, log=print):
        self.db_path = db_path
        self.connect(db_path)
        super().__init__(log)

    def connect(self, db_path):
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.execute('pragma journal_mode=wal;')
        self.conn = conn

    def disconnect(self):
        self.conn.close()

    def create(self, name, schema: OrderedDict, log = lambda *args: None):
        c = self.conn.cursor()
        query = 'CREATE TABLE IF NOT EXISTS {} ({})'.format(
            name,
            ','.join(' '.join([key, entry[1]]) for key, entry in schema.items()),
        )
        if (name in self.schemata):
            return (self.schemata[name], 'already exists')
        self.schemata[name] = schema
        log(query)
        c.execute(query)
        self.conn.commit()
    
    def insert(self, name, fields: OrderedDict, log = lambda *args: None):
        c = self.conn.cursor()
        query = 'INSERT INTO {}({}) VALUES({})'.format(
            name,
            ','.join(fields.keys()),
            ','.join(self._transform(name, fields).values()),
        )
        log(query)
        c.execute(query)
        self.conn.commit()

    def update(self, name, fields: typing.Union[dict, OrderedDict], selector: typing.Union[dict, OrderedDict], log = lambda *args: None):
        c = self.conn.cursor()
        query = 'UPDATE {} SET {} WHERE {}'.format(
            name,
            ','.join('{}={}'.format(k, v) for k, v in self._transform(name, fields).items()),
            ','.join('{}={}'.format(k, v) for k, v in self._transform(name, selector).items()),
        )
        log(query)
        c.execute(query)
        self.conn.commit()

    def delete(self, name, selector: typing.Union[dict, OrderedDict], log = lambda *args: None):
        c = self.conn.cursor()
        query = 'DELETE FROM {} WHERE {}'.format(
            name,
            ','.join('{}={}'.format(k, v) for k, v in self._transform(name, selector).items()),
        )
        log(query)
        c.execute(query)
        self.conn.commit()

    def select_one(self, name, selector: typing.Union[dict, OrderedDict, None] = None, log = lambda *args: None):
        c = self.conn.cursor()
        query = 'SELECT * FROM {} WHERE {} LIMIT 1'.format(
            name,
            ','.join('{}={}'.format(k, v) for k, v in self._transform(name, selector).items()),
        ) if selector else 'SELECT * FROM {} LIMIT 1'.format(name)
        log(query)
        c.execute(query)
        values = c.fetchone()
        return OrderedDict(zip(self.schemata[name].keys(), values)) if values else None

    def select_many(self, name, selector: typing.Union[dict, OrderedDict, None] = None, log = lambda *args: None):
        c = self.conn.cursor()
        query = 'SELECT * FROM {} WHERE {}'.format(
            name,
            ','.join('{}={}'.format(k, v) for k, v in self._transform(name, selector).items()),
        ) if selector else 'SELECT * FROM {}'.format(name)
        log(query)
        c.execute(query)
        entries = c.fetchall()
        return [OrderedDict(zip(self.schemata[name].keys(), values)) for values in entries]
    
    def select_max(self, name, field: str, log = lambda *args: None):
        c = self.conn.cursor()
        query = 'SELECT * FROM {} WHERE {} = (SELECT MAX({}) FROM {})'.format(name, field, field, name)
        log(query)
        c.execute(query)
        value = c.fetchone()
        return value
