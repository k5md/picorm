import pickle
from collections import OrderedDict
import typing
from functools import reduce

from nanorm.Storage import Storage 

class FileStorage(Storage):
    # name, field type in db, transform function
    types = { 
        'int': ('int', '', int), 
        'str': ('str', '', str),
    }

    @staticmethod
    def _read(file_path):
        try:
            with open(file_path, 'rb') as f:
                data = pickle.load(f, encoding='utf-8')
                return data
        except FileNotFoundError:
            return {}
    
    @staticmethod
    def _write(data, file_path):
        with open(file_path, 'wb') as f:
            data = pickle.dump(data, f)
            return data
    
    def _transform(self, name, entry: OrderedDict):
        return OrderedDict((k, self.schemata[name][k][2](v)) for k, v in entry.items())

    def __init__(self, db_path, log=print):
        self.db_path = db_path
        self.connect(db_path)
        super().__init__(log)

    def connect(self, db_path):
        self.conn = FileStorage._read(db_path)

    def disconnect(self):
        FileStorage._write(self.conn, self.db_path)
        self.conn = None

    def create(self, name, schema: OrderedDict, log = lambda *args: None):
        self.conn = self.conn or OrderedDict()
        self.schemata[name] = schema
        if (name in self.conn):
            return
        self.conn[name] = []
        log('CREATE', name, schema)
        FileStorage._write(self.conn, self.db_path)
    
    def insert(self, name, fields: OrderedDict, log = lambda *args: None):
        if (name not in self.conn):
            return
        transformed_fields = self._transform(name, fields)
        self.conn[name].append(transformed_fields)
        log('INSERT', name, fields, transformed_fields)
        FileStorage._write(self.conn, self.db_path)

    def update(self, name, fields: typing.Union[dict, OrderedDict], selector: typing.Union[dict, OrderedDict], log = lambda *args: None):
        if (name not in self.conn):
            return
        transformed_fields = self._transform(name, fields)
        transformed_selector = self._transform(name, selector)
        predicate = lambda entry: all([entry[k] == v for k, v in transformed_selector.items()])
        reports = []
        matches = (index for index, entry in enumerate(self.conn[name]) if predicate(entry))
        for index in matches:
            report = []
            report.append('BEFORE: {}'.format(self.conn[name][index]))
            for k, v in transformed_fields.items():
                self.conn[name][index][k] = v
            report.append('AFTER: {}'.format(self.conn[name][index]))
            reports.append(' '.join(report))
        log('UPDATE', name, fields, transformed_fields, selector, transformed_selector, ', '.join(reports))
        FileStorage._write(self.conn, self.db_path)

    def delete(self, name, selector: typing.Union[dict, OrderedDict], log = lambda *args: None):
        if (name not in self.conn):
            return
        transformed_selector = self._transform(name, selector)
        predicate = lambda entry: all([entry[k] == v for k, v in transformed_selector.items()])
        reports = []
        matches = (index for index, entry in enumerate(self.conn[name]) if predicate(entry))
        for index in matches:
            del self.conn[name][index]
        log('DELETE', name, selector, transformed_selector, ', '.join(reports))
        FileStorage._write(self.conn, self.db_path)

    def select_one(self, name, selector: typing.Union[dict, OrderedDict] = {}, log = lambda *args: None):
        if (name not in self.conn):
            return None
        transformed_selector = self._transform(name, selector)
        predicate = lambda entry:all([entry[k] == v for k, v in transformed_selector.items()]) if transformed_selector else lambda entry: True
        generator = (entry for entry in self.conn[name] if predicate(entry))
        match = next(generator, None)
        log('SELECT ONE', name, selector, transformed_selector, match)
        return match

    def select_many(self, name, selector: typing.Union[dict, OrderedDict] = {}, log = lambda *args: None):
        if (name not in self.conn):
            return []
        transformed_selector = self._transform(name, selector)
        predicate = lambda entry:all([entry[k] == v for k, v in transformed_selector.items()]) if transformed_selector else lambda entry: True
        generator = (entry for entry in self.conn[name] if predicate(entry))
        matches = list(generator)
        log('SELECT MANY', name, selector, matches)
        return matches

    def select_max(self, name, field: str, log = lambda *args: None):
        if (name not in self.conn):
            return None
        value = reduce(lambda acc, cur: cur if cur[field] > acc[field] else acc, self.conn[name])
        log('SELECT MAX', name, field, value)
        return value
