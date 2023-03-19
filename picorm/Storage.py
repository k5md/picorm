from collections import OrderedDict
import typing

class Storage():
    """Base storage class, serves as an adapter between storage-specific methods,
    that have to be implemented in subclasses, and interface available to users
    on user-defined subclasses of StorageTable and StorageTableRecord
    """
    def connect(self, db_path): raise NotImplementedError()
    def disconnect(self): raise NotImplementedError()
    def create(self, name, fields: OrderedDict, log = lambda *args: None): raise NotImplementedError()
    def insert(self, name, fields: OrderedDict, log = lambda *args: None): raise NotImplementedError()
    def update(self, name, fields: typing.Union[dict, OrderedDict], selector: typing.Union[dict, OrderedDict], log = lambda *args: None): raise NotImplementedError()
    def delete(self, name, selector: typing.Union[dict, OrderedDict], log = lambda *args: None): raise NotImplementedError()
    def select_one(self, name, selector: typing.Union[dict, OrderedDict], log = lambda *args: None): raise NotImplementedError()
    def select_many(self, name, selector: typing.Union[dict, OrderedDict], log = lambda *args: None): raise NotImplementedError()
    def select_max(self, name, field: str, log = lambda *args: None): raise NotImplementedError()

    def __init__(self, log):
        self.log = log
        self.schemata = {}
        storage_context = self

        class StorageTable():
            def __init__(self, fields):
                self.fields = fields

                storage_table_context = self

                class StorageTableRecord():
                    def __init__(self, fields):
                        self.fields = fields
                        if self.extractor() not in fields or self.extract() == -1:
                            try:
                                max_extractor_record = storage_context.select_max(storage_table_context.extract(), self.extractor(), log)
                                self.fields[self.extractor()] = max_extractor_record[self.extractor()] + 1
                            except Exception:
                                self.fields[self.extractor()] = 0
                    
                    def set(self, fields):
                        for k in self.fields.keys():
                            if not k in fields:
                                continue
                            self.fields[k] = fields[k]
                        storage_context.update(
                            storage_table_context.extract(),
                            self.fields,
                            {self.extractor(): self.extract()},
                            log,
                        )

                    def get(self, key):
                        fields = storage_context.select_one(
                            storage_table_context.extract(),
                            {self.extractor(): self.extract()},
                            log,
                        )
                        for k in self.fields.keys():
                            if not k in fields:
                                continue
                            self.fields[k] = fields[k]
                        return self.fields[key]

                    def extractor(self):
                        return 'key'

                    def extract(self):
                        return self.fields[self.extractor()]

                    def serialize(self):
                        return self.fields

                self.Record = StorageTableRecord

                def add(self, record):
                    storage_context.insert(
                        storage_table_context.extract(),
                        record.serialize(),
                        log,
                    )
                
                def remove(self, record):
                    storage_context.delete(
                        storage_table_context.extract(),
                        record.serialize(),
                        log,
                    )

                self.add = lambda record, context = self: add(self, record)
                self.remove = lambda record, context = self: remove(self, record)

            def find(self, selector):
                results = storage_context.select_many(
                    self.extract(),
                    selector,
                    log,
                )
                return [self.Record(result) for result in results]

            def find_one(self, selector):
                result = storage_context.select_one(
                    self.extract(),
                    selector,
                    log,
                )
                return self.Record(result) if result else None
            
            def find_max(self, key):
                result = storage_context.select_max(
                    self.extract(),
                    key,
                    log,
                )
                return self.Record(result) if result else None

            def extractor(self):
                return 'key'

            def extract(self):
                return self.fields[self.extractor()]

        self.Table = StorageTable
