from collections import OrderedDict
import os

import pytest

from picorm import FileStorage
from picorm import SQLiteStorage

@pytest.fixture(autouse=True)
def db_path():
    db_path = 'test_storage'
    backup_postfix = '_backup'
    if os.path.exists(db_path): os.remove(db_path)
    if os.path.exists(db_path + backup_postfix): os.remove(db_path + backup_postfix)
    yield db_path
    if os.path.exists(db_path): os.remove(db_path)
    if os.path.exists(db_path + backup_postfix): os.remove(db_path + backup_postfix)

@pytest.mark.parametrize('Storage', [FileStorage, SQLiteStorage])
def test_storage(db_path, Storage):
    storage = Storage(db_path)

    table_name = 'test'
    storage.create(table_name, OrderedDict([('key', Storage.types['int']), ('foo', Storage.types['str'])]))
    actual = storage.select_one(table_name)
    expected = None
    assert actual is expected

    actual = storage.select_many(table_name)
    expected = []
    assert actual == expected

    expected_a_len = 5
    expected_b_len= 10
    expected_len = expected_a_len + expected_b_len
    for i in range(expected_len):
        storage.insert(table_name, OrderedDict([('key', i), ('foo', 'bar' if i < expected_a_len else 'baz')]))

    actual = storage.select_many(table_name)
    assert len(actual) == expected_len

    actual_a = storage.select_many(table_name, {'foo': 'bar'})
    assert len(actual_a) == expected_a_len

    actual_b = storage.select_many(table_name, {'foo': 'baz'})
    assert len(actual_b) == expected_b_len

    storage.update(table_name, {'foo': 'bar'}, {'foo': 'baz'})
    actual = storage.select_many(table_name, {'foo': 'bar'})
    assert len(actual) == expected_len

    storage.disconnect()

@pytest.mark.parametrize('Storage', [FileStorage, SQLiteStorage])
def test_table_record(db_path, Storage):
    storage = Storage(db_path)

    class Users(storage.Table):
        def __init__(self, fields = OrderedDict()):
            defaults = OrderedDict([('key', 'users')])
            for k, v in fields.items():
                defaults[k] = v
            super().__init__(defaults)
            storage.create('users', OrderedDict([('key', Storage.types['int']), ('lobby_id', Storage.types['int']), ('id', Storage.types['int'])]))
    users = Users()
    class User(users.Record):
        def __init__(self, fields = OrderedDict()):
            defaults = OrderedDict([
                ('key', -1),
                ('lobby_id', -1),
                ('id', -1),
            ])
            for k, v in fields.items():
                defaults[k] = v
            super().__init__(defaults)
    users.Record = User

    test_size = 100
    for i in range(test_size):
        expected = OrderedDict([('key', i), ('lobby_id', -1), ('id', -i)])
        user = User(expected)
        users.add(user)
        actual_a = users.find_one({'key': i})
        actual_b = users.find_one({'id': -i})
        assert actual_a.fields == expected
        assert actual_b.fields == expected
    expected = users.find({})
    assert len(expected) == test_size

    storage.disconnect()
    storage.connect(db_path)
    test_size = 100
    for i in range(test_size):
        expected = OrderedDict([('key', i), ('lobby_id', -1), ('id', -i)])
        actual = users.find_one({'id': -i})
        assert actual.fields == expected
    expected = users.find({})
    assert len(expected) == test_size

    expected = 9000
    user = users.find_one({'key': test_size - 1})
    user.set({'lobby_id': expected})
    actual = user.get('lobby_id')
    assert actual == expected

    user = users.find_one({'key': test_size + 1})
    assert user is None

    storage.disconnect()   

@pytest.mark.parametrize('Storage', [FileStorage, SQLiteStorage])
def test_multiple_tables(db_path, Storage):
    storage = Storage(db_path)

    class Users(storage.Table):
        def __init__(self, fields = OrderedDict()):
            defaults = OrderedDict([('key', 'users')])
            for k, v in fields.items():
                defaults[k] = v
            super().__init__(defaults)
            storage.create('users', OrderedDict([('key', Storage.types['int']), ('lobby_id', Storage.types['int']), ('id', Storage.types['int'])]))
    users = Users()
    class User(users.Record):
        def __init__(self, fields = OrderedDict()):
            defaults = OrderedDict([('key', -1), ('lobby_id', -1), ('id', -1)])
            for k, v in fields.items():
                defaults[k] = v
            super().__init__(defaults)
    users.Record = User

    class Lobbies(storage.Table):
        def __init__(self, fields = OrderedDict()):
            defaults = OrderedDict([('key', 'lobbies')])
            for k, v in fields.items():
                defaults[k] = v
            super().__init__(defaults)
            storage.create('lobbies', OrderedDict([('key', Storage.types['int']), ('lobby_id', Storage.types['int']), ('name', Storage.types['str'])]))
    lobbies = Lobbies()
    class Lobby(lobbies.Record):
        def __init__(self, fields = OrderedDict()):
            defaults = OrderedDict([('key', -1), ('lobby_id', -1), ('name', ':null')])
            for k, v in fields.items():
                defaults[k] = v
            super().__init__(defaults)
    lobbies.Record = Lobby

    test_size = 100
    for i in range(test_size):
        expected_user = OrderedDict([('key', i), ('lobby_id', -1), ('id', -i)])
        expected_lobby = OrderedDict([('key', i), ('lobby_id', -i), ('name', str(-i))])
        user = User(expected_user)
        users.add(user)
        lobby = Lobby(expected_lobby)
        lobbies.add(lobby)
        actual_user = users.find_one({'id': -i})
        actual_lobby = lobbies.find_one({'lobby_id': -i})
        assert actual_user.fields == expected_user
        assert actual_lobby.fields == expected_lobby
    expected = users.find({})
    assert len(expected) == test_size

    storage.disconnect()

@pytest.mark.parametrize('Storage', [FileStorage, SQLiteStorage])
def test_from_readme(db_path, Storage):
    storage = Storage(db_path) # create storage object

    # every collection that you need to store must inherit from storage's Table class
    class Users(storage.Table): 
        def __init__(self, fields = {}):
            defaults = OrderedDict([('key', 'users')]) # specify table name
            for k, v in fields.items():
                defaults[k] = v
            super().__init__(defaults)
            storage.create('users', OrderedDict([ # specify table name and desired fields to be stored
                ('key', storage.types['int']), 
                ('id', storage.types['int']),
                ('name', storage.types['str']),
                ('type', storage.types['str']),
            ]))
            # every query on storage.users will return object of this class if not specified otherwise
            class User(self.Record): 
                def __init__(self, fields = {}):
                    # default values for fields
                    defaults = OrderedDict([
                        ('key', -1), 
                        ('id', -1),
                        ('name', ':null'),
                        ('type', ':null'),
                    ])
                    for k, v in fields.items():
                        defaults[k] = v
                    super().__init__(defaults)
                # that's it, you can add your own methods and properties
            self.User = User # attach classes to table object
    users = Users() # create table object
    users.Record = users.User # make every query return object of class User
    ### Storage querying
    new_user = users.User({'id': 42, 'name': 'foo'}) # create record, you can do it this way or through storage.users.User
    users.add (new_user) # place it in storage
    found_user = users.find_one({'name': 'foo'}) # get storage Record - an object of class User
    found_user.get('id') # get record's field value
    found_user.set({'name': 'bar'}) # change desired fields
    assert found_user.get('id') == new_user.get('id')
    assert found_user.fields == new_user.fields
    assert found_user.get('name') == 'bar' and new_user.get('name') == 'bar'
    storage.disconnect() # disconnect from storage

@pytest.mark.parametrize('Storage', [FileStorage])
def test_backup(db_path, Storage):
    backup_db_path = db_path + '_backup'
    storage = Storage(db_path)
    def makeUsers(storage):
        class Users(storage.Table): 
            def __init__(self, fields = {}):
                defaults = OrderedDict([('key', 'users')])
                for k, v in fields.items():
                    defaults[k] = v
                super().__init__(defaults)
                storage.create('users', OrderedDict([
                    ('key', storage.types['int']), 
                    ('id', storage.types['int']),
                    ('name', storage.types['str']),
                    ('type', storage.types['str']),
                ]))
                class User(self.Record): 
                    def __init__(self, fields = {}):
                        defaults = OrderedDict([
                            ('key', -1), 
                            ('id', -1),
                            ('name', ':null'),
                            ('type', ':null'),
                        ])
                        for k, v in fields.items():
                            defaults[k] = v
                        super().__init__(defaults)
                self.User = User
        return Users
    users = makeUsers(storage)()
    users.Record = users.User
    new_user = users.User({'id': 42, 'name': 'foo'})
    users.add(new_user)
    expected_user_fields = new_user.fields
    storage.disconnect()
    
    open(db_path, 'w').close()
    # backup must be present and seamlessly replace corrupted db
    storage = Storage(db_path)
    users = makeUsers(storage)()
    users.Record = users.User
    found_user = users.find_one({'name': 'foo'})
    assert found_user.fields == expected_user_fields
    # backup must be identical to original after each write
    found_user.set({'name': 'bar'})
    expected_user_fields = found_user.fields
    storage.disconnect()
    storage = Storage(db_path)
    users = makeUsers(storage)()
    users.Record = users.User
    found_user = users.find_one({'name': 'bar'})
    assert found_user.fields == expected_user_fields
    # if backup is corrupted it must be corrected on db connection
    storage.disconnect()
    open(backup_db_path, 'w').close()
    storage = Storage(db_path)
    users = makeUsers(storage)()
    storage.disconnect()
    open(db_path, 'w').close()
    storage = Storage(db_path)
    users = makeUsers(storage)()
    found_user = users.find_one({'name': 'bar'})
    assert found_user.fields == expected_user_fields
    