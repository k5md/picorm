from collections import OrderedDict
import os

import pytest

from nanorm.FileStorage import FileStorage
from nanorm.SQLiteStorage import SQLiteStorage

@pytest.fixture(autouse=True)
def db_path():
    db_path = 'test_storage'
    if os.path.exists(db_path): os.remove(db_path)
    yield db_path
    if os.path.exists(db_path): os.remove(db_path)

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
