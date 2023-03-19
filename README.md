# picorm
[![Python 3.9](https://img.shields.io/badge/python-3.9-blue.svg)](https://www.python.org/downloads/release/python-390/) 

Picorm is a small ORM with limited functions for multiple database engines for pet-projects, mostly for simple CRUD operations in cases, when you don't want to introduce a solid ORM like SQLAlchemy or whatever, but still want to keep storage-related stuff out of your code.

## Getting Started
### Installation
To install from [pypi](https://pypi.org/project/picorm/):
```
pip install picorm
```

### Simple usage
```python
from picorm import FileStorage as Storage # import one of storage implementations

storage = Storage('path_to_database_file') # create storage object

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
```
### Storage querying
```python
new_user = users.User({'id': 42, 'name': 'foo'}) # create record, you can do it this way or through storage.users.User
users.add (new_user) # place it in storage
found_user = users.find_one({'name': 'foo'}) # get storage Record - an object of class User
found_user.get('id') # get record's field value
found_user.set({'name': 'bar'}) # change desired fields
storage.disconnect() # don't forget to disconnect from storage if you're planning to switch engines
```

### Return different subclasses of table record
In those cases, when you need queries to return various subclasses of table record, you can simply introduce them after User definition in example:
```python
class Users(storage.Table): 
    def __init__(self, fields = {}):
        # ...
        class Player(User): # an example of subclassing table Record
            type = 'player'
            def __init__(self, fields):
                overrides = {'type': Player.type}
                merged = {**fields, **overrides}
                super().__init__(merged)
        
        # ...
        self.Player = Player # attach class to table object
        
        # create a type -> class map to return objects of different subclasses of table record
        self.type_class_map = { 
            Player.type: Player,
            User.type: User,
            ':null': User, # "default" class to wrap record
        }

# make every query return object of either Player or User class based on "type" field value
users.Record = lambda fields: users.type_class_map[fields['type']](fields)
```

### Table methods 
| method   | arguments               | returns              |
| :------  | :---------------------- | :------------------- |
| create   |self, name, schema, log  |None                  |
| find     |self, selector           |list of Record objects|
| find_one |self, selector           |Record object or None |
| find_max |self, key                |Record object or None |
| add      |self, record             |None                  |
| remove   |self, record             |None                  |

### Record operations
| method  | arguments    | returns             |
| :------ | :----------- | :------------------ |
| set     |self, fields  |None                 |
| get     |self, key     |Record's field value |

### Supported engines
- File storage
- SQLite storage

## Development
### Environment setup
1.  Install Python 3.9+
2.  Install `virtualenv`
    ```sh
    pip install virtualenv
    ```
3.  Clone this project
4.  From project directory, run
    ```sh
    virtualenv .env
    ```
    **Note**: This will create a virtual environment using the Python version
    that `virtualenv` was run with (which will be the version it was installed
    with). To use a specific Python version, run:
    ```sh
    virtualenv --python=<path_to_other_python_version> .env
    # For example, this might look like
    virtualenv --python=/usr/bin/python3.6 .env
    ```
5.  Assuming you are using the `bash` shell, run:
    ```sh
    source .env/bin/activate
    ```
    For other shells, see the other `activate.*` scripts in the `.env/bin/`
    directory. If you are on Windows, run:
    ```sh
    .env\Scripts\activate.bat
    ```
6.  Install all of the required packages using
    ```sh
    pip install -r requirements.txt
    ```

### Testing
This project uses tox -> pytest. To trigger tests either run ```tox``` or ```pytest``` optionally with -vv and -s flags for verbosity and prints.

### Packaging module
Run the following command to package picorm module:
```sh
python -m pip install --upgrade build
python -m build
```
Generated archive and .whl package will be placed in **dist** directory.

## Contributions
PR are always welcome!
