# conftest.py
import sys

def t(*args, **kwargs):
    return str(args) + str(kwargs)

module = type(sys)('i18n')
module.t = t
sys.modules['i18n'] = module