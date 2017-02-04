import importlib

"""
Usage:
>>> from national_voter_file.us_states.all import load
>>> state_modules = load(['ny', 'pa'])

This file should document and list all the states and what we support for each.

It should *not* load any particular state module unless through load()

"""

STATES = [
    'co',
    'fl',
    'mi',
    'nc',
    'ny',
    'ok',
    'pa',
    'wa',
]

SUPPORT = {
    'co':[],
    'fl':[],
    'mi':[],
    'nc':[],
    'ny':[],
    'ok':[],
    'pa':set(['transformer']),
    'wa':[],
}

def load(states=STATES, modules=['transformer']):
    for m in modules:
        #import sub-modules
        [importlib.import_module("national_voter_file.us_states.%s.%s" % (state, m))
         for state in states]
    return [importlib.import_module("national_voter_file.us_states.%s" % (state))
            for state in states]

