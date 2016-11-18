import os

# this will create a group of bootstrap-style buttons that will
# allow the user to change engines with the click of a button!

__ENGINES_JSON__ = {
    "LOCAL": {
        "engine": os.getenv('LOCAL'),
        "caution_level": "warning",
        "order": 0
    },
    "DEV": {
        "engine": os.getenv('DEV'),
        "caution_level": "warning",
        "order": 1
    },
    "PROD": {
        "engine": os.getenv('PROD'),
        "caution_level": "danger",
        "order": 2
    },
    "PRODALL": {
        "engine": os.getenv('PRODALL'),
        "caution_level": "danger",
        "order": 3
    },
    "DATALAKE":{
        "engine": os.getenv('DATALAKE'),
        "caution_level": "danger",
        "order": 4
    }
}
