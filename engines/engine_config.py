# this will create a group of bootstrap-style buttons that will
# allow the user to change engines with the click of a button!

__ENGINES_JSON__ = {
    "LOCAL": {
        "engine": "engine_url_goes_here", # engine
        "caution_level": "warning", # bootstrap btn label ("warning"=yello, "danger"=red
        "order": 0 # buttons are grouped by order asc
    },
    "DEV": {
        "engine": "engine_url_goes_here",
        "caution_level": "warning",
        "order": 1
    },
    "PROD": {
        "engine": "engine_url_goes_here",
        "caution_level": "danger", # "danger" for those databases that your boss said not to touch but you can't help yourself
        "order": 2
    }
}