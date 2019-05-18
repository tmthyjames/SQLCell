import os
import json


### these engines should get removed when you run the 
# --declare_engines new
### flag


__ENGINES_JSON__ = {'LOCAL': {'engine': 'postgresql://tdobbins:password@localhost:5432/', 'caution_level': 'warning', 'order': 0}, 'EC2': {'engine': 'postgresql://tmthyjames:password@ec2-HOST.compute-1.amazonaws.com:5432/', 'caution_level': 'warning', 'order': 1}, 'RDS': {'engine': 'postgresql://tmthyjames:password@HOST.rds.amazonaws.com:5432/', 'caution_level': 'warning', 'order': 0}}

__ENGINES_JSON_DUMPS__ = json.dumps(__ENGINES_JSON__)