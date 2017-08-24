from os.path import expanduser
import subprocess
import json
from sqlalchemy import create_engine

from .params import __SQLCell_GLOBAL_VARS__
from ..engines.engines import __ENGINES_JSON__

def declare_engines(cell, mode='new', **kwargs):
    home = expanduser("~")
    filepath = home + '/.ipython/profile_default/startup/SQLCell/engines/engines.py'
    engines_json = {} if mode == 'new' else __ENGINES_JSON__
    for n,i in enumerate(cell.split('\n')):
        eng = i.split('=')
        name, conn = str(eng[0]), str(eng[1])
        engines_json[name] = {
            'engine': conn,
            'caution_level': 'warning',
            'order': n
        }
    with open(filepath, 'w') as f:
        f.write(
            'import os\nimport json\n\n\n__ENGINES_JSON__ = {0}\n\n__ENGINES_JSON_DUMPS__ = json.dumps(__ENGINES_JSON__)'.format(engines_json)
        )
    __SQLCell_GLOBAL_VARS__.__ENGINES_JSON_DUMPS__ = json.dumps(engines_json)
    print 'new engines created'
    return ''

def pg_dump(cell, **kwargs):
    conn_str = create_engine(__SQLCell_GLOBAL_VARS__.ENGINE).url
    args = cell.strip().split(' ')
    if not cell.startswith('-') and ">" not in cell:
        pg_dump_cmds = ['pg_dump', '-t', args[0], args[1], '--schema-only', '-h', conn_str.host, '-U', conn_str.username]
    elif ">" in cell:
        pg_dump_cmds = ['pg_dump'] + map(lambda x: str.replace(str(x), ">", "-f"), args)
    else:
        pg_dump_cmds = ['pg_dump'] + args + ['-h', conn_str.host, '-U', conn_str.username, '-W']
    p = subprocess.Popen(
        pg_dump_cmds, 
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    p.stdin.write(conn_str.password)
    p.stdin.flush()
    stdout, stderr = p.communicate()
    rc = p.returncode
    if not stdout and stderr: 
        raise Exception(stderr)
    return stdout

def eval_flag(flag):
    flags = {
        'declare_engines': declare_engines,
        'pg_dump': pg_dump
    }
    return flags[flag]