import re
import fileinput
from os.path import expanduser
from IPython.core.magic import (register_line_magic, register_cell_magic,
                                register_line_cell_magic)
import IPython
from sqlalchemy import create_engine
from engine_config import driver, username, password, host, port, default_db

engine = create_engine(driver+'://'+username+':'+password+'@'+host+':'+port+'/'+default_db)

class HTMLTable(list):
    """
    Creates an HTML table if pandas isn't installed.
    The .empty attribute takes the place of df.empty,
    and to_csv takes the place of df.to_csv.
    """
    
    empty = []
    
    def _repr_html_(self):
        table = '<table width=100%>'
        thead = '<thead><tr>'
        tbody = '<tbody><tr>'
        for n,row in enumerate(self):
            if n == 0:
                thead += ''.join([('<th>' + str(r) + '</th>') for r in row])
            else:
                tbody += '<tr>' + ''.join([('<td>' + str(r) + '</td>') for r in row]) + '</tr>'
        thead += '</tr></thead>'
        tbody += '</tbody>'
        table += thead + tbody
        return table

    def to_csv(self, path):
        import csv
        with open(path, 'w') as fp:
            a = csv.writer(fp, delimiter=',')
            a.writerows(self)
    
try:
    import pandas as pd
    pd.options.display.max_columns = None
    to_table = pd.DataFrame
except ImportError as e:
    to_table = HTMLTable
    

@register_line_cell_magic
def sql(path, cell=None):
    """
    Create magic cell function to treat cell text as SQL
    to remove the need of third party SQL interfaces. The 
    args are split on spaces so don't use spaces except to 
    input a new argument.
    Args:
        PATH (str): path to write dataframe to in csv.
        MAKE_GLOBAL: make dataframe available globally.
        DB: name of database to connect to.
        RAW: when used with MAKE_GLOBAL, will return the
            raw RowProxy from sqlalchemy.
    Returns:
        DataFrame:
    """
    global driver, username, password, host, port, db
    
    if cell.strip() == '\d':
        cell = """
            SELECT n.nspname as "Schema",
                c.relname as "Name",
                CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' END as "Type",
                pg_catalog.pg_get_userbyid(c.relowner) as "Owner",
                pg_catalog.pg_size_pretty(pg_catalog.pg_table_size(c.oid)) as "Size",
                pg_catalog.obj_description(c.oid, 'pg_class') as "Description"
            FROM pg_catalog.pg_class c
                LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r','v','m','S','f','')
                AND n.nspname <> 'pg_catalog'
                AND n.nspname <> 'information_schema'
                AND n.nspname !~ '^pg_toast'
                AND pg_catalog.pg_table_is_visible(c.oid)
            ORDER BY 1,2;
        """
    
    elif cell.startswith("\d"):
        table = cell.replace('\d', '').strip()
        cell = """
            SELECT a.attname,
                pg_catalog.format_type(a.atttypid, a.atttypmod),
                (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
                FROM pg_catalog.pg_attrdef d
                WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
                a.attnotnull, a.attnum,
                (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t
                WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation,
                NULL AS indexdef,
                NULL AS attfdwoptions,
                a.attstorage,
            CASE WHEN a.attstattarget=-1 THEN NULL ELSE a.attstattarget END AS attstattarget, pg_catalog.col_description(a.attrelid, a.attnum)
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c on c.oid = a.attrelid
            WHERE c.relname = %(table)s AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum;
        """
        
    args = path.split(' ')
    for i in args:
        if i.startswith('MAKE_GLOBAL'):
            glovar = i.split('=')
            exec(glovar[0]+'='+glovar[1]+'=None')
        elif i.startswith('DB'):
            db = i.replace('DB=', '') 
            exec("global engine\nengine=create_engine('"+driver+"://"+username+":"+password+"@"+host+":"+port+"/"+db+"')")
            exec('global DB\nDB=db')

            home = expanduser("~")
            filepath = home + '/.ipython/profile_default/startup/engine_config.py'
            
            for line in fileinput.FileInput(filepath,inplace=1):
                line = re.sub("default_db = '.*'","default_db = '"+db+"'", line)
                print line,
                
        elif i.startswith('ENGINE'):
            exec("global engine\nengine=create_engine("+i.replace('ENGINE=', "")+")")
            conn_str = engine.url
            driver, username = conn_str.drivername, conn_str.username
            password, host = conn_str.password, conn_str.host
            port, db = conn_str.port, conn_str.database
            
        else:
            exec(i)

    matches = re.findall(r'%\(.*\)s', cell)
    for m in matches:
        param = eval(m.replace('%(', '').replace(')s', ''))
        quotes = '' if isinstance(param, int) else '\''
        cell = re.sub(re.escape(m), quotes+str(param)+quotes, cell)

    data = engine.execute(cell)
    columns = data.keys()
    table_data = [i for i in data] if 'pd' in globals() else [columns] + [i for i in data]
    df = to_table(table_data)
    
    if df.empty:
        return 'No data available'
    
    df.columns = columns

    if 'PATH' in locals():
        df.to_csv(PATH)

    if 'MAKE_GLOBAL' in locals():
        exec('global ' + glovar[1] + '\n' + glovar[1] + '=df if \'RAW\' not in locals() else table_data')
        
    return df

def send_to_client(data, filename=None, key=None):
    import json
    filename = filename if filename else 'data.json'
    key = key if key else 'data'
    with open('/Users/tdobbins/bidirect/' + filename, 'w') as f:
        f.write(json.dumps(data))
    return None

js = "IPython.CodeCell.config_defaults.highlight_modes['magic_sql'] = {'reg':[/^%%sql/]};"
IPython.core.display.display_javascript(js, raw=True)
