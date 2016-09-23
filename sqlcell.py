import re
import fileinput
import time
import uuid
import json
from os.path import expanduser
from IPython.core.magic import (register_line_magic, register_cell_magic,
                                register_line_cell_magic)
import IPython
from IPython.display import Javascript
from IPython.core.display import display, HTML
from sqlalchemy import create_engine
from engine_config import driver, username, password, host, port, default_db
from engines import __ENGINES_JSON__


unique_db_id = str(uuid.uuid4())
application_name = '?application_name=jupyter' + unique_db_id

for k,v in __ENGINES_JSON__.iteritems():
    exec(k+'="'+v['engine']+'"')

__ENGINES_JSON_DUMPS__ = json.dumps(__ENGINES_JSON__)

engine = create_engine(driver+'://'+username+':'+password+'@'+host+':'+port+'/'+default_db+application_name)

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
    pd.set_option('display.max_colwidth', -1)
#     pd.set_option('display.max_rows', 500)
    to_table = pd.DataFrame
except ImportError as e:
    to_table = HTMLTable

def timer(func):
    import time
    def wrapper(*args, **kwargs):
        t0 = time.time()
        output = func(*args, **kwargs)
        t1 = time.time() - t0
        print("Time to run: {t1:f} ms".format(t1=t1*1000))
        return output
    return wrapper

def build_dict(output, row):
    output[row.replace('%(','').replace(')s','')] = eval(row.replace('%(','').replace(')s',''))
    return output

# @timer
def _SQL(path, cell):
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
    global driver, username, password, host, port, db, table, __EXPLAIN__, __GETDATA__, __SAVEDATA__, engine, PATH
    
    unique_id = str(uuid.uuid4())

    display(
        HTML(
            '''
            <div class="row">
                <div class="btn-group col-md-3">
                    <button id="explain" title="Explain Analyze" onclick="explain()" type="button" class="btn btn-info btn-sm"><p class="fa fa-info-circle"</p></button>
                    <button type="button" title="Execute" onclick="run()" class="btn btn-success btn-sm"><p class="fa fa-play"></p></button>
                    <button type="button" title="Execute and Return Data as Variable" onclick="getData()" class="btn btn-success btn-sm"><p class="">var</p></button>
                    <button id="saveData'''+unique_id+'''" title="Save" class="btn btn-success btn-sm disabled" type="button"><p class="fa fa-save"</p></button>
                    <button id="cancelQuery'''+unique_id+'''" title="Cancel Query" class="btn btn-danger btn-sm" type="button"><p class="fa fa-stop"</p></button>
                </div>
                <div id="engineButtons'''+unique_id+'''" class="btn-group col-md-5"></div>
            </div>
            <script type="text/Javascript">
            
                var engines = JSON.parse(`'''+str(__ENGINES_JSON_DUMPS__)+'''`);
                
                var sortedEngineKeys = Object.keys(engines).sort(function(a,b){
                    return engines[a].order - engines[b].order;
                });
                
                var sortedEngines = sortedEngineKeys.reduce(function(output, row, idx){
                    engines[row]['key'] = row;
                    output.push(engines[row]);
                    return output;
                }, []);
                
                var engineButtons = '';
                for (var engine in sortedEngines){
                    var engineKey = sortedEngines[engine].key;
                    var warningLabel = sortedEngines[engine].caution_level;
                    engineButtons += '<button title="Switch Engine" onclick="switchEngines('+"'"+engineKey+"'"+')" class="btn btn-'+warningLabel+' btn-sm">'+engineKey+'</button>';
                };
                
               $("#engineButtons'''+unique_id+'''").append(engineButtons);
               
               $("#cancelQuery'''+unique_id+'''").on('click', function(){
                   if ($("#cancelQuery'''+unique_id+'''").hasClass('disabled')){
                       console.log('alread stopped or finished query...');
                   } else {
                       cancelQuery("'''+unique_db_id+'''");
                       //$("#cancelQuery'''+unique_id+'''").addClass('disabled')
                   }
               });
            
                function explain(){
                    var command =  `global __EXPLAIN__\n__EXPLAIN__ = True`;
                    var kernel = IPython.notebook.kernel;
                    kernel.execute(command);
                    IPython.notebook.execute_cell();
                };
                
                function run(){
                    IPython.notebook.execute_cell();
                    $.get('/api/contents', function(data){
                        console.log(data);
                    });
                };
                
                function cancelQuery(applicationID){
                    $.get('/api/contents?kill_last_postgres_process=jupyter'+applicationID+'&engine=localhost', function(d){
                        console.log(d);
                    });
                };
                
                function getData(){
                    var command = `global __GETDATA__ \n__GETDATA__ = True`;
                    var kernel = IPython.notebook.kernel;
                    kernel.execute(command);
                    IPython.notebook.execute_cell();
                };
                
                function saveData(data, filename){
                    var path = $('#path').val();
                    var command = `global __SAVEDATA__ \n__SAVEDATA__, PATH = True,'`+path+`'`;
                    var kernel = IPython.notebook.kernel;
                    kernel.execute(command);
                    //IPython.notebook.execute_cell();
                    
                    function download(data, filename, type) {
                        var a = document.createElement("a"),
                            file = new Blob([data], {type: type});
                        if (window.navigator.msSaveOrOpenBlob) // IE10+
                            window.navigator.msSaveOrOpenBlob(file, filename);
                        else { // Others
                            var url = URL.createObjectURL(file);
                            a.href = url;
                            a.download = filename;
                            document.body.appendChild(a);
                            a.click();
                            setTimeout(function() {
                                document.body.removeChild(a);
                                window.URL.revokeObjectURL(url);  
                            }, 0); 
                        }
                    }
                    
                    download(data, filename, 'csv');
                    
                };
                
                function switchEngines(engine){
                    var command = `global ENGINE \nENGINE = eval('` + engine + `')`;
                    var kernel = IPython.notebook.kernel;
                    kernel.execute(command);
                    IPython.notebook.execute_cell();
                };
                
            </script>
            '''
        )
    )
    
    if '__EXPLAIN__' in globals() and __EXPLAIN__:
        cell = 'EXPLAIN ANALYZE ' + cell
        __EXPLAIN__ = False
        
    elif '__GETDATA__' in globals() and __GETDATA__:
        if 'MAKE_GLOBAL' not in path:
            path = 'MAKE_GLOBAL=DATA RAW=True ' + path.strip()
            print 'data available as DATA'
        __GETDATA__ = False
    
    elif '__SAVEDATA__' in globals() and __SAVEDATA__:
        path = 'PATH="'+PATH+'" '+path
        __SAVEDATA__ = PATH = False
        
    elif 'ENGINE' in globals() and ENGINE:
        engine = create_engine(ENGINE+application_name)
    
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
            ORDER BY "Type" desc;
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
            exec("global engine\nengine=create_engine('"+driver+"://"+username+":"+password+"@"+host+":"+port+"/"+db+application_name+"')")
            exec('global DB\nDB=db')

            home = expanduser("~")
            filepath = home + '/.ipython/profile_default/startup/ac_engine_config.py'

            for line in fileinput.FileInput(filepath,inplace=1):
                line = re.sub("default_db = '.*'","default_db = '"+db+"'", line)
                print line,

        elif i.startswith('ENGINE'):
            exec("global ENGINE\nENGINE="+i.replace('ENGINE=', ""))
            if ENGINE != str(engine.url):
                exec("global engine\nengine=create_engine("+i.replace('ENGINE=', "")+application_name+")")
                conn_str = engine.url
                driver, username = conn_str.drivername, conn_str.username
                password, host = conn_str.password, conn_str.host
                port, db = conn_str.port, conn_str.database

        else:
            exec(i)

    matches = re.findall(r'%\([a-zA-Z_][a-zA-Z0-9_]*\)s', cell)
    t0 = time.time()
    data = engine.execute(cell, reduce(build_dict, matches, {}))
    t1 = time.time() - t0
    t2 = time.time()
    columns = data.keys()
    table_data = [i for i in data] if 'pd' in globals() else [columns] + [i for i in data]
    df = to_table(table_data)
    data.close()

    if df.empty:
        return 'No data available'

    df.columns = columns

    if 'PATH' in globals() and PATH:
        try:
            df.to_csv(PATH)
        except IOError as e:
            print 'ATTENTION:', e
            return None

    if 'MAKE_GLOBAL' in locals():
        exec('global ' + glovar[1] + '\n' + glovar[1] + '=df if \'RAW\' not in locals() else table_data')
        
    data = df.values.tolist()
    str_data = '\t'.join(columns) + '\n'
    for d in data:
        str_data += '\t'.join([str(i) for i in d]) + '\n'
    
    display(
        Javascript(
            """
                $('#saveData"""+unique_id+"""').removeClass('disabled');
                $("#cancelQuery"""+unique_id+"""").addClass('disabled')

                $('#saveData"""+unique_id+"""').on('click', function(){
                    if (!$(this).hasClass('disabled')){
                        saveData(`"""+str_data+"""`, 'test.tsv');
                    }
                });
            """
        )
    )
    
    t3 = time.time() - t2
    print 'Time to execute: ' + str(t1*1000) + ' ms', ' | ', 'Time to render: ' + str(t3*1000) + ' ms', ' | ', 'Engine:', str(engine.url)
    return df.replace(to_replace={'QUERY PLAN': {' ': '-'}}, regex=True)


@register_line_cell_magic
def sql(path, cell):
    return _SQL(path, cell)


js = "IPython.CodeCell.config_defaults.highlight_modes['magic_sql'] = {'reg':[/^%%sql/]};"
IPython.core.display.display_javascript(js, raw=True)
