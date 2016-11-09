import re
import fileinput
import time
import uuid
import json
import subprocess
from os.path import expanduser
from IPython.core.magic import (register_line_magic, register_cell_magic,
                                register_line_cell_magic)
import IPython
from IPython.display import Javascript
from IPython.core.display import display, HTML
from sqlalchemy import create_engine, exc
from ac_engine_config import driver, username, password, host, port, default_db
from ae_engines import __ENGINES_JSON__


unique_db_id = str(uuid.uuid4())
application_name = '?application_name=jupyter' + unique_db_id

for k,v in __ENGINES_JSON__.iteritems():
    exec(k+'="'+v['engine']+'"')

__ENGINES_JSON_DUMPS__ = json.dumps(__ENGINES_JSON__)

engine = create_engine(driver+'://'+username+':'+password+'@'+host+':'+port+'/'+default_db+application_name)
EDIT = False

class QUERY(object):
	raw = ''
	history = []

class HTMLTable(list):
    """
    Creates an HTML table if pandas isn't installed.
    The .empty attribute takes the place of df.empty,
    and to_csv takes the place of df.to_csv.
    """

    def __init__(self, data, id_):
        self.id_ = id_
        self.data = data

    empty = []

    def _repr_html_(self):
        table = '<table id="table'+self.id_+'" width=100%>'
        thead = '<thead><tr>'
        tbody = '<tbody>'
        for n,row in enumerate(self.data):
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
            a.writerows(self.data)

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

def update_table(sql):
    engine.execute(sql)
    return None

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
            <style>
            .input { 
                position:relative; 
            }
            #childDiv'''+unique_id+''' { 
                width: 90%;
                position:absolute; 
            }
            #dummy'''+unique_id+''' {
                height:25px;
            }
            </style>
            <div id="dummy'''+unique_id+'''"</div>
            <div class="row" id="childDiv'''+unique_id+'''">
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
                    $.get('/halt_query?kill_last_postgres_process=jupyter'+applicationID+'&engine=localhost', function(d){
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
        engine = create_engine(ENGINE)
    
    psql_command = False
    if cell.startswith('\\'):
        psql_command = True
        pg = ['psql', db, '-c', cell.strip(), '-H']

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
    connection = engine.connect()

    try:
        if not psql_command:
            data = connection.execute(cell, reduce(build_dict, matches, {}))
            columns = data.keys()
            table_data = [i for i in data] if 'pd' in globals() else [columns] + [i for i in data]
            df = to_table(table_data)
        else:
            output = subprocess.check_output(pg)
            data = pd.read_html(output, header=0)[0]
            columns = data.keys()
            table_data = [i for i in data.values.tolist()]
            df = data
    except exc.OperationalError as e:
        print 'Query cancelled...'
        return None
    except exc.ResourceClosedError as e:
        print 'Query ran successfully...'
        return None
    
    t1 = time.time() - t0
    t2 = time.time()

    QUERY.raw = (cell, t1)
    QUERY.history.append((cell, t1))
    
    columns = data.keys()

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
    print 'To execute: ' + str(round(t1, 3)) + ' sec', '|', 
    print 'To render: ' + str(round(t3, 3)) + ' sec', '|', 
    print 'Rows:', len(df.index), '|',
    print 'DB:', engine.url.database, '| Host:', engine.url.host,

    table_name = re.search('from\s*([a-z]{,})', cell, re.IGNORECASE)
    table_name = None if not table_name else table_name.group(1).strip()

    if EDIT:

        primary_key_results = engine.execute("""
                SELECT               
                  %(table_name)s as table_name, pg_attribute.attname as column_name
                FROM pg_index, pg_class, pg_attribute, pg_namespace 
                WHERE 
                  pg_class.oid = %(table_name)s::regclass AND 
                  indrelid = pg_class.oid AND 
                  nspname = 'public' AND 
                  pg_class.relnamespace = pg_namespace.oid AND 
                  pg_attribute.attrelid = pg_class.oid AND 
                  pg_attribute.attnum = any(pg_index.indkey)
                 AND indisprimary
             """, {'table_name': table_name}).first()

        if primary_key_results:
            primary_key = primary_key_results.column_name

            update_dict = None
            if not re.search('join', cell, re.IGNORECASE):
                print '| EDIT MODE:', table_name

                display(
                    HTML(
                        HTMLTable([columns] + table_data, unique_id)._repr_html_()
                    )
                )
                display(
                    Javascript(
                        """
                        $('#table%s').editableTableWidget();
                        $('#table%s').on('change', function(evt, newValue){
                            var th = $('#table%s th').eq(evt.target.cellIndex);
                            var columnName = th.text();

                            var tableName = '%s';
                            var primary_key = '%s';

                            var pkId,
                                pkValue;
                            $('#table%s tr th').filter(function(i,v){
                                if (v.innerHTML == primary_key){
                                    pkId = i;
                                }
                            });

                            var row = $('#table%s > tbody > tr').eq(evt.target.parentNode.rowIndex-1);
                            row.find('td').each(function(i,v){
                                if (i == pkId){
                                    pkValue = v.innerHTML;
                                }
                            });

                            var SQLText = "UPDATE " + tableName + " SET " + columnName + " = '" + newValue + "' WHERE " + primary_key + " = " + pkValue;
                            console.log(SQLText);

                            IPython.notebook.kernel.execute('update_table("'+SQLText+'")');
                        });
                        """ % (unique_id, unique_id, unique_id, table_name, primary_key, unique_id, unique_id)
                    )
                )
                if update_dict:
                    print update_dict
                return None

            else:
                print '| CAN\'T EDIT MULTIPLE TABLES'
                return df.replace(to_replace={'QUERY PLAN': {' ': '-'}}, regex=True)
        else:
            print '| TABLE HAS NO PK'
            return df.replace(to_replace={'QUERY PLAN': {' ': '-'}}, regex=True)
    else:
        print '| READ MODE'
        return df.replace(to_replace={'QUERY PLAN': {' ': '-'}}, regex=True)


@register_line_cell_magic
def sql(path, cell):
    return _SQL(path, cell)


js = "IPython.CodeCell.config_defaults.highlight_modes['magic_sql'] = {'reg':[/^%%sql/]};"
IPython.core.display.display_javascript(js, raw=True)
