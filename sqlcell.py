# __builtin__'s used with MAKE_GLOBAL param so data can 
# be passed from this module to the notebook without referencing 
# a class or any other object. We can just call the variable that's
# passed to MAKE_GLOBAL
import __builtin__
import re
import fileinput
import time
import uuid
import subprocess
import sys
import threading
import logging
from os.path import expanduser

import IPython
from IPython.display import Javascript
from IPython.core.display import display, HTML

from sqlalchemy import create_engine, exc

from .engines.engine_config import driver, username, password, host, port, default_db
from .engines.engines import __ENGINES_JSON_DUMPS__


display(Javascript("""$.getScript( "js/editableTableWidget.js");"""))

unique_db_id = str(uuid.uuid4())
jupyter_id = 'jupyter' + unique_db_id
application_name = '?application_name='+jupyter_id


engine = create_engine(driver+'://'+username+':'+password+'@'+host+':'+port+'/'+default_db+application_name)


class __KERNEL_VARS__(object):
    g = {}


class __SQLCell_GLOBAL_VARS__(object):

    jupyter_id = jupyter_id
    engine = engine
    EDIT = False

    logger = logging.getLogger()
    handler = logging.StreamHandler()
    logger.setLevel(logging.DEBUG)

    def kill_last_pid_on_new_thread(self, app, db, unique_id):
        t = threading.Thread(target=kill_last_pid, args=(app, db))
        t.start()
        HTMLTable([], unique_id).display(msg='<h5 id="error" style="color:#d9534f;">QUERY DEAD...</h5>')
        return True

    @staticmethod
    def update_table(sql):
        return engine.execute(sql)


def threaded(fn):
    def wrapper(*args, **kwargs):
        threading.Thread(target=fn, args=args, kwargs=kwargs).start()
    return wrapper

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

    def _repr_html_(self, n_rows=100):
        table = '<table id="table'+self.id_+'" width=100%>'
        thead = '<thead><tr>'
        tbody = '<tbody>'
        for n,row in enumerate(self.data):
            if n == 0:
                thead += '<th>' + ' ' + '</th>' ''.join([('<th>' + str(r) + '</th>') for r in row])
            elif n > n_rows:
                break
            else:
                tbody += '<tr><td>' + str(n) + '</td>' + ''.join([('<td>' + str(r).replace('  ', '&nbsp;&nbsp;&nbsp;') + '</td>') for r in row]) + '</tr>'
        # tbody += '<tr style="height:40px;">' + ''.join([('<td></td>') for r in row]) + '</tr>' # for adding new row
        thead += '</tr></thead>'
        tbody += '</tbody>'
        table += thead + tbody
        return table

    @threaded
    def display(self, columns=[], msg=None):
        table_str = HTMLTable([columns] + self.data, self.id_)._repr_html_(n_rows=100)
        table_str = table_str.replace('<table', '<table class="table-striped table-hover"').replace("'", "\\'")
        display(
            Javascript(
                """
                $('#dbinfo{id}').append('{msg}');
                $('#table{id}').append('{table}');
                """.format(msg=str(msg), table=table_str, id=self.id_)
            )
        )

    def to_csv(self, path):
        import csv
        with open(path, 'w') as fp:
            a = csv.writer(fp, delimiter=',')
            a.writerows(self.data)

try:
    import pandas as pd
    pd.options.display.max_columns = None
    pd.set_option('display.max_colwidth', -1)
    to_table = pd.DataFrame
except ImportError as e:
    to_table = HTMLTable


def build_dict(output, row):
    output[row.replace('%(','').replace(')s','')] = eval("__KERNEL_VARS__.g.get('"+row.replace('%(','').replace(')s','')+"')")
    return output


def kill_last_pid(app=None, db=None):
    from sqlalchemy import create_engine    

    connection = create_engine("postgresql://tdobbins:tdobbins@localhost:5432/"+db+"?application_name=garbage_collection")
    try:
        pid_sql = """
            SELECT pid 
            FROM pg_stat_activity 
            where application_name = %(app)s
            """
        pids = [i.pid for i in connection.execute(pid_sql, {
                'app': app
            }
        )]
        for pid in pids:
            cancel_sql = "select pg_cancel_backend(%(pid)s);"
            cancel_execute = [i for i in connection.execute(cancel_sql, {
                    'pid': pid
                }
            )]
            print 'cancelled postgres job:', pid, 'application: ', app

        return True

    except Exception as e:
        print e
        return False

    finally:
        print 'closing DB connection....'
        connection.dispose()

    return True


def _SQL(path, cell, __KERNEL_VARS__):
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
    global driver, username, password, host, port, db, table, __EXPLAIN__, __GETDATA__, __SAVEDATA__, engine
    db = default_db

    unique_id = str(uuid.uuid4())
    if '__EXPLAIN__' in dir(__SQLCell_GLOBAL_VARS__) and __SQLCell_GLOBAL_VARS__.__EXPLAIN__:
        cell = 'EXPLAIN ANALYZE ' + cell
        __SQLCell_GLOBAL_VARS__.__EXPLAIN__ = False
        
    elif '__GETDATA__' in dir(__SQLCell_GLOBAL_VARS__) and __SQLCell_GLOBAL_VARS__.__GETDATA__:
        if 'MAKE_GLOBAL' not in path:
            path = 'MAKE_GLOBAL=DATA RAW=True ' + path.strip()
            print 'data available as DATA'
        __SQLCell_GLOBAL_VARS__.__GETDATA__ = False
    
    elif '__SAVEDATA__' in dir(__SQLCell_GLOBAL_VARS__) and __SQLCell_GLOBAL_VARS__.__SAVEDATA__:
        # path = 'PATH="'+PATH+'" '+path # not sure what this did
        __SQLCell_GLOBAL_VARS__.__SAVEDATA__ = PATH = False
        
    elif 'ENGINE' in dir(__SQLCell_GLOBAL_VARS__) and __SQLCell_GLOBAL_VARS__.ENGINE:
        engine = create_engine(__SQLCell_GLOBAL_VARS__.ENGINE + application_name)

    args = path.split(' ')
    for i in args:
        if i:
            if i.startswith('MAKE_GLOBAL'):
                glovar = i.split('=')
                exec(glovar[0]+'='+glovar[1]+'=None')
                exec('__SQLCell_GLOBAL_VARS__.'+i)
            elif i.startswith('DB'):
                db = i.replace('DB=', '') 
                __SQLCell_GLOBAL_VARS__.DB = db
                engine = engine if 'ENGINE' in dir(__SQLCell_GLOBAL_VARS__) else create_engine(driver+"://"+username+":"+password+"@"+host+":"+port+"/"+db+application_name)

                home = expanduser("~")
                filepath = home + '/.ipython/profile_default/startup/SQLCell/engines/engine_config.py'

                for line in fileinput.FileInput(filepath,inplace=1):
                    line = re.sub("default_db = '.*'","default_db = '"+db+"'", line)
                    print line,

                exec('__SQLCell_GLOBAL_VARS__.'+i)

            elif i.startswith('ENGINE'):
                exec("global ENGINE\nENGINE="+i.replace('ENGINE=', ""))
                if ENGINE != str(engine.url):
                    exec("global engine\nengine=create_engine(\'"+eval(i.replace('ENGINE=', ""))+application_name+"\')")
                    conn_str = engine.url
                    driver, username = conn_str.drivername, conn_str.username
                    password, host = conn_str.password, conn_str.host
                    port, db = conn_str.port, conn_str.database
                    exec('__SQLCell_GLOBAL_VARS__.ENGINE="'+i.replace('ENGINE=', "").replace("'", '')+application_name+'"')

            else:
                exec(i)
                exec('__SQLCell_GLOBAL_VARS__.'+i)

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
            #table'''+unique_id+'''{
                padding-top: 40px;
            }
            </style>
            <div class="row" id="childDiv'''+unique_id+'''">
                <div class="btn-group col-md-3">
                    <button id="explain" title="Explain Analyze" onclick="explain()" type="button" class="btn btn-info btn-sm"><p class="fa fa-info-circle"</p></button>
                    <button type="button" title="Execute" onclick="run()" class="btn btn-success btn-sm"><p class="fa fa-play"></p></button>
                    <button type="button" title="Execute and Return Data as Variable" onclick="getData()" class="btn btn-success btn-sm"><p class="">var</p></button>
                    <button id="saveData'''+unique_id+'''" title="Save" class="btn btn-success btn-sm disabled" type="button"><p class="fa fa-save"</p></button>
                    <button id="cancelQuery'''+unique_id+'''" title="Cancel Query" class="btn btn-danger btn-sm" type="button"><p class="fa fa-stop"</p></button>
                </div>
                <div id="engineButtons'''+unique_id+'''" class="btn-group col-md-4"></div>
                <div id="tableData'''+unique_id+'''"></div>
            </div>
            <div class="table" id="table'''+unique_id+'''"></div>
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
                    var command =  `__SQLCell_GLOBAL_VARS__.__EXPLAIN__ = True`;
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
                    IPython.notebook.kernel.execute('__SQLCell_GLOBAL_VARS__().kill_last_pid_on_new_thread(__SQLCell_GLOBAL_VARS__.jupyter_id, "'''+db+'''", "'''+unique_id+'''")',
                        {
                            iopub: {
                                output: function(response) {
                                    var $table = $("#table'''+unique_id+'''").parent();
                                    console.log(response);
                                    if (response.content && response.content.text){
                                        $table.append('<h5 style="color:#d9534f;">'+response.content.text+'</h5>')
                                    } else if (response.content && response.content.evalue){
                                        $table.append('<h5 style="color:#d9534f;">'+response.content.evalue+'</h5>');
                                    }
                                }
                            }
                        },
                        {
                            silent: false, 
                            store_history: false, 
                            stop_on_error: true
                        }
                    );
                };
                
                function getData(){
                    var command = `__SQLCell_GLOBAL_VARS__.__GETDATA__ = True`;
                    var kernel = IPython.notebook.kernel;
                    kernel.execute(command);
                    IPython.notebook.execute_cell();
                };
                
                function saveData(data, filename){
                    var path = $('#path').val() || './'
                    var command = `__SQLCell_GLOBAL_VARS__.__SAVEDATA__, PATH = True,'`+path+`'`;
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
                    var command = "__SQLCell_GLOBAL_VARS__.ENGINE = " + "'" + engines[engine].engine + "'''+db+'''" + "'";
                    var kernel = IPython.notebook.kernel;
                    kernel.execute(command,{
                        iopub: {
                            output: function(response) {
                                var $table = $("#table'''+unique_id+'''").parent();
                                console.log(response);
                                if (response.content && response.content.text){
                                    $table.append('<h5 style="color:#d9534f;">'+response.content.text+'</h5>')
                                } else if (response.content && response.content.evalue){
                                    $table.append('<h5 style="color:#d9534f;">'+response.content.evalue+'</h5>');
                                }
                            }
                        }
                    },
                        {
                            silent: false, 
                            store_history: false, 
                            stop_on_error: true
                        }
                    );
                    IPython.notebook.execute_cell();
                };

            </script>
            '''
        )
    )

    psql_command = False
    if cell.startswith('\\'):
        psql_command = True
        db_name = db if isinstance(db, (str, unicode)) else __ENGINE__.url.database

        commands = ''
        for i in cell.strip().split(';'):
            if i:
                commands += ' -c ' + '"'+i+'" '
        commands = 'psql ' + db_name + commands + '-H'

    matches = re.findall(r'%\([a-zA-Z_][a-zA-Z0-9_]*\)s', cell)
    
    connection = engine.connect()
    t0 = time.time()

    try:
        if not psql_command:
            data = connection.execute(cell, reduce(build_dict, matches, {}))
            t1 = time.time() - t0
            columns = data.keys()
            table_data = [i for i in data] if 'pd' in globals() else [columns] + [i for i in data]
            if 'DISPLAY' in locals():
                if not DISPLAY:
                    if 'MAKE_GLOBAL' in locals():
                        exec('__builtin__.' + glovar[1] + '=table_data')
                    else:
                        exec('__builtin__.DATA=table_data')
                        glovar = ['', 'DATA']
                    print 'To execute: ' + str(round(t1, 3)) + ' sec', '|', 
                    print 'Rows:', len(table_data), '|',
                    print 'DB:', engine.url.database, '| Host:', engine.url.host
                    print 'data not displayed but captured in variable: ' + glovar[1]
                    return None
            df = to_table(table_data)
        else:
            output = subprocess.check_output(commands, shell=True)
            t1 = time.time() - t0
            if '<table border=' not in output: # if tabular, psql will send back as a table because of the -H option
                print output
                return None 
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

    if 'PATH' in locals() and PATH:
        try:
            df.to_csv(PATH)
        except IOError as e:
            print 'ATTENTION:', e
            return None

    if 'MAKE_GLOBAL' in locals():
        exec('__builtin__.' + glovar[1] + '=df if \'RAW\' not in locals() else table_data')
        

    str_data = df.to_csv(sep="\t") # for downloading

    t3 = time.time() - t2
    
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
                $('#tableData"""+unique_id+"""').append(
                    '<p id=\"dbinfo"""+unique_id+"""\">To execute: %s sec | '
                    +'To render: %s sec | '
                    +'Rows: %s | '
                    +'DB: %s | Host: %s'
                )
            """ % (str(round(t1, 3)), str(round(t3, 3)), len(df.index), engine.url.database, engine.url.host)
        )
    )

    table_name = re.search('from\s*([a-z_][a-z\-_0-9]{,})', cell, re.IGNORECASE)
    table_name = None if not table_name else table_name.group(1).strip()

    if __SQLCell_GLOBAL_VARS__.EDIT:
        __SQLCell_GLOBAL_VARS__.EDIT = False
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

            if not re.search('join', cell, re.IGNORECASE):

                HTMLTable(table_data, unique_id).display(columns, msg=' | EDIT MODE')

                display(
                    Javascript(
                        """
                        $('#table%s').editableTableWidget({preventColumns:[1,2]});
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
                            console.log(SQLText, 't' + pkValue + 't');

                            if (pkValue === ''){
                                console.log('testingietren');
                            } else {
                                $('#error').remove();
                                IPython.notebook.kernel.execute('__SQLCell_GLOBAL_VARS__.update_table("'+SQLText+'")',
                                    {
                                        iopub: {
                                            output: function(response) {
                                                var $table = $('#table%s').parent();
                                                if (response.content.evalue){
                                                    var error = response.content.evalue.replace(/\\n/g, "</br>");
                                                    $table.append('<h5 id="error" style="color:#d9534f;">'+error+'</h5>');
                                                } else {
                                                    $table.append('<h5 id="error" style="color:#5cb85c;">Update successful</h5>');
                                                }
                                            }
                                        }
                                    },
                                    {
                                        silent: false, 
                                        store_history: false, 
                                        stop_on_error: true
                                    }
                                );
                            }
                        });
                        """ % (unique_id, unique_id, unique_id, table_name, primary_key, unique_id, unique_id, unique_id)
                    )
                )

            else:
                HTMLTable(table_data, unique_id).display(columns, msg=" | CAN\\'T EDIT MULTIPLE TABLES")
                return None
        else:
            HTMLTable(table_data, unique_id).display(columns, msg=' | TABLE HAS NO PK')
            return None
    else:
        HTMLTable(table_data, unique_id).display(columns, msg=' | READ MODE')
        return None


def sql(path, cell):
    t = threading.Thread(
        target=_SQL, 
        args=(
            path, cell, {
                    k:v
                    for (k,v) in __KERNEL_VARS__.g.iteritems() 
                        if k not in ('In', 'Out', 'v', 'k') 
                            and not k.startswith('_') 
                            and isinstance(v, 
                               (str, int, float, list, unicode, tuple)
                            )
                }
            )
        )
    t.daemon = True
    t.start()
    return None


js = "IPython.CodeCell.config_defaults.highlight_modes['magic_sql'] = {'reg':[/^%%sql/]};"
IPython.core.display.display_javascript(js, raw=True)
 