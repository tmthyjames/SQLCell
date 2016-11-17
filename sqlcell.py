import re
import fileinput
import time
import uuid
import json
import subprocess
import sys
import thread
import threading
import Queue
from contextlib import contextmanager
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
jupyter_id = 'jupyter' + unique_db_id
application_name = '?application_name='+jupyter_id

for k,v in __ENGINES_JSON__.iteritems():
    exec(k+'="'+v['engine']+'"')

__ENGINES_JSON_DUMPS__ = json.dumps(__ENGINES_JSON__)

__ENGINE__ = create_engine(driver+'://'+username+':'+password+'@'+host+':'+port+'/'+default_db+application_name)
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
        tbody += '<tr style="height:50px;">' + ''.join([('<td></td>') for r in row]) + '</tr>' # add row on edit mode
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

# we need a lock, so that other threads don't snatch control
# while we have set a temporary parent # from: http://nbviewer.jupyter.org/gist/minrk/4563193

@contextmanager
def set_stdout_parent(parent):
    """a context manager for setting a particular parent for sys.stdout
    
    the parent determines the destination cell of output
    """
    stdout_lock = threading.Lock()
    save_parent = sys.stdout.parent_header
    with stdout_lock:
        sys.stdout.parent_header = parent
        try:
            yield
        finally:
            # the flush is important, because that's when the parent_header actually has its effect
            sys.stdout.flush()
            sys.stdout.parent_header = save_parent

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
    return __ENGINE__.execute(sql)

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

def kill_last_pid_on_new_thread(app, db):
    print 'killing query',
    t = threading.Thread(target=kill_last_pid, args=(app, db))
    # thread.start_new_thread(kill_last_pid, (app, db))
    t.start()
    print 'query dead...'
    return True

def new_queue():
    return Queue.Queue()

# @timer
def _SQL(path, cell, q=None, thread_parent=None):
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
    global driver, username, password, host, port, db, table, __EXPLAIN__, __GETDATA__, __SAVEDATA__, __ENGINE__, PATH
    unique_id = str(uuid.uuid4())
    thread_parent = sys.stdout.parent_header

    display(
        HTML(
            '''
            <style>
            .output_scroll {
                display: #flex; /* cancel css property set by ipython */
            }
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
                    console.log(applicationID)
                    $.get('/halt_query?kill_last_postgres_process=jupyter'+applicationID+'&engine=localhost&db='+"'''+__ENGINE__.url.database+'''", function(d){
                        console.log(d);
                    });
                    //IPython.notebook.kernel.execute('kill_last_pid(jupyter_id, db)',
                      //  {
                        //    iopub: {
                          //      output: function(response) {
                            //        var $table = $("#table'''+unique_id+'''").parent();
                              //      console.log(response);
                                //}
                            //}
                        //},
                        //{
                          //  silent: false, 
                            //store_history: false, 
                            //stop_on_error: true
                        //}
                    //);
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
            with set_stdout_parent(thread_parent):
                print 'data available as DATA'
        __GETDATA__ = False
    
    elif '__SAVEDATA__' in globals() and __SAVEDATA__:
        path = 'PATH="'+PATH+'" '+path
        __SAVEDATA__ = PATH = False
        
    elif 'ENGINE' in globals() and ENGINE:
        __ENGINE__ = create_engine(ENGINE)

    args = path.split(' ')
    for i in args:
        if i.startswith('MAKE_GLOBAL'):
            glovar = i.split('=')
            exec(glovar[0]+'='+glovar[1]+'=None')
        elif i.startswith('DB'):
            db = i.replace('DB=', '') 
            e = __ENGINE__.url
            driver, username, password, host, port = e.drivername, e.username, e.password, e.host, ":" + str(e.port) if e.port else ""
            exec("global __ENGINE__\n__ENGINE__=create_engine('"+driver+"://"+username+":"+password+"@"+host+port+"/"+db+application_name+"')")
            exec('global DB\nDB=db')

            home = expanduser("~")
            filepath = home + '/.ipython/profile_default/startup/ac_engine_config.py'

            for line in fileinput.FileInput(filepath,inplace=1):
                line = re.sub("default_db = '.*'","default_db = '"+db+"'", line)
                print line,

        elif i.startswith('ENGINE'):
            exec("global ENGINE\nENGINE="+i.replace('ENGINE=', ""))
            if ENGINE != str(__ENGINE__.url):
                exec("global __ENGINE__\n__ENGINE__=create_engine("+i.replace('ENGINE=', "")+application_name+")")
                conn_str = __ENGINE__.url
                driver, username = conn_str.drivername, conn_str.username
                password, host = conn_str.password, conn_str.host
                port, db = conn_str.port, conn_str.database
        else:
            exec(i)

    psql_command = False
    if cell.startswith('\\'):
        psql_command = True
        db_name = db if isinstance(db, (str, unicode)) else db.__ENGINE__.url.database

        commands = ''
        for i in cell.strip().split(';'):
            if i:
                commands += ' -c ' + '"'+i+'" '
        commands = 'psql ' + db + commands + '-H'

    matches = re.findall(r'%\([a-zA-Z_][a-zA-Z0-9_]*\)s', cell)
    
    t0 = time.time()
    connection = __ENGINE__.connect()

    try:
        if not psql_command:
            data = connection.execute(cell, reduce(build_dict, matches, {}))
            t1 = time.time() - t0
            columns = data.keys()
            table_data = [i for i in data] if 'pd' in globals() else [columns] + [i for i in data]
            if 'DISPLAY' in locals():
                if not DISPLAY:
                    exec('global ' + glovar[1] + '\n' + glovar[1] + '=table_data')
                    print 'To execute: ' + str(round(t1, 3)) + ' sec', '|', 
                    print 'Rows:', len(table_data), '|',
                    print 'DB:', __ENGINE__.url.database, '| Host:', __ENGINE__.url.host
                    print 'data not displayed but captured in variable: ' + glovar[1]
                    return None
            df = to_table(table_data)
        else:
            output = subprocess.check_output(commands, shell=True)
            t1 = time.time() - t0
            if '<table border=' not in output: # if tabular, psql will send back as a table because of the -H option
                return output
            data = pd.read_html(output, header=0)[0]
            columns = data.keys()
            table_data = [i for i in data.values.tolist()]
            df = data
    except exc.OperationalError as e:
        # with set_stdout_parent(thread_parent):
        print 'Query cancelled...'
        return None
    except exc.ResourceClosedError as e:
        # with set_stdout_parent(thread_parent):
        print 'Query ran successfully...'
        return None
    
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
            # with set_stdout_parent(thread_parent):
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
    # with set_stdout_parent(thread_parent):
    print 'To execute: ' + str(round(t1, 3)) + ' sec', '|', 
    print 'To render: ' + str(round(t3, 3)) + ' sec', '|', 
    print 'Rows:', len(df.index), '|',
    print 'DB:', __ENGINE__.url.database, '| Host:', __ENGINE__.url.host,

    table_name = re.search('from\s*([a-z_][a-z\-_0-9]{,})', cell, re.IGNORECASE)
    table_name = None if not table_name else table_name.group(1).strip()

    if EDIT:

        primary_key_results = __ENGINE__.execute("""
                SELECT               
                  %(table_name)s as table_name, pg_attribute.attname as column_name, pg_attribute.attnum as column_index
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
            primary_key_index = primary_key_results.column_index

            update_dict = None
            if not re.search('join', cell, re.IGNORECASE):
                # with set_stdout_parent(thread_parent):
                print '| EDIT MODE:', table_name

                display(
                    HTML(
                        HTMLTable([columns] + table_data, unique_id)._repr_html_()
                    )
                )
                display(
                    Javascript(
                        """
                        $('#table%s').editableTableWidget({preventColumns:[%s]});
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
                                IPython.notebook.kernel.execute('update_table("'+SQLText+'")',
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
                        """ % (unique_id, primary_key_index, unique_id, unique_id, table_name, primary_key, unique_id, unique_id, unique_id)
                    )
                )
                if update_dict:
                    # with set_stdout_parent(thread_parent):
                    print update_dict
                return None

            else:
                # with set_stdout_parent(thread_parent):
                print '| CAN\'T EDIT MULTIPLE TABLES'
                return df.replace(to_replace={'QUERY PLAN': {' ': '-'}}, regex=True)
        else:
            # with set_stdout_parent(thread_parent):
            print '| TABLE HAS NO PK'
            return df.replace(to_replace={'QUERY PLAN': {' ': '-'}}, regex=True)
    else:
        # with set_stdout_parent(thread_parent):
        print '| READ MODE'
        return df.replace(to_replace={'QUERY PLAN': {' ': '-'}}, regex=True)


@register_line_cell_magic
def sql(path, cell):
    return _SQL(path, cell)


js = "IPython.CodeCell.config_defaults.highlight_modes['magic_sql'] = {'reg':[/^%%sql/]};"
IPython.core.display.display_javascript(js, raw=True)
