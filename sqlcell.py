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
import json
from os.path import expanduser

import IPython
from IPython.display import Javascript
from IPython.core.display import display, HTML

from sqlalchemy import create_engine, exc

from .engines.engine_config import driver, username, password, host, port, default_db
from .engines.engines import __ENGINES_JSON_DUMPS__, __ENGINES_JSON__


unique_db_id = str(uuid.uuid4())
jupyter_id = 'jupyter' + unique_db_id
application_name = '?application_name='+jupyter_id


engine = create_engine(driver+'://'+username+':'+password+'@'+host+':'+port+'/'+default_db+application_name)
conn_string = engine.url


class __KERNEL_VARS__(object):
    g = {}


class __SQLCell_GLOBAL_VARS__(object):

    jupyter_id = jupyter_id
    ENGINE = str(engine.url)
    engine = engine
    EDIT = False
    ENGINES = __ENGINES_JSON__
    __EXPLAIN_GRAPH__ = False
    __ENGINES_JSON_DUMPS__ = __ENGINES_JSON_DUMPS__
    DB = default_db
    ISOLATION_LEVEL = 1
    TRANSACTION_BLOCK = True
    INITIAL_QUERY = True
    PATH = False
    RAW = False

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

    def _repr_html_(self, n_rows=100, length=100):
        table = '<table id="table'+self.id_+'" width=100%>'
        thead = '<thead><tr>'
        tbody = '<tbody>'
        j = 48
        query_plan = False
        for n,row in enumerate(self.data):
            if n == 0:
                if list(row):
                    query_plan = True if row[0] == 'QUERY PLAN' else False
                    if query_plan:
                        execution_time = re.findall('[0-9]{,}\.[0-9]{,}', str(self.data[-1][0]))
                        execution_time = execution_time if not execution_time else float(execution_time[0])
                    thead += '<th>' + ' ' + '</th>' ''.join([('<th>' + str(r) + '</th>') for r in row])
            elif n > n_rows:
                if not query_plan:
                    break
            else:
                if not query_plan:
                    if n > 50 and length > 100:
                        n = length - j
                        j -= 1
                    tbody += '<tr><td>' + str(n) + '</td>' + ''.join([('<td>' + str(r) + '</td>') for r in row]) + '</tr>'
                else:
                    section_time = re.search('actual time=([0-9]{,}\.[0-9]{,})\.\.([0-9]{,}\.[0-9]{,})', str(row[0]))
                    background_color = ""

                    if section_time:
                        start_time = float(section_time.group(1))
                        stop_time = float(section_time.group(2))

                        if (stop_time - start_time) > (execution_time * 0.9):
                            background_color = "#800026"
                        elif (stop_time - start_time) > (execution_time * 0.8):
                            background_color = "#bd0026"
                        elif (stop_time - start_time) > (execution_time * 0.7):
                            background_color = "#e31a1c"
                        elif (stop_time - start_time) > (execution_time * 0.6):
                            background_color = "#fc4e2a"
                        elif (stop_time - start_time) > (execution_time * 0.5):
                            background_color = "#fd8d3c"
                        elif (stop_time - start_time) > (execution_time * 0.4):
                            background_color = "#feb24c"
                        elif (stop_time - start_time) > (execution_time * 0.3):
                            background_color = "#fed976"
                        elif (stop_time - start_time) > (execution_time * 0.2):
                            background_color = "#ffeda0"
                        elif (stop_time - start_time) > (execution_time * 0.1):
                            background_color = "#ffffcc"
                        else:
                            background_color = ""

                    td_row = '<tr><td>' + str(n) + '</td>' + ''.join([('<td>' + str(r).replace('  ', '&nbsp;&nbsp;&nbsp;') + '</td>') for r in row]) + '</tr>'
                    repl = '<b style="background-color:{color};">actual time</b>'.format(color=background_color)
                    td_row = re.sub('actual time', repl, td_row)
                    tbody += td_row
        # tbody += '<tr style="height:40px;">' + ''.join([('<td></td>') for r in row]) + '</tr>' # for adding new row
        thead += '</tr></thead>'
        tbody += '</tbody>'
        table += thead + tbody
        return table

    @threaded
    def display(self, columns=[], msg=None):
        data = self.data if len(self.data) <= 100 else self.data[:49] + [['...'] * (len(self.data[0]))] + self.data[-49:]
        table_str = HTMLTable([columns] + data, self.id_)._repr_html_(n_rows=100, length=len(self.data))
        table_str = table_str.replace('<table', '<table class="table-striped table-hover"').replace("'", "\\'").replace('\n','')
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

def get_depth(obj, itr=0, depth=[]):
    if isinstance(obj, dict):
        for k, v2 in obj.items():
            if 'Plan' in k:
                if k == 'Plans':
                    itr += 1
                    depth.append(itr)
                get_depth(v2, itr=itr, depth=depth)
    elif isinstance(obj, list):
        for i, v2 in enumerate(obj):
            if 'Plans' in v2:
                get_depth(v2, itr=itr, depth=depth)
    else:
        depth.append(itr)
    return depth

def build_node(id_, node, xPos):
    _node = {
        'name': id_,
        'nodetype': node.get('Plan', node).get('Node Type'),
        'starttime': node.get('Plan', node).get('Actual Startup Time'),
        'endtime': node.get('Plan', node).get('Actual Total Time'),
        'subplan': node.get('Plan', node).get('Subplan Name'),
        'display': str(node.get('Plan', node).get('Join Filter', 
                                              node.get('Filter', 
                                                       node.get('Index Cond', 
                                                                node.get('Hash Cond', 
                                                                         node.get('One-Time Filter',
                                                                                 node.get('Recheck Cond',
                                                                                         node.get('Group Key')
                                                                                         )
                                                                                 )
                                                                        )
                                                               )
                                                      )
                                             ) or '') + (' using ' 
                                        + str(node.get('Index Name', 
                                                       node.get('Relation Name',
                                                               node.get('Schema')))) + ' ' + str(node.get('Alias')or'')
                                            if node.get('Index Name', 
                                                        node.get('Relation Name',
                                                                node.get('Schema'))) 
                                            else ''),
        'rows': node.get('Plan', node).get('Plan Rows'),
        'xPos': xPos
    }
    return _node

def node_walk(obj, key, nodes={}, xPos=None):
    if not nodes.get('nodes'):
        nodes['nodes'] = []
        nodes['links'] = []
        nodes['executionTime'] = obj.get('Execution Time')
        nodes['depth'] = 0
    target = id(obj)
    source_node = build_node(target, obj, xPos)
    xPos -= 1
    if source_node not in nodes['nodes']:
        nodes['nodes'].append(source_node)
    for i in obj.get('Plan', obj)[key]:
        source = id(i)
        if isinstance(i, dict):
            plans = i.get('Plans')
            target_node = build_node(source, i, xPos)
            if target_node not in nodes['nodes']:
                nodes['nodes'].append(target_node)
            nodes['links'].append({'source':source, 'target':target,'value':i.get('Total Cost')})
            if plans:
                nodes['depth'] += 1
                
                node_walk(i, 'Plans', nodes, xPos)
    return nodes

def load_js_files():
    display(HTML(
        """
        <script>
        $.getScript('//d3js.org/d3.v3.min.js', function(resp, status){
            console.log(resp, status, 'd3');
            $.getScript('//cdn.rawgit.com/tmthyjames/SQLCell/bootstrap-notify/js/sankey.js', function(i_resp, i_status){
                console.log(i_resp, i_status, 'd3.sankey');
            });
        });
        $.getScript('//cdn.rawgit.com/tmthyjames/SQLCell/bootstrap-notify/js/editableTableWidget.js', function(resp, status){
            console.log(resp, status, 'editableTableWidget')
        });
        </script>
        """
    ))
    return None

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
    return None

def pg_dump(cell, **kwargs):
    conn_str = create_engine(__SQLCell_GLOBAL_VARS__.ENGINE).url
    args = cell.strip().split(' ')
    if not cell.startswith('-'):
        pg_dump_cmds = ['pg_dump', '-t', args[0], args[1], '--schema-only', '-h', conn_str.host, '-U', conn_str.username]
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
    return stdout or stderr

def eval_flag(flag):
    flags = {
        'declare_engines': declare_engines,
        'pg_dump': pg_dump
    }
    return flags[flag]

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

    cell = re.sub(' \:([a-zA-Z_][a-zA-Z0-9_]{,})', '%(\g<1>)s', cell)

    unique_id = str(uuid.uuid4())
    if '__EXPLAIN__' in dir(__SQLCell_GLOBAL_VARS__) and __SQLCell_GLOBAL_VARS__.__EXPLAIN__:
        cell = 'EXPLAIN ANALYZE ' + cell
        __SQLCell_GLOBAL_VARS__.__EXPLAIN__ = False

    elif '__EXPLAIN_GRAPH__' in dir(__SQLCell_GLOBAL_VARS__) and __SQLCell_GLOBAL_VARS__.__EXPLAIN_GRAPH__:
        cell = 'EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) ' + cell
        
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
    for n,i in enumerate(args):
        if i:
            glovar = i.split('=')
            if i.startswith('MAKE_GLOBAL'):
                make_global_param = glovar
                exec(make_global_param[0]+'='+make_global_param[1]+'=None')
                exec('__SQLCell_GLOBAL_VARS__.'+make_global_param[0] + '="' + make_global_param[1] + '"')
            elif i.startswith('DB'):
                db_param = glovar
                db = i.replace('DB=', '')
                # if db != __SQLCell_GLOBAL_VARS__.DB: # revisit
                __SQLCell_GLOBAL_VARS__.DB = db
                # engine = create_engine(str(engine.url)+application_name)
                conn_string = engine.url
                engine = create_engine(conn_string.drivername+"://"+conn_string.username+":"+conn_string.password+"@"+conn_string.host+":"+str(conn_string.port or 5432)+"/"+db+application_name)

                home = expanduser("~")
                filepath = home + '/.ipython/profile_default/startup/SQLCell/engines/engine_config.py'

                for line in fileinput.FileInput(filepath,inplace=1):
                    line = re.sub("default_db = '.*'","default_db = '"+db+"'", line)
                    print line,

                exec('__SQLCell_GLOBAL_VARS__.'+db_param[0] + '="' + db_param[1] + '"')
                # exec('__SQLCell_GLOBAL_VARS__.engine ="'+str(engine.url)+'"')
                exec('__SQLCell_GLOBAL_VARS__.ENGINE ="'+str(engine.url)+'"')

            elif i.startswith('ENGINE'):
                exec("global ENGINE\nENGINE="+i.replace('ENGINE=', ""))
                if ENGINE != str(engine.url):
                    exec("global engine\nengine=create_engine(\'"+eval(i.replace('ENGINE=', ""))+application_name+"\')")
                    conn_str = engine.url
                    driver, username = conn_str.drivername, conn_str.username
                    password, host = conn_str.password, conn_str.host
                    port, db = conn_str.port, conn_str.database
                    exec('__SQLCell_GLOBAL_VARS__.ENGINE="'+i.replace('ENGINE=', "").replace("'", '')+application_name+'"')

            elif i.startswith('TRANSACTION_BLOCK'):
                __SQLCell_GLOBAL_VARS__.TRANSACTION_BLOCK = eval(glovar[1])
                if not __SQLCell_GLOBAL_VARS__.TRANSACTION_BLOCK:
                    __SQLCell_GLOBAL_VARS__.ISOLATION_LEVEL = 0

            elif i.startswith('PATH'):
                __SQLCell_GLOBAL_VARS__.PATH = glovar[1]
            elif i.startswith('--'):
                pass
            else:
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
                    <button id="explain" title="Explain Analyze Graph" onclick="explain('__EXPLAIN_GRAPH__')" type="button" class="btn btn-info btn-sm"><p class="fa fa-code-fork fa-rotate-270"</p></button>
                    <button id="explain" title="Explain Analyze" onclick="explain('__EXPLAIN__')" type="button" class="btn btn-info btn-sm"><p class="fa fa-info-circle"</p></button>
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

                (function($) {
                    var MutationObserver = window.MutationObserver || window.WebKitMutationObserver || window.MozMutationObserver;

                    $.fn.attrchange = function(callback) {
                        if (MutationObserver) {
                            var options = {
                                subtree: false,
                                attributes: true
                            };

                            var observer = new MutationObserver(function(mutations) {
                                mutations.forEach(function(e) {
                                    callback.call(e.target, e.attributeName);
                                });
                            });

                            return this.each(function() {
                                observer.observe(this, options);
                            });

                        }
                    }
                } )(jQuery);
                $("#childDiv'''+unique_id+'''").parents('.code_cell').attrchange(function(attrName){
                    if (attrName=='class'){
                        if ($(this).hasClass('unselected')){
                            $("#childDiv'''+unique_id+'''").find('button').each(function(i, obj){
                                $(obj).addClass('disabled');
                            });
                        } else if ($(this).hasClass('selected')){
                            $("#childDiv'''+unique_id+'''").find('button').each(function(i, obj){
                                $(obj).removeClass('disabled');
                            });
                        }
                    }
                });


            
                var engines = JSON.parse(`'''+str(__SQLCell_GLOBAL_VARS__.__ENGINES_JSON_DUMPS__)+'''`);
                
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
            
                function explain(gloVar){
                    var command =  `__SQLCell_GLOBAL_VARS__.`+gloVar+` = True`;
                    console.log(command);
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

    for i in args:
        if i.startswith('--'):
            mode = 'new' if len(args) == 1 else args[n+1]
            flag = i.replace('--', '')
            try:
                flag_output = eval_flag(flag)(cell, mode=mode)
                flag_output_html = flag_output.replace('\n', '<br/>').replace('    ', '&nbsp;&nbsp;&nbsp;&nbsp;')
                display(
                    Javascript(
                        """
                            $('#table{id}').append('{msg}');
                            $('#table{id}').append(`{flag_output_html}`);

                            $('#saveData{id}').removeClass('disabled');
                            $("#cancelQuery{id}").addClass('disabled')

                            $('#saveData{id}').on('click', function(){{
                                if (!$(this).hasClass('disabled')){{
                                    saveData(`{flag_output}`, 'create.txt');
                                }}
                            }});
                        """.format(
                            id=unique_id, 
                            flag_output_html=flag_output_html,
                            flag_output=flag_output,
                            msg=__SQLCell_GLOBAL_VARS__.ENGINE
                        )
                    )
                )
            except Exception as e:
                print e
            # finally:
                __SQLCell_GLOBAL_VARS__.__EXPLAIN_GRAPH__ = False
            return None

    psql_command = False
    if cell.startswith('\\'):
        psql_command = True
        db_name = db if isinstance(db, (str, unicode)) else engine.url.database

    t0 = time.time()

    if not psql_command:
        matches = re.findall(r'%\([a-zA-Z_][a-zA-Z0-9_]*\)s|:[a-zA-Z_][a-zA-Z0-9_]{,}', cell)
        connection = engine.connect()
        __SQLCell_GLOBAL_VARS__.engine = engine
        __SQLCell_GLOBAL_VARS__.DB = engine.url.database
        connection.connection.connection.set_isolation_level(__SQLCell_GLOBAL_VARS__.ISOLATION_LEVEL)

        try:
            data = connection.execute(cell, reduce(build_dict, matches, {}))
        except exc.OperationalError as e:
            print 'query cancelled...'
            __SQLCell_GLOBAL_VARS__.__EXPLAIN_GRAPH__ = False
            return None
        except exc.ProgrammingError as e:
            print e
            __SQLCell_GLOBAL_VARS__.__EXPLAIN_GRAPH__ = False
            return None
        except exc.ResourceClosedError as e:
            display(
                Javascript(
                    """
                        $('#tableData"""+unique_id+"""').append(
                            'Query finished...'
                            +'<p id=\"dbinfo"""+unique_id+"""\">To execute: %s sec | '
                            +'DB: %s | Host: %s'
                        )
                    """  % (str(round(t1, 3)), engine.url.database, engine.url.host)
                )
            )
            __SQLCell_GLOBAL_VARS__.__EXPLAIN_GRAPH__ = False
            return None
        except Exception as e:
            print e
            __SQLCell_GLOBAL_VARS__.__EXPLAIN_GRAPH__ = False
        finally:
            __SQLCell_GLOBAL_VARS__.ISOLATION_LEVEL = 1
            __SQLCell_GLOBAL_VARS__.TRANSACTION_BLOCK = True
            connection.connection.connection.set_isolation_level(__SQLCell_GLOBAL_VARS__.ISOLATION_LEVEL)

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
        conn_str = engine.url
        psql_cmds = ['psql', '-h', conn_str.host, '-U', conn_str.username, '-W', db_name, '-c', cell.strip(), '-H']
        p = subprocess.Popen(
            psql_cmds, 
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        p.stdin.write(conn_str.password)
        p.stdin.flush()
        stdout, stderr = p.communicate()
        rc = p.returncode
        t1 = time.time() - t0
        if '<table border=' not in stdout: # if tabular, psql will send back as a table because of the -H option
            display(
                Javascript(
                    """
                        $('#tableData%s').append(
                            `%s`
                            +"<p id='dbinfo%s'>To execute: %s sec | "
                            +'DB: %s | Host: %s'
                        )
                    """  % (unique_id, str(stderr), unique_id, str(round(t1, 3)), engine.url.database, engine.url.host)
                )
            )
            return None 
        data = pd.read_html(stdout, header=0)[0]
        columns = data.keys()
        table_data = [i for i in data.values.tolist()]
        df = data
    
    t1 = time.time() - t0
    t2 = time.time()

    QUERY.raw = (cell, t1)
    QUERY.history.append((cell, t1))
    
    columns = data.keys()

    if df.empty:
        display(
            Javascript(
                """
                    $("#cancelQuery"""+unique_id+"""").addClass('disabled')

                    $('#tableData"""+unique_id+"""').append(
                        '<p id=\"dbinfo"""+unique_id+"""\">To execute: %s sec | '
                        +'Rows: %s | '
                        +'DB: %s | Host: %s'
                    )
                """ % (str(round(t1, 3)), len(df.index), engine.url.database, engine.url.host)
            )
        )
        return None

    df.columns = columns

    if __SQLCell_GLOBAL_VARS__.PATH:
        try:
            df.to_csv(__SQLCell_GLOBAL_VARS__.PATH)
        except IOError as e:
            print 'ATTENTION:', e
            return None
        finally:
            __SQLCell_GLOBAL_VARS__.PATH = False

    if 'MAKE_GLOBAL' in locals():
        exec('__builtin__.' + make_global_param[1] + '=df if not __SQLCell_GLOBAL_VARS__.RAW else table_data')
        __SQLCell_GLOBAL_VARS__.RAW = False
        

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
                    +'Rows: %s | '
                    +'DB: %s | Host: %s'
                )
            """ % (str(round(t1, 3)), len(df.index), engine.url.database, engine.url.host)
        )
    )

    table_name = re.search('from\s*([a-z_][a-z\-_0-9]{,})', cell, re.IGNORECASE)
    table_name = None if not table_name else table_name.group(1).strip()

    if __SQLCell_GLOBAL_VARS__.__EXPLAIN_GRAPH__:
        query_plan_obj = table_data[0][0][0]
        try:
            xPos = max(get_depth(query_plan_obj))
            qp = node_walk(query_plan_obj, 'Plans', nodes={}, xPos=xPos)

            nodes_enum = [{'name': i['name']} for i in qp['nodes']]
            for i in reversed(qp['links']):
                i['source'] = nodes_enum.index({'name': i['source']})
                i['target'] = nodes_enum.index({'name': i['target']})
            query_plan_depth = qp['depth']

            query_plan = json.dumps(qp)

            display(
                HTML(
                """
                <style>
                .node rect {
                  cursor: move;
                  fill-opacity: .9;
                  shape-rendering: crispEdges;
                }

                .node text {
                  pointer-events: none;
                  text-shadow: 0 0px 0 #fff;
                }

                .link {
                  fill: none;
                  stroke: #000;
                  stroke-opacity: .2;
                }

                .link:hover {
                  stroke-opacity: .5;
                }
                div.output_area img, div.output_area svg{ 
                    max-width:none;
                }
                </style>
                <div id='table"""+unique_id+"""'></div>
                <script>
                var margin = {top: 10,right: 1,bottom: 6,left: 1},
                    width = Math.max("""+str(query_plan_depth*125)+""", 1000) - margin.left - margin.right,
                    height = 500 - margin.bottom;

                var formatNumber = d3.format(",.0f"),
                    format = function(d) {
                        return formatNumber(d);
                    },
                    color = d3.scale.category20();

                var svg = d3.select('#table"""+unique_id+"""').append("svg")
                    .attr("width", width + margin.left + margin.right)
                    .attr("height", height + margin.top + margin.bottom)
                    .append("g")
                    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

                var sankey = d3.sankey()
                    .nodeWidth(15)
                    .nodePadding(50)
                    .size([width, height]);

                var path = sankey.link();
                var energy = """+query_plan+"""
                var executionTime = energy.executionTime
                energy = {
                    nodes: energy.nodes,
                    links: energy.links
                };
                sankey
                    .nodes(energy.nodes)
                    .links(energy.links)
                    .layout(32);
                var link = svg.append("g").selectAll(".link")
                    .data(energy.links)
                    .enter().append("path")
                    .attr("class", "link")
                    .attr("d", path)
                    .style("stroke-width", function(d) {
                        return Math.max(1, d.dy);
                    })
                    .sort(function(a, b) {
                        return b.dy - a.dy;
                    });

                link.append("title")
                    .html(function(d) {
                        return d.source.nodetype + " -> " 
                            + d.target.nodetype + "<br/>" 
                            + 'Total Cost: ' + format(d.value) + "<br/>"
                            + 'Child Rows: ' + format(d.source.rows) + "<br/>"
                            + 'Parent Rows: ' + format(d.target.rows);
                    });

                var node = svg.append("g").selectAll(".node")
                    .data(energy.nodes)
                    .enter().append("g")
                    .attr("class", "node")
                    .attr("transform", function(d) {
                        return "translate(" + d.x + "," + d.y + ")";
                    })
                    .call(d3.behavior.drag()
                        .origin(function(d) {
                            return d;
                        })
                        .on("dragstart", function() {
                            this.parentNode.appendChild(this);
                        })
                        .on("drag", dragmove));

                node.append("rect")
                    .attr("height", function(d) {
                        return Math.max(d.dy, 3);
                    })
                    .attr("width", sankey.nodeWidth())
                    .style("fill", function(d) {
                        if ((d.endtime - d.starttime) > (executionTime * 0.9)) return d.color = "#800026"
                        else if ((d.endtime - d.starttime) > (executionTime * 0.8)) return d.color = "#bd0026"
                        else if ((d.endtime - d.starttime) > (executionTime * 0.7)) return d.color = "#e31a1c"
                        else if ((d.endtime - d.starttime) > (executionTime * 0.6)) return d.color = "#fc4e2a"
                        else if ((d.endtime - d.starttime) > (executionTime * 0.5)) return d.color = "#fd8d3c"
                        else if ((d.endtime - d.starttime) > (executionTime * 0.4)) return d.color = "#feb24c"
                        else if ((d.endtime - d.starttime) > (executionTime * 0.3)) return d.color = "#fed976"
                        else if ((d.endtime - d.starttime) > (executionTime * 0.2)) return d.color = "#ffeda0"
                        else if ((d.endtime - d.starttime) > (executionTime * 0.1)) return d.color = "#ffffcc"
                        else return d.color = "#969696"
                    })
                    .append("title")
                    .html(function(d) { 
                        return (d.display || '') + "<br/>Cost: " 
                            + formatNumber(d.value) + "<br/>Time: " 
                            + d.starttime + '...' + d.endtime
                            + '<br/>Rows: ' + formatNumber(d.rows);
                    });

                node.append("text")
                    .attr("x", -6)
                    .attr("y", function(d) {
                        return d.dy / 2;
                    })
                    .attr("dy", ".35em")
                    .attr("text-anchor", "end")
                    .attr("transform", null)
                    .text(function(d) {
                        return d.subplan || d.nodetype;
                    })
                    .filter(function(d) {
                        return d.x < width / 2;
                    })
                    .attr("x", 6 + sankey.nodeWidth())
                    .attr("text-anchor", "start");

                function dragmove(d) {
                    d3.select(this).attr("transform", "translate(" + d.x + "," + (d.y = Math.max(0, Math.min(height - d.dy, d3.event.y))) + ")");
                    sankey.relayout();
                    link.attr("d", path);
                }
                </script>
                """
                )
            )
        except KeyError as e:
            print "No visual available for this query"
        finally:
            __SQLCell_GLOBAL_VARS__.__EXPLAIN_GRAPH__ = False
        return None

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
    if __SQLCell_GLOBAL_VARS__.INITIAL_QUERY:
        load_js_files()

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
    __SQLCell_GLOBAL_VARS__.INITIAL_QUERY = False
    return None


js = "IPython.CodeCell.config_defaults.highlight_modes['magic_sql'] = {'reg':[/^%%sql/]};"
IPython.core.display.display_javascript(js, raw=True)
 
