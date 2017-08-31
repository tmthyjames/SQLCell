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
import functools
from os.path import expanduser

import IPython
from IPython.core.display import display, HTML

from sqlalchemy import create_engine, exc

from .engines.engine_config import driver, username, password, host, port, default_db
from .engines.engines import __ENGINES_JSON_DUMPS__, __ENGINES_JSON__
from .python_js.interface_js import buttons_js, notify_js, sankey_js, table_js, psql_table_js, load_js_scripts, info_bar_js, finished_query_js
from .tasks.params import __SQLCell_GLOBAL_VARS__, unique_db_id, engine, application_name
from .tasks.utility_belt import threaded, HTMLTable, ParseNodes, build_dict, kill_last_pid, load_js_files
from .tasks.flags import declare_engines, pg_dump, eval_flag


class __KERNEL_VARS__(object):
    g = {}


class QUERY(object):
    raw = ''
    history = []

try:
    import pandas as pd
    pd.options.display.max_columns = None
    pd.set_option('display.max_colwidth', -1)
    to_table = pd.DataFrame
except ImportError as e:
    to_table = HTMLTable


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
            else:
                if not path.strip().startswith('--'):
                    exec('__SQLCell_GLOBAL_VARS__.'+i)

    display(HTML(
        buttons_js(unique_id, __SQLCell_GLOBAL_VARS__.__ENGINES_JSON_DUMPS__, unique_db_id, db)
        )
    )

    display(HTML(
        """
        <script>
        $('.kernel_indicator_name')[0].innerHTML = '{engine}'
        </script>
        """.format(engine=engine.url.host)
    ))

    for i in args:
        if i.startswith('--'):
            mode = 'new' if len(args) == 1 else args[1]
            flag = i.replace('--', '')
            try:
                flag_output = eval_flag(flag)(cell, mode=mode, __SQLCell_GLOBAL_VARS__=__SQLCell_GLOBAL_VARS__)
                flag_output_html = flag_output.replace('\n', '<br/>').replace('    ', '&nbsp;&nbsp;&nbsp;&nbsp;')
                display(
                    HTML(
                        info_bar_js(unique_id, flag_output_html, flag_output, __SQLCell_GLOBAL_VARS__.ENGINE)
                    )
                )
                __SQLCell_GLOBAL_VARS__.__EXPLAIN_GRAPH__ = False if __SQLCell_GLOBAL_VARS__.__EXPLAIN_GRAPH__ else __SQLCell_GLOBAL_VARS__.__EXPLAIN_GRAPH__
            except Exception as e:
                print e
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
            data = connection.execute(cell, reduce(functools.partial(build_dict, __KERNEL_VARS__=__KERNEL_VARS__), matches, {}))
        except exc.OperationalError as e:
            print 'query cancelled...', e
            __SQLCell_GLOBAL_VARS__.__EXPLAIN_GRAPH__ = False
            return None
        except exc.ProgrammingError as e:
            print e
            __SQLCell_GLOBAL_VARS__.__EXPLAIN_GRAPH__ = False
            return None
        except exc.ResourceClosedError as e:
            display(
                HTML(
                    finished_query_js(unique_id, t1, engine)
                )
            )
            __SQLCell_GLOBAL_VARS__.__EXPLAIN_GRAPH__ = False
            return None
        # except Exception as e:
        #     print e
            # __SQLCell_GLOBAL_VARS__.__EXPLAIN_GRAPH__ = False
        finally:
            __SQLCell_GLOBAL_VARS__.ISOLATION_LEVEL = 1
            __SQLCell_GLOBAL_VARS__.TRANSACTION_BLOCK = True
            connection.connection.connection.set_isolation_level(__SQLCell_GLOBAL_VARS__.ISOLATION_LEVEL)

        t1 = time.time() - t0
        columns = data.keys()
        if data.returns_rows:
            table_data = [i for i in data] if 'pd' in globals() else [columns] + [i for i in data]
            if hasattr(__SQLCell_GLOBAL_VARS__, 'DISPLAY') and __SQLCell_GLOBAL_VARS__.DISPLAY is False:
                if 'MAKE_GLOBAL' in locals():
                    var_name = make_global_param[1]
                    exec('__builtin__.' + var_name + '=table_data')
                else:
                    var_name = 'DATA'
                    exec('__builtin__.DATA=table_data')
                    glovar = ['', 'DATA']
                print 'To execute: ' + str(round(t1, 3)) + ' sec', '|', 
                print 'Rows:', len(table_data), '|',
                print 'DB:', engine.url.database, '| Host:', engine.url.host
                print 'data not displayed but captured in variable: ' + var_name
                __SQLCell_GLOBAL_VARS__.DISPLAY = True
                return None
            df = to_table(table_data)
        else:
            display(
                HTML(
                    finished_query_js(unique_id, t1, engine)
                )
            )
            __SQLCell_GLOBAL_VARS__.__EXPLAIN_GRAPH__ = False
            return None

    else:
        __SQLCell_GLOBAL_VARS__.engine = engine
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
                HTML(
                    """
                    <script>
                        $('#tableData%s').append(
                            `%s`
                            +"<p class='smallfont' id='dbinfo%s'>To execute: %s sec | "
                            +'DB: %s | Host: %s'
                        )
                    </script>
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
            HTML(
                """
                <script>
                    $("#cancelQuery"""+unique_id+"""").addClass('disabled')

                    $('#tableData"""+unique_id+"""').append(
                        '<p class="smallfont" id=\"dbinfo"""+unique_id+"""\">To execute: %s sec | '
                        +'Rows: %s | '
                        +'DB: %s | Host: %s'
                    )
                </script>
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
        

    str_data = df.to_csv(sep="\t", encoding='utf-8') # for downloading

    t3 = time.time() - t2

    sql_sample = cell[:] if len(cell) < 100 else cell[:100] + " ..."
    
    display(
        HTML(
            """
            <script>
                $('#saveData"""+unique_id+"""').removeClass('disabled');
                $("#cancelQuery"""+unique_id+"""").addClass('disabled')

                $('#saveData"""+unique_id+"""').on('click', function(){
                    if (!$(this).hasClass('disabled')){
                        saveData(`"""+str_data+"""`, 'data.tsv');
                    }
                });
                $('#tableData"""+unique_id+"""').append(
                    '<p class="smallfont" id=\"dbinfo"""+unique_id+"""\">To execute: %s sec | '
                    +'Rows: %s | '
                    +'DB: %s | Host: %s'
                )
            </script>
            """ % (str(round(t1, 3)), len(df.index), engine.url.database, engine.url.host)
        )
    )
    
    if __SQLCell_GLOBAL_VARS__.NOTIFY:           
        display(
            HTML(
                notify_js(unique_id, cell, t1, df, engine, 0 if isinstance(__SQLCell_GLOBAL_VARS__.NOTIFY, bool) else __SQLCell_GLOBAL_VARS__.NOTIFY)
            )
        )

    table_name = re.search('from\s*([a-z_][a-z\-_0-9]{,})', cell, re.IGNORECASE)
    table_name = None if not table_name else table_name.group(1).strip()

    if __SQLCell_GLOBAL_VARS__.__EXPLAIN_GRAPH__:
        query_plan_obj = table_data[0][0][0]
        try:
            xPos = max(ParseNodes(query_plan_obj).get_depth())
            qp = ParseNodes(query_plan_obj).node_walk('Plans', nodes={}, xPos=xPos)

            nodes_enum = [{'name': i['name']} for i in qp['nodes']]
            for i in reversed(qp['links']):
                i['source'] = nodes_enum.index({'name': i['source']})
                i['target'] = nodes_enum.index({'name': i['target']})
            query_plan_depth = qp['depth']

            query_plan = json.dumps(qp)

            display(
                HTML(
                    sankey_js(unique_id, query_plan_depth, query_plan)
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
                    HTML( 
                        table_js(unique_id, table_name, primary_key)
                    )
                )

            else:
                HTMLTable(table_data, unique_id).display(columns, msg=" | CAN\\'T EDIT MULTIPLE TABLES")
                return None

        elif psql_command:
            table_match = re.search('\\\d +([a-zA-Z_][a-zA-Z0-9_]{,})', cell)
            table_name = table_match if not table_match else table_match.group(1)
            if table_match:
                HTMLTable(table_data, unique_id).display(columns, msg=' | ALTER TABLE')
                display(
                    HTML(
                        psql_table_js(unique_id, table_name)
                    )
                )
            else:
                HTMLTable(table_data, unique_id).display(columns, msg=' | <code>\\\d</code> ONLY')
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
        time.sleep(0.4) # to make sure all the JS files load

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
 
